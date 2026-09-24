#include "kvstore/kvstore.h"
#include <sys/poll.h>
#include <sys/wait.h>
#include <signal.h>
#include <sys/mman.h>
#include <liburing.h>
#include <sys/eventfd.h>
#include <stdatomic.h>

int g_aof_fd = -1;
pid_t g_bgsave_pid = -1;
long long g_bgsave_last_start_ms = 0;
long long g_bgsave_last_end_ms = 0;
unsigned long long g_dirty_counter = 0;

/* T2 review: AOF 线程（reap，写）与主线程（SAVE/BGSNAPSHOT/finalize，读/写）
 * 并发访问该偏移。声明为 _Atomic 使所有读写（含 += 复合赋值）原子化，消除跨线程 C 数据竞态。 */
static _Atomic long long g_aof_write_offset = 0;
static long long g_aof_write_submitted = 0;

static int g_persist_recovering = 0;
static long long g_recover_last_total_ms = 0;
static long long g_recover_last_dump_ms = 0;
static long long g_recover_last_aof_ms = 0;
static unsigned long long g_recover_mmap_attempts = 0;
static unsigned long long g_recover_mmap_success = 0;
static unsigned long long g_recover_mmap_fallbacks = 0;
static unsigned long long g_recover_last_mmap_bytes = 0;
static unsigned long long g_recover_last_fread_bytes = 0;
static unsigned long long g_recover_last_tail_bytes = 0;

static int g_bgsave_status = 0;
static unsigned long long g_bgsave_base_dirty = 0;
static long long g_last_snapshot_ms = 0;

static int g_aof_disabled = 0;

int persist_aof_disable(void) {
    g_aof_disabled = 1;
    return 0;
}

static pid_t g_bgrewrite_pid = -1;
static int g_bgrewrite_status = 0;
static char g_rewrite_tmp_path[512] = {0};

typedef struct rewrite_buf_node_s {
    unsigned char *data;
    size_t len;
    struct rewrite_buf_node_s *next;
} rewrite_buf_node_t;

static rewrite_buf_node_t *g_rewrite_buf_head = NULL;
static rewrite_buf_node_t *g_rewrite_buf_tail = NULL;
static pthread_mutex_t g_rewrite_buf_lock = PTHREAD_MUTEX_INITIALIZER;

/* ---- async persist inflight ring ----
 *
 * Fixed-size slot ring.  Each in-flight AOF batch owns one slot.
 * Both SQEs (write + fsync) carry the slot pointer as user_data.
 *
 * Group-commit (Redis-style): multiple commands accumulate in a
 * global buffer, then one write + one fsync covers the entire batch.
 * 先回复：回包不等 fsync（由 MAX_OUTSTANDING 背压界住崩溃窗口），AOF 线程原序落盘。
 */
#define PERSIST_INFLIGHT_SIZE 1024

typedef struct persist_slot_s {
    unsigned char *aof_buf;          /* 追加目标（write-in-place），跨批复用 */
    size_t         aof_len;
    size_t         aof_cap;          /* aof_buf 已分配容量 */
    long long      base_offset;      /* 本槽首个字节的 AOF 偏移 */
    int            cqe_seen;
    int            cqe_ok;
    int            last_error;
    int            in_use;
    struct persist_slot_s *ready_next, *completed_next;
} persist_slot_t;

static persist_slot_t g_inflight[PERSIST_INFLIGHT_SIZE];

/* ---- write-in-place slot 链表（T1，主线程） ----
 * g_ready / g_completed / g_free 为槽回收池。
 * g_outstanding = ready + in-flight 批数（slot_push_ready +=，slot_push_completed -=）。
 */
static persist_slot_t *g_cur_slot = NULL;
static long long g_batch_start_us = 0;     /* 当前批首条命令的时间戳（group-commit 窗口计时） */
static long long g_max_batch_age_us = 0;   /* 观测：handoff 时最大批龄（崩溃窗口上界，实验用） */
static persist_slot_t *g_ready_head = NULL, *g_ready_tail = NULL;
static persist_slot_t *g_completed_head = NULL, *g_completed_tail = NULL;
static persist_slot_t *g_free_slots = NULL;
static pthread_mutex_t g_slot_lock = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int g_outstanding = 0;  /* ready + in-flight 批数 */
#define MAX_OUTSTANDING 16
#define AOF_SLOT_INIT (64 * 1024)
#define AOF_SLOT_MAX  (4 * 1024 * 1024)

/* ---- global AOF buffer for group-commit batching ---- */
#define AOF_BUF_INIT_SIZE (64 * 1024)
#define AOF_BUF_MAX_SIZE  (4 * 1024 * 1024)

static unsigned char *g_aof_buf = NULL;
static size_t g_aof_buf_len = 0;
static size_t g_aof_buf_cap = 0;

static int g_persist_eventfd = -1;
static int g_persist_fatal_error = 0;
static int g_persist_aof_registered = 0;

static int g_persist_uring_ready = 0;
static struct io_uring g_persist_uring;

/* ---- AOF 线程接管 io_uring（T2，SINGLE_ISSUER） ----
 * io_uring get_sqe/prep/submit/reap 只在 AOF 线程内做（T2 起 SINGLE_ISSUER）。
 * 主线程不可再 submit_and_wait：SAVE/BGREWRITE/close 的 drain 走跨线程 fence。
 * 同步模式（aof_fsync_sync）不建线程，仍走主线程原 g_aof_buf 路径。 */
static int g_aof_wake_efd = -1;         /* 主线程 → AOF 线程 */
static int g_aof_complete_efd = -1;     /* AOF 线程 → 主线程（io_uring_fd 返回语义「批完成」） */
static pthread_t g_aof_thread;
static int g_aof_thread_created = 0;
static pthread_mutex_t g_work_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_work_flags = 0;
#define AOF_WORK_STOP 1
#define AOF_WORK_DRAIN 2
#define AOF_WORK_REREGISTER 4

/* ---- 跨线程 fence 原语（主线程 submit_and_wait 的替代） ---- */
static pthread_mutex_t g_fence_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_fence_cond = PTHREAD_COND_INITIALIZER;
static int g_drain_done = 0;
static int g_reregister_done = 0;
static int g_reregister_fd = -1;

/* ---- write-in-place slot 链表辅助（T1；仅主线程调用，调用点见注释） ---- */

static void slot_push_free(persist_slot_t *s) {   /* 仅主线程 */
    /* 保留 aof_buf/aof_cap（write-in-place 缓冲跨批复用，勿 memset 掉）；
     * 只重置本批游标与链表链接。 */
    s->aof_len = 0;
    s->base_offset = 0;
    s->cqe_seen = 0;
    s->cqe_ok = 0;
    s->last_error = 0;
    s->in_use = 0;
    s->completed_next = NULL;
    s->ready_next = g_free_slots;
    g_free_slots = s;
}

static persist_slot_t *slot_pop_free(void) {      /* 仅主线程 */
    persist_slot_t *s = g_free_slots;
    if (s) {
        g_free_slots = s->ready_next; s->ready_next = NULL;
        s->in_use = 1;   /* 进入使用：g_cur_slot → ready → in-flight → completed */
    }
    return s;
}

static void slot_push_ready(persist_slot_t *s) {  /* 持 g_slot_lock 调用 */
    s->ready_next = NULL;
    if (g_ready_tail) g_ready_tail->ready_next = s; else g_ready_head = s;
    g_ready_tail = s; g_outstanding++;
}

static persist_slot_t *slot_pop_ready(void) {     /* 持 g_slot_lock 调用 */
    persist_slot_t *s = g_ready_head;
    if (s) { g_ready_head = s->ready_next; if (!g_ready_head) g_ready_tail = NULL; s->ready_next = NULL; }
    return s;
}

static void slot_push_completed(persist_slot_t *s) { /* 持 g_slot_lock 调用 */
    s->completed_next = NULL;
    if (g_completed_tail) g_completed_tail->completed_next = s; else g_completed_head = s;
    g_completed_tail = s; g_outstanding--;
}

static persist_slot_t *slot_pop_completed_all(void) { /* 持 g_slot_lock 调用 */
    persist_slot_t *h = g_completed_head;
    g_completed_head = g_completed_tail = NULL;
    return h;
}

/* ---- 跨线程 fence 原语（主线程等待 AOF 线程完成 drain/reregister） ---- */

static void fence_signal(int *done_flag) {
    pthread_mutex_lock(&g_fence_mutex);
    *done_flag = 1;
    pthread_cond_broadcast(&g_fence_cond);
    pthread_mutex_unlock(&g_fence_mutex);
}
static void fence_wait(int *done_flag) {
    pthread_mutex_lock(&g_fence_mutex);
    while (!*done_flag) pthread_cond_wait(&g_fence_cond, &g_fence_mutex);
    pthread_mutex_unlock(&g_fence_mutex);
}
static void aof_work_request(int flag) {
    pthread_mutex_lock(&g_work_mutex);
    g_work_flags |= flag;
    pthread_mutex_unlock(&g_work_mutex);
    uint64_t one = 1; (void)!write(g_aof_wake_efd, &one, sizeof(one));
}

/* completed 槽回收（主线程，T2）：从 completed 弹出槽，仅归还 free（release 链表已删除，回包与落盘解耦）。 */
static void drain_completed(void) {
    pthread_mutex_lock(&g_slot_lock);
    persist_slot_t *h = slot_pop_completed_all();
    pthread_mutex_unlock(&g_slot_lock);
    for (persist_slot_t *s = h; s; ) {
        persist_slot_t *next = s->completed_next;
        s->completed_next = NULL;
        s->last_error = 0;
        slot_push_free(s);
        s = next;
    }
}

/* ---- uring lifecycle ---- */

static int persist_uring_init_once(void) {
    if (g_persist_uring_ready) return 0;

    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    p.flags = IORING_SETUP_SINGLE_ISSUER
            | IORING_SETUP_COOP_TASKRUN
            | IORING_SETUP_SQPOLL;
    p.sq_thread_idle = 2000;

    int rc = io_uring_queue_init_params(1024, &g_persist_uring, &p);
    if (rc != 0) {
        fprintf(stderr, "persist: io_uring init(SQPOLL) failed rc=%d(%s), retrying without SQPOLL\n",
                rc, strerror(-rc));
        memset(&p, 0, sizeof(p));
        p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;
        rc = io_uring_queue_init_params(1024, &g_persist_uring, &p);
    }
    if (rc != 0) {
        /* 第三次回退：不带任何 flags 的裸 io_uring。
         * SINGLE_ISSUER / COOP_TASKRUN 都是 kernel 6.0+ 才有的 flag，5.15 这类老内核会
         * 直接拒绝（实测 EPERM），于是前两次都失败 → g_persist_fatal_error=1 →
         * 此后所有 append 返回 ERR，进程看起来一切正常但 AOF 永远 0 字节、
         * durable_offset 冻结，复制从机还会据此上报一个自己恢复不了的 offset。
         * 丢掉的只是性能优化（少一次自旋锁/少一次 IPI），语义不受影响：
         * ring 仍只由 AOF 线程提交，单 issuer 纪律照旧。 */
        fprintf(stderr, "persist: io_uring init(SINGLE_ISSUER|COOP_TASKRUN) failed rc=%d(%s), "
                        "retrying with plain flags\n", rc, strerror(-rc));
        memset(&p, 0, sizeof(p));
        rc = io_uring_queue_init_params(1024, &g_persist_uring, &p);
    }
    if (rc != 0) {
        /* 静默返回会让上层只看到 "applied 在涨、durable 不动"，排查成本极高
         * （复制从机的 AOF 全空就是这么暴露出来的），因此必须打出来。 */
        fprintf(stderr, "persist: io_uring init failed rc=%d(%s) — AOF 将无法落盘，"
                        "此后所有 append 返回 KVS_PERSIST_ERR\n", rc, strerror(-rc));
        return -1;
    }

    g_persist_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_persist_eventfd < 0) {
        fprintf(stderr, "persist: eventfd failed errno=%d(%s)\n", errno, strerror(errno));
        io_uring_queue_exit(&g_persist_uring);
        return -1;
    }
    if (io_uring_register_eventfd(&g_persist_uring, g_persist_eventfd) != 0) {
        fprintf(stderr, "persist: io_uring_register_eventfd failed errno=%d(%s)\n",
                errno, strerror(errno));
        close(g_persist_eventfd);
        g_persist_eventfd = -1;
        io_uring_queue_exit(&g_persist_uring);
        return -1;
    }
    g_persist_uring_ready = 1;

    if (g_aof_fd >= 0) {
        if (io_uring_register_files(&g_persist_uring, &g_aof_fd, 1) == 0)
            g_persist_aof_registered = 1;
    }
    return 0;
}

static void persist_uring_close(void) {
    if (!g_persist_uring_ready) return;
    if (g_persist_aof_registered) {
        io_uring_unregister_files(&g_persist_uring);
        g_persist_aof_registered = 0;
    }
    if (g_persist_eventfd >= 0) {
        io_uring_unregister_eventfd(&g_persist_uring);
        close(g_persist_eventfd);
        g_persist_eventfd = -1;
    }
    io_uring_queue_exit(&g_persist_uring);
    g_persist_uring_ready = 0;
}

/* BGREWRITE 后在主线程发起跨线程 reregister fence（实际换 fd 在 AOF 线程做）。 */
void persist_reregister_aof_fd(void) {
    if (!g_aof_thread_created || g_aof_fd < 0) return;
    g_reregister_fd = g_aof_fd;           /* 新 fd 已由 finalize_rewrite_parent 打开 */
    g_reregister_done = 0;
    aof_work_request(AOF_WORK_REREGISTER);
    fence_wait(&g_reregister_done);
}

/* ---- AOF 线程：CQE 收割 / 提交 ready / drain / reregister / 主循环 ----
 * T2 起 io_uring 归 AOF 线程（SINGLE_ISSUER），主线程不再 get_sqe/submit/reap。 */

static int submit_one_slot(persist_slot_t *s);   /* 定义见下文（AOF 线程唯一调用方） */
static void handoff_current_slot(void);          /* 定义见下文（主线程 handoff + 背压） */
static void persist_flush_pending_force(void);   /* 定义见下文（强制 flush，drain/槽满用） */

/* 收割 CQE（AOF 线程内）：双 CQE 完成 → 槽挂 completed（持锁）并通知主线程。
 * 错误槽置 g_persist_fatal_error。 */
static void reap_completions_inline(void) {
    struct io_uring_cqe *cqe;
    while (io_uring_peek_cqe(&g_persist_uring, &cqe) == 0) {
        persist_slot_t *s = io_uring_cqe_get_data(cqe);
        if (s) {
            if (cqe->res > 0) { g_aof_write_offset += cqe->res; s->cqe_ok++; }
            else if (cqe->res == 0) s->cqe_ok++;
            else s->last_error = cqe->res;
            s->cqe_seen++;
        }
        io_uring_cqe_seen(&g_persist_uring, cqe);
        if (s && s->cqe_seen == 2) {
            if (s->cqe_ok != 2) {
                fprintf(stderr, "persist: CQE error cqe_ok=%d last_error=%d\n",
                        s->cqe_ok, s->last_error);
                g_persist_fatal_error = 1;
            }
            pthread_mutex_lock(&g_slot_lock);
            slot_push_completed(s);
            pthread_mutex_unlock(&g_slot_lock);
        }
    }
}
static void signal_complete_efd(void) {
    uint64_t one = 1; (void)!write(g_aof_complete_efd, &one, sizeof(one));
}

/* AOF 线程侧 drain：先提交全部 ready，再等 g_outstanding==0（in-flight 全部落盘并回 completed）。
 * signal 是否置由 calling-as-request 决定（I1）：
 *   - AOF_WORK_DRAIN（SAVE/BGREWRITE/close 的跨线程 fence）→ signal g_drain_done；
 *   - REREGISTER 的内部 drain 属于 helper，不碰 g_drain_done，避免与主线程等待中的
 *     g_drain_done 共享同一 CV 产生歧义/spurious release。 */
static void aof_thread_drain_impl(int signal_main) {
    for (;;) {
        pthread_mutex_lock(&g_slot_lock);
        persist_slot_t *s = slot_pop_ready();
        pthread_mutex_unlock(&g_slot_lock);
        if (!s) break;
        if (submit_one_slot(s) != 0) { g_persist_fatal_error = 1; break; }
    }
    while (atomic_load_explicit(&g_outstanding, memory_order_acquire) > 0) {
        io_uring_submit_and_wait(&g_persist_uring, 1);
        reap_completions_inline();
        signal_complete_efd();
    }
    if (signal_main) fence_signal(&g_drain_done);
}
static void aof_thread_drain_request(void) {
    aof_thread_drain_impl(1);
}

/* 提交全部 ready 槽（AOF 线程：主循环每次唤醒时调用） */
static void aof_thread_submit_all_ready(void) {
    if (g_persist_fatal_error) return;
    for (;;) {
        pthread_mutex_lock(&g_slot_lock);
        persist_slot_t *s = slot_pop_ready();
        pthread_mutex_unlock(&g_slot_lock);
        if (!s) break;
        if (submit_one_slot(s) != 0) { g_persist_fatal_error = 1; break; }
    }
}

/* BGREWRITE 完成后把新 aof_fd 换成 files 注册（AOF 线程）：先 drain 清 ready+in-flight，再换 fd。 */
static void aof_thread_reregister_fd(void) {
    int fd = g_reregister_fd;
    aof_thread_drain_impl(0);          /* 内部 drain（I1）：不 signal g_drain_done */
    if (g_persist_aof_registered) io_uring_unregister_files(&g_persist_uring);
    if (fd >= 0 && io_uring_register_files(&g_persist_uring, &fd, 1) == 0)
        g_persist_aof_registered = 1;
    else
        g_persist_aof_registered = 0;
    fence_signal(&g_reregister_done);
}

static void *aof_thread_main(void *arg) {
    (void)arg;
    if (persist_uring_init_once() != 0) { g_persist_fatal_error = 1; return NULL; }
    for (;;) {
        struct pollfd pfd[2];
        pfd[0].fd = g_aof_wake_efd;   pfd[0].events = POLLIN;
        pfd[1].fd = g_persist_eventfd; pfd[1].events = POLLIN;
        poll(pfd, 2, -1);

        /* wake efd：主线程请求 STOP/DRAIN/REREGISTER 或新 ready 槽的信号 */
        if (pfd[0].revents & POLLIN) {
            uint64_t v; while (read(g_aof_wake_efd, &v, sizeof(v)) > 0) {}
            pthread_mutex_lock(&g_work_mutex);
            int flags = g_work_flags; g_work_flags = 0;
            pthread_mutex_unlock(&g_work_mutex);
            if (flags & AOF_WORK_REREGISTER) aof_thread_reregister_fd();
            if (flags & AOF_WORK_DRAIN)     aof_thread_drain_request();
            if (flags & AOF_WORK_STOP)      break;
            aof_thread_submit_all_ready();
        }
        /* uring eventfd（CQE 到达）：收割 + 通知主线程 re-fetch（drain_completed） */
        if (pfd[1].revents & POLLIN) {
            uint64_t v; while (read(g_persist_eventfd, &v, sizeof(v)) > 0) {}
            reap_completions_inline();
            signal_complete_efd();
        }
    }
    persist_uring_close();
    return NULL;
}

/* ---- submit ----（AOF 线程唯一调用 submit_one_slot；主线程不再 submit） ---- */

/* ---- reap completions（主线程）：只是回收 completed 槽并释放 conn（T2） ---- */

void persist_reap_completions(void) {
    drain_completed();
}

int persist_uring_fd(void) {
    return g_aof_complete_efd;
}

/* 跨线程 drain fence：同步模式/无线程走原 submit_and_wait 路径；否则请求 AOF 线程 drain 并等待。 */
void persist_drain_pending(void) {
    if (g_cfg.aof_fsync_sync || !g_aof_thread_created) {
        /* 同步模式/无线程：持久化已同步落盘，只要 flush 累积的全局缓冲即可。
         * （同步路径只用 g_aof_buf，从不 push slot，g_outstanding 恒为 0，
         *   原 while(g_outstanding>0) 循环不可达，已移除；I2。） */
        if (!g_persist_uring_ready) return;
        persist_flush_pending_force();
        return;
    }
    if (g_cur_slot && g_cur_slot->aof_len > 0) handoff_current_slot();  /* handoff 内含背压，无 signal */
    g_drain_done = 0;
    aof_work_request(AOF_WORK_DRAIN);
    fence_wait(&g_drain_done);
}

/* ---- synchronous uring helpers ---- */

static int persist_uring_wait_single(void) {
    struct io_uring_cqe *cqe = NULL;
    int rc = io_uring_submit_and_wait(&g_persist_uring, 1);
    if (rc < 0) return -1;
    rc = io_uring_wait_cqe(&g_persist_uring, &cqe);
    if (rc < 0 || !cqe) return -1;
    rc = cqe->res;
    io_uring_cqe_seen(&g_persist_uring, cqe);
    return rc;
}

static int persist_write_fd_uring(int fd, const unsigned char *buf, size_t len, off_t *offset) {
    size_t written = 0;
    if (persist_uring_init_once() != 0) return -1;
    while (written < len) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_persist_uring);
        size_t chunk = len - written;
        int rc;
        if (!sqe) return -1;
        io_uring_prep_write(sqe, fd, buf + written, chunk, offset ? *offset : -1);
        rc = persist_uring_wait_single();
        if (rc <= 0) return -1;
        written += (size_t)rc;
        if (offset) *offset += rc;
    }
    return 0;
}

static int persist_fsync_fd_uring(int fd) {
    struct io_uring_sqe *sqe;
    int rc;
    if (persist_uring_init_once() != 0) return -1;
    sqe = io_uring_get_sqe(&g_persist_uring);
    if (!sqe) return -1;
    io_uring_prep_fsync(sqe, fd, 0);
    rc = persist_uring_wait_single();
    return rc < 0 ? -1 : 0;
}

static int persist_write_fd_sync(int fd, const unsigned char *buf, size_t len, off_t *offset) {
    size_t written = 0;
    while (written < len) {
        ssize_t rc = pwrite(fd, buf + written, len - written, offset ? *offset : -1);
        if (rc <= 0) return -1;
        written += (size_t)rc;
        if (offset) *offset += rc;
    }
    return 0;
}

static int persist_write_fd_best_effort(int fd, const unsigned char *buf, size_t len, off_t *offset) {
    /* T2：AOF 线程独享 io_uring（SINGLE_ISSUER），主线程不可用共享 ring；
     * 有 AOF 线程时主线程写/fsync 一律走直接系统调用。 */
    if (!g_aof_thread_created) {
        if (persist_write_fd_uring(fd, buf, len, offset) == 0) return 0;
    }
    return persist_write_fd_sync(fd, buf, len, offset);
}

int persist_write_raw_fd(int fd, const unsigned char *buf, size_t len, long long *offset_io) {
    off_t off;
    if (fd < 0 || !buf) return -1;
    off = offset_io ? (off_t)(*offset_io) : lseek(fd, 0, SEEK_CUR);
    if (off < 0) return -1;
    if (persist_write_fd_best_effort(fd, buf, len, &off) != 0) return -1;
    if (offset_io) *offset_io = (long long)off;
    return 0;
}

static int persist_fsync_fd_best_effort(int fd) {
    /* T2：有 AOF 线程时主线程不用共享 ring，直接 fsync。 */
    if (!g_aof_thread_created) {
        if (persist_fsync_fd_uring(fd) == 0) return 0;
    }
    return fsync(fd);
}

int persist_fsync_fd(int fd) {
    return persist_fsync_fd_best_effort(fd);
}

/* ---- recovery (mmap / fread) ---- */

static int replay_file_fread(const char *path, unsigned long long skip_bytes) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return 0;
    if (skip_bytes > 0 && fseeko(fp, (off_t)skip_bytes, SEEK_SET) != 0) {
        fclose(fp);
        return 0;
    }
    unsigned char buf[BUFFER_CAP];
    size_t len = 0, n;
    unsigned long long total = 0;
    while ((n = fread(buf + len, 1, sizeof(buf) - len, fp)) > 0) {
        len += n;
        total += (unsigned long long)n;
        parse_resp_stream(NULL, buf, &len, 1);
    }
    g_recover_last_fread_bytes += total;
    fclose(fp);
    return 0;
}

static int replay_file_mmap(const char *path, unsigned long long skip_bytes) {
    int fd;
    struct stat st;
    unsigned char *mapped;
    size_t len;

    g_recover_mmap_attempts++;

    fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    if (fstat(fd, &st) != 0) {
        g_recover_mmap_fallbacks++;
        close(fd);
        return replay_file_fread(path, skip_bytes);
    }
    if ((unsigned long long)st.st_size <= skip_bytes) {
        close(fd);
        return 0;
    }

    mapped = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        g_recover_mmap_fallbacks++;
        close(fd);
        return replay_file_fread(path, skip_bytes);
    }

    g_recover_mmap_success++;
    g_recover_last_mmap_bytes += (unsigned long long)st.st_size - skip_bytes;

    len = (size_t)st.st_size - (size_t)skip_bytes;
    parse_resp_stream(NULL, mapped + skip_bytes, &len, 1);
    g_recover_last_tail_bytes += (unsigned long long)len;

    munmap(mapped, (size_t)st.st_size);
    close(fd);
    return 0;
}

static int replay_file(const char *path, unsigned long long skip_bytes) {
    return replay_file_mmap(path, skip_bytes);
}

unsigned long long replay_dump_file(const char *path) {
    int fd;
    struct stat st;
    unsigned char *mapped;
    size_t pos = 0, size;
    unsigned long long aof_offset = 0;

    fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    if (fstat(fd, &st) != 0) { close(fd); return 0; }
    if (st.st_size <= 0) { close(fd); return 0; }

    mapped = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) { close(fd); return 0; }
    size = (size_t)st.st_size;
    g_recover_mmap_success++;
    g_recover_last_mmap_bytes += (unsigned long long)size;

    if (pos + sizeof(aof_offset) > size) goto out;
    memcpy(&aof_offset, mapped + pos, sizeof(aof_offset));
    pos += sizeof(aof_offset);

    while (pos + 2 + 4 <= size) {
        uint8_t engine_id, flags;
        uint32_t klen, vlen;
        char *key, *value;

        engine_id = mapped[pos++];
        if (engine_id < 1 || engine_id > 5) {
            fprintf(stderr, "replay_dump_file: invalid engine_id %u at pos %zu (old format KVSD file?)\n",
                (unsigned int)engine_id, pos - 1);
            break;
        }

        if (pos + 1 > size) break;
        flags = mapped[pos++];

        if (pos + 4 > size) break;
        memcpy(&klen, mapped + pos, sizeof(klen));
        pos += sizeof(klen);
        if (pos + klen > size) break;

        key = (char *)kvs_malloc(klen + 1);
        if (!key) break;
        if (klen > 0) memcpy(key, mapped + pos, klen);
        key[klen] = '\0';
        pos += klen;

        if (pos + 4 > size) { kvs_free(key); break; }

        memcpy(&vlen, mapped + pos, sizeof(vlen));
        pos += sizeof(vlen);
        if (pos + vlen > size) { kvs_free(key); break; }

        value = (char *)kvs_malloc(vlen + 1);
        if (!value) { kvs_free(key); break; }
        if (vlen > 0) memcpy(value, mapped + pos, vlen);
        value[vlen] = '\0';
        pos += vlen;

        switch (engine_id) {
        case KVS_ENGINE_ARRAY:
            kvs_array_set(&global_array, key, value);
            break;
        case KVS_ENGINE_RBTREE:
            kvs_rbtree_set(&global_rbtree, key, value);
            break;
        case KVS_ENGINE_HASH:
            kvs_hash_set(&global_hash, key, value);
            break;
        case KVS_ENGINE_SKIPTABLE:
            kvs_skiptable_set(&global_skiptable, key, value);
            break;
        case KVS_ENGINE_DOC:
            {
                char *tok, *saveptr;
                char *dup = (char *)kvs_malloc(vlen + 1);
                if (dup) {
                    memcpy(dup, value, vlen);
                    dup[vlen] = '\0';
                    tok = strtok_r(dup, " ", &saveptr);
                    while (tok) {
                        char *eq = strchr(tok, '=');
                        if (eq) {
                            *eq = '\0';
                            kvs_doc_set(&global_doc, key, tok, eq + 1);
                        }
                        tok = strtok_r(NULL, " ", &saveptr);
                    }
                    kvs_free(dup);
                }
            }
            break;
        default:
            break;
        }

        if (flags & KVSD_FLAG_HAS_EXPIRE) {
            uint64_t expire_at_ms;
            if (pos + sizeof(expire_at_ms) <= size) {
                memcpy(&expire_at_ms, mapped + pos, sizeof(expire_at_ms));
                pos += sizeof(expire_at_ms);
                long long ttl_ms = (long long)expire_at_ms - kvs_now_ms();
                if (ttl_ms > 0) {
                    kvs_expire_set(&global_expire, engine_id, key, ttl_ms);
                }
            } else {
                kvs_free(key);
                kvs_free(value);
                break;
            }
        }

        kvs_free(key);
        kvs_free(value);

        if (pos >= size) break;
    }

out:
    g_recover_last_tail_bytes += (unsigned long long)pos;
    munmap(mapped, size);
    close(fd);
    return aof_offset;
}

/* ---- snapshot / bgrewrite ---- */

static int persist_flush_aof_fd(int fd) {
    if (fd < 0) return -1;
    if (persist_fsync_fd_best_effort(fd) != 0) return -1;
    return 0;
}

static int persist_save_dump_to(const char *path, unsigned long long aof_offset) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    int rc;
    if (fd < 0) return -1;
    rc = kvs_dump_to_fd(fd, aof_offset);
    if (rc == 0 && persist_fsync_fd(fd) != 0) rc = -1;
    close(fd);
    return rc;
}

static int persist_write_aof_snapshot_to(const char *path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    int rc;
    if (fd < 0) return -1;
    rc = kvs_snapshot_to_fd(fd);
    if (rc == 0 && persist_fsync_fd(fd) != 0) rc = -1;
    close(fd);
    return rc;
}

static void persist_mark_snapshot_success(unsigned long long snap_dirty) {
    g_bgsave_last_end_ms = kvs_now_ms();
    g_last_snapshot_ms = g_bgsave_last_end_ms;
    if (g_dirty_counter >= snap_dirty) g_dirty_counter -= snap_dirty;
    else g_dirty_counter = 0;
}

static void free_rewrite_buffer_locked(void) {
    rewrite_buf_node_t *cur = g_rewrite_buf_head;
    while (cur) {
        rewrite_buf_node_t *next = cur->next;
        kvs_free(cur->data);
        kvs_free(cur);
        cur = next;
    }
    g_rewrite_buf_head = NULL;
    g_rewrite_buf_tail = NULL;
}

static int append_to_rewrite_buffer(const unsigned char *buf, size_t len) {
    rewrite_buf_node_t *node = (rewrite_buf_node_t *)kvs_malloc(sizeof(*node));
    if (!node) return -1;
    node->data = (unsigned char *)kvs_malloc(len);
    if (!node->data) {
        kvs_free(node);
        return -1;
    }
    memcpy(node->data, buf, len);
    node->len = len;
    node->next = NULL;

    pthread_mutex_lock(&g_rewrite_buf_lock);
    if (g_bgrewrite_pid <= 0) {
        pthread_mutex_unlock(&g_rewrite_buf_lock);
        kvs_free(node->data);
        kvs_free(node);
        return 0;
    }
    if (g_rewrite_buf_tail) g_rewrite_buf_tail->next = node;
    else g_rewrite_buf_head = node;
    g_rewrite_buf_tail = node;
    pthread_mutex_unlock(&g_rewrite_buf_lock);
    return 0;
}

static int finalize_rewrite_parent(void) {
    int fd = open(g_rewrite_tmp_path, O_WRONLY | O_APPEND);
    if (fd < 0) return -1;

    pthread_mutex_lock(&g_rewrite_buf_lock);
    long long off = lseek(fd, 0, SEEK_END);
    for (rewrite_buf_node_t *cur = g_rewrite_buf_head; cur; cur = cur->next) {
        if (off < 0 || persist_write_raw_fd(fd, cur->data, cur->len, &off) != 0) {
            pthread_mutex_unlock(&g_rewrite_buf_lock);
            close(fd);
            return -1;
        }
    }
    pthread_mutex_unlock(&g_rewrite_buf_lock);

    if (persist_flush_aof_fd(fd) != 0) {
        close(fd);
        return -1;
    }
    close(fd);

    if (rename(g_rewrite_tmp_path, g_cfg.aof_path) != 0) return -1;

    if (g_aof_fd >= 0) close(g_aof_fd);
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_offset = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_offset < 0) g_aof_write_offset = 0;
    g_aof_write_submitted = g_aof_write_offset;

    persist_reregister_aof_fd();

    pthread_mutex_lock(&g_rewrite_buf_lock);
    free_rewrite_buffer_locked();
    pthread_mutex_unlock(&g_rewrite_buf_lock);

    return 0;
}

/* 创建 AOF 线程（仅异步模式：ALWAYS && !sync）。失败置 fatal，由调用方处理。 */
static int aof_thread_start(void) {
    g_aof_wake_efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    g_aof_complete_efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_aof_wake_efd < 0 || g_aof_complete_efd < 0) return -1;
    if (pthread_create(&g_aof_thread, NULL, aof_thread_main, NULL) != 0) return -1;
    g_aof_thread_created = 1;
    return 0;
}

int persist_init(void) {
    if (g_aof_disabled) {
        g_aof_fd = -1;
        return 0;
    }
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_offset = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_offset < 0) g_aof_write_offset = 0;
    g_aof_write_submitted = g_aof_write_offset;
    /* T1：异步路径种子化 free 槽池（同步路径保留 g_aof_buf，不占槽）。 */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && !g_cfg.aof_fsync_sync) {
        for (int i = 0; i < PERSIST_INFLIGHT_SIZE; ++i)
            slot_push_free(&g_inflight[i]);
        /* T2：AOF 线程接管 io_uring（SINGLE_ISSUER）。主线程不再 uring_init_once。 */
        if (aof_thread_start() != 0) g_persist_fatal_error = 1;
    }
    return 0;
}

void persist_close(void) {
    if (g_aof_thread_created) {
        persist_drain_pending();
        aof_work_request(AOF_WORK_STOP);
        pthread_join(g_aof_thread, NULL);           /* 线程内 persist_uring_close() */
        g_aof_thread_created = 0;
        if (g_aof_wake_efd >= 0) close(g_aof_wake_efd);
        if (g_aof_complete_efd >= 0) close(g_aof_complete_efd);
        g_aof_wake_efd = g_aof_complete_efd = -1;
    } else {
        /* 同步模式：原 persist_close 逻辑（主线程持有 uring，直接 drain + close） */
        persist_drain_pending();
        persist_uring_close();
    }
    if (g_aof_fd >= 0) close(g_aof_fd);
    g_aof_fd = -1;
    g_aof_write_offset = 0;
    g_aof_write_submitted = 0;
    /* 释放各写槽 write-in-place 缓冲（drain 已把槽回收进 free 链表） */
    for (int i = 0; i < PERSIST_INFLIGHT_SIZE; ++i) {
        kvs_free(g_inflight[i].aof_buf);
        g_inflight[i].aof_buf = NULL;
        g_inflight[i].aof_cap = 0;
    }
    g_free_slots = NULL;
    g_ready_head = g_ready_tail = NULL;
    g_completed_head = g_completed_tail = NULL;
    g_cur_slot = NULL;
    g_outstanding = 0;
    kvs_free(g_aof_buf);
    g_aof_buf = NULL;
    g_aof_buf_len = 0;
    g_aof_buf_cap = 0;
}

/* AOF 运行时内部状态（供 INFO 诊断）。复制从机出现 "applied 在涨但 durable 不动" 时，
 * 第一个要看的就是这里：aof_fatal=1 说明 AOF 线程的 io_uring 提交/CQE 出过错，
 * 此后 persist_append_prepare() 一律返回 KVS_PERSIST_ERR，复制数据只进内存不落 AOF。 */
void persist_aof_debug_state(kvs_aof_debug_t *out) {
    if (!out) return;
    out->aof_fd = g_aof_fd;
    out->aof_disabled = g_aof_disabled;
    out->aof_fatal = g_persist_fatal_error;
    out->aof_thread_created = g_aof_thread_created;
    out->aof_write_submitted = g_aof_write_submitted;
    out->aof_write_offset = g_aof_write_offset;
    out->aof_outstanding = (int)atomic_load_explicit(&g_outstanding, memory_order_relaxed);
}

/* 全量同步完成后把 AOF 重定基线：drain 在途写 → 截断为 0 → 计数归零。
 * 语义：快照就是新的持久化基线，快照之前的 AOF 内容已被取代；留着不仅浪费空间，
 * 还会在恢复时把旧命令重放到快照之上（复活已删 key、非幂等命令双倍执行）。
 * 调用方（从机 fullsync 收尾）必须同步把 dump 头部的 aof_offset 也改成 0。 */
int persist_aof_rebase(void) {
    if (g_aof_disabled || g_aof_fd < 0) return -1;
    persist_drain_pending();                       /* 等在途批次落盘，之后才可安全截断 */
    if (ftruncate(g_aof_fd, 0) != 0) {
        fprintf(stderr, "persist: aof rebase ftruncate failed errno=%d(%s)\n",
                errno, strerror(errno));
        return -1;
    }
    g_aof_write_offset = 0;
    g_aof_write_submitted = 0;
    g_cur_slot = NULL;
    return 0;
}

int persist_set_aof_policy(kvs_aof_fsync_policy_t policy) {
    if (policy != KVS_AOF_FSYNC_OFF && policy != KVS_AOF_FSYNC_ALWAYS) return -1;
    g_cfg.aof_fsync = policy;
    return 0;
}

kvs_aof_fsync_policy_t persist_get_aof_policy(void) {
    return g_cfg.aof_fsync;
}

const char *persist_aof_policy_name(void) {
    return g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS ? "always" : "off";
}

int persist_force_aof_flush(void) {
    if (g_aof_fd < 0) return -1;
    persist_drain_pending();
    if (persist_fsync_fd_best_effort(g_aof_fd) != 0) return -1;
    return 0;
}

/* ---- group-commit append + flush ----
 *
 * Redis-style appendfsync always: response is sent immediately, the
 * batch write+fsync happens asynchronously.  Guarantee: data is on
 * disk before the next epoll cycle processes more commands.
 */

/* 同步直接写 AOF 缓冲 + fdatasync（对齐 redis：直接系统调用，无 io_uring/fsync 元数据开销）。 */
static int persist_sync_append_write(void) {
    if (!g_aof_buf || g_aof_buf_len == 0) return KVS_PERSIST_OK;
    ssize_t w = pwrite(g_aof_fd, g_aof_buf, g_aof_buf_len, (off_t)g_aof_write_offset);
    if (w < 0 || (size_t)w != g_aof_buf_len) return KVS_PERSIST_ERR;
    g_aof_write_offset += (long long)w;
    g_aof_write_submitted = g_aof_write_offset;
    if (fdatasync(g_aof_fd) != 0) return KVS_PERSIST_ERR;
    g_aof_buf_len = 0;
    return KVS_PERSIST_OK;
}

int persist_append_prepare(conn_t *c, const unsigned char *buf, size_t len,
                           unsigned char *resp, size_t resp_len) {
    (void)resp; (void)resp_len; (void)c;
    if (g_aof_fd < 0) return g_aof_disabled ? KVS_PERSIST_OK : KVS_PERSIST_ERR;
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) return KVS_PERSIST_OK;
    if (g_persist_fatal_error) return KVS_PERSIST_ERR;

    /* ---- 同步模式（--aof-fsync-sync / --aof-fsync-sync-batch）：保留原 g_aof_buf 逻辑不动 ---- */
    if (g_cfg.aof_fsync_sync) {
        if (persist_uring_init_once() != 0) return KVS_PERSIST_ERR;

        /* ensure global AOF buffer capacity */
        if (!g_aof_buf) {
            g_aof_buf_cap = AOF_BUF_INIT_SIZE;
            g_aof_buf = (unsigned char *)kvs_malloc(g_aof_buf_cap);
            if (!g_aof_buf) return KVS_PERSIST_ERR;
            g_aof_buf_len = 0;
        }
        while (g_aof_buf_len + len > g_aof_buf_cap) {
            size_t new_cap = g_aof_buf_cap * 2;
            unsigned char *new_buf = (unsigned char *)kvs_realloc(g_aof_buf, new_cap);
            if (!new_buf) return KVS_PERSIST_ERR;
            g_aof_buf = new_buf;
            g_aof_buf_cap = new_cap;
        }

        /* append AOF data to global buffer */
        memcpy(g_aof_buf + g_aof_buf_len, buf, len);
        g_aof_buf_len += len;

        g_aof_write_submitted += (long long)len;

        /* 同步模式已同步落盘，响应正常立即发。 */
        if (g_cfg.aof_fsync_per_command) {
            persist_sync_append_write();   /* 同步：已落盘，响应立即发 */
            return KVS_PERSIST_OK;
        }
        if (g_aof_buf_len >= AOF_BUF_MAX_SIZE)
            persist_flush_pending();
        if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);
        return KVS_PERSIST_OK;
    }

    /* ---- 异步（默认/逐条）：write-in-place 追加到当前槽 ----
     * T2：uring 由 AOF 线程 init/隶属（SINGLE_ISSUER），主线程在此只填槽，不做 init。 */

    if (!g_cur_slot) {
        g_cur_slot = slot_pop_free();
        while (!g_cur_slot) {          /* free 空 = 上一批未回收 → 背压 */
            persist_reap_completions(); sched_yield();
            g_cur_slot = slot_pop_free();
        }
        g_cur_slot->aof_len = 0;
        g_cur_slot->base_offset = g_aof_write_submitted;
        g_batch_start_us = kvs_now_us();   /* 批龄起点：group-commit 窗口计时 */
    }
    if (g_cur_slot->aof_len + len > g_cur_slot->aof_cap) {
        size_t need = g_cur_slot->aof_len + len;
        size_t new_cap = g_cur_slot->aof_cap ? g_cur_slot->aof_cap : AOF_SLOT_INIT;
        while (new_cap < need) new_cap *= 2;
        if (new_cap > AOF_SLOT_MAX) return KVS_PERSIST_ERR;
        unsigned char *nb = g_cur_slot->aof_buf
            ? kvs_realloc(g_cur_slot->aof_buf, new_cap)
            : (unsigned char *)kvs_malloc(new_cap);
        if (!nb) return KVS_PERSIST_ERR;
        g_cur_slot->aof_buf = nb;
        g_cur_slot->aof_cap = new_cap;
    }
    memcpy(g_cur_slot->aof_buf + g_cur_slot->aof_len, buf, len);
    g_cur_slot->aof_len += len;
    g_aof_write_submitted += (long long)len;

    /* 先回复：回包与落盘解耦，AOF 线程后台 fsync，背压 MAX_OUTSTANDING 界住崩溃窗口。 */

    if (g_cfg.aof_fsync_per_command || g_cur_slot->aof_len >= AOF_SLOT_MAX)
        persist_flush_pending_force();

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    return KVS_PERSIST_OK;
}

/* write-in-place 槽 handoff：把当前槽推入 ready 链表（持锁，受 MAX_OUTSTANDING 背压） */
static void handoff_current_slot(void) {
    if (!g_cur_slot || g_cur_slot->aof_len == 0) return;
    pthread_mutex_lock(&g_slot_lock);
    while (g_outstanding >= MAX_OUTSTANDING) {
        pthread_mutex_unlock(&g_slot_lock);
        /* 背压：收割 CQE 才能把槽转 completed（reap 尾部会 drain 回 free），驱动 g_outstanding 下降 */
        persist_reap_completions();
        sched_yield();
        pthread_mutex_lock(&g_slot_lock);
    }
    slot_push_ready(g_cur_slot);
    g_cur_slot = NULL;
    pthread_mutex_unlock(&g_slot_lock);
}

/* 提交失败兜底（AOF 线程内，T2 review）：AOF 线程绝不触碰主线程独占的 g_free_slots。
 * 把槽直接推入 completed 链表（持锁，g_outstanding 平衡），signal 主线程，
 * 由主线程 drain_completed 做 slot_push_free。仅谓词前置失败
 * （尚未 prep 任何 SQE，无 CQE 在途）时调用。 */
static void slot_abort_inflight(persist_slot_t *s) {
    pthread_mutex_lock(&g_slot_lock);
    s->cqe_ok = 0;
    s->last_error = -1;
    s->cqe_seen = 2;   /* 无 CQE 在途；标记双 CQE 已到，防止 reap 再推一次 */
    s->completed_next = NULL;
    slot_push_completed(s);
    pthread_mutex_unlock(&g_slot_lock);
    signal_complete_efd();
}

/* 提交一个槽的 write(IOSQE_IO_LINK) + fsync 两个 SQE；返回 0=已提交。T2 起仅 AOF 线程调用。 */
static int submit_one_slot(persist_slot_t *s) {
    struct io_uring_sqe *sqe_f;

    /* 需 write+fsync 两个 SQE。先确认剩余空间 >= 2 再 prep，否则早期失败时尚未 prep 任何
     * SQE：既不留半 prep（后续 submit 会写到本槽的隐患），也让已 pop 的槽安全归还 free 不泄漏。 */
    if (io_uring_sq_space_left(&g_persist_uring) < 2) {
        slot_abort_inflight(s);
        return -1;
    }
    struct io_uring_sqe *sqe_w = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_w) { slot_abort_inflight(s); return -1; }
    io_uring_prep_write(sqe_w, g_persist_aof_registered ? 0 : g_aof_fd,
                        s->aof_buf, s->aof_len, (off_t)s->base_offset);
    sqe_w->flags |= IOSQE_IO_LINK;
    if (g_persist_aof_registered) sqe_w->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe_w, s);
    sqe_f = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_f) {
        /* 被上面 space==2 守卫挡住，正常不可达；兜底仍提交已 prep 的 write 清空 ring 半状态，
         * 槽保持 in-flight（write CQE 在途，不改 free）避免 UAF。 */
        io_uring_submit(&g_persist_uring);
        return -1;
    }
    io_uring_prep_fsync(sqe_f, g_persist_aof_registered ? 0 : g_aof_fd, IORING_FSYNC_DATASYNC);
    if (g_persist_aof_registered) sqe_f->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe_f, s);
    io_uring_submit(&g_persist_uring);
    return 0;
}

/* 强制 flush：无视 group-commit 窗口立即 handoff（drain/槽满/同步路径用）。 */
static void persist_flush_pending_force(void) {
    if (g_cfg.aof_fsync_sync) { persist_sync_append_write(); return; }  /* 同步批量路径不动 */

    /* T2：主线程只 handoff + signal + reap（提交由 AOF 线程做，SINGLE_ISSUER）。
     * handoff 内含背压：卡在 MAX_OUTSTANDING 时由 persist_reap_completions（drain）
     * 依赖 AOF 线程自行收割 CQE 并推进 completed 来缓解。 */
    int had = (g_cur_slot && g_cur_slot->aof_len > 0);
    if (had) {
        long long age_us = kvs_now_us() - g_batch_start_us;
        if (age_us > g_max_batch_age_us) g_max_batch_age_us = age_us;
    }
    handoff_current_slot();
    if (had) { uint64_t one = 1; (void)!write(g_aof_wake_efd, &one, sizeof(one)); }
    persist_reap_completions();
}

/* 周期 flush（reactor/ntyco/proactor 每循环调）：带 group-commit 窗口。
 * 窗口未到且槽未满 → 跳过 handoff（多攒几周期的命令共享一次 fsync），仍在收 completed 槽。
 * window=0 → 每周期 flush，行为与旧版一致。 */
void persist_flush_pending(void) {
    if (g_cfg.aof_fsync_sync) { persist_sync_append_write(); return; }
    if (g_cfg.aof_group_commit_window_us > 0 && g_cur_slot && g_cur_slot->aof_len > 0) {
        long long age_us = kvs_now_us() - g_batch_start_us;
        if (age_us < g_cfg.aof_group_commit_window_us) {
            persist_reap_completions();
            return;
        }
    }
    persist_flush_pending_force();
}

/* 观测：handoff 时最大批龄（µs）。先回复语义下 ≈ 崩溃窗口的攒批分量。 */
long long persist_aof_max_batch_age_us(void) {
    return g_max_batch_age_us;
}

/* 距下次 AOF flush 的剩余时间（ms）。无待 flush 槽、同步模式或 window=0 时返回 -1（不约束）。
 * 供 reactor 缩短 epoll_wait 超时：否则稀疏流量下孤立命令要等 reactor 下次唤醒
 * （epoll 100ms 超时）才 flush，崩溃窗口膨胀到 ~100ms。主线程独占 g_cur_slot，无需加锁。 */
int persist_aof_pending_flush_ms(void) {
    if (g_cfg.aof_fsync_sync || g_cfg.aof_group_commit_window_us <= 0) return -1;
    if (!g_cur_slot || g_cur_slot->aof_len == 0) return -1;
    long long remain_us = (g_batch_start_us + (long long)g_cfg.aof_group_commit_window_us) - kvs_now_us();
    if (remain_us <= 0) return 0;                 /* 已到窗口 → 立即 flush */
    return (int)((remain_us + 999) / 1000);       /* ceil 到 ms */
}

int persist_append_raw(const unsigned char *buf, size_t len) {
    return persist_append_prepare(NULL, buf, len, NULL, 0);
}

int persist_save_dump(void) {
    /* C1 修复：先 drain 再取头偏离量。先回复语义下 dump 反映到 g_aof_write_submitted，
     * 而 g_aof_write_offset 只推进到 fsynced frontier，两者可差 MAX_OUTSTANDING 批。
     * drain（fence 到 g_outstanding==0）后 submitted==offset==durable frontier，
     * 头偏移才与 dump 快照吻合，恢复 replay 不会重叠重放 [fsynced, submitted] 的非幂等命令。 */
    persist_drain_pending();
    unsigned long long aof_off = (unsigned long long)g_aof_write_offset;
    int rc = persist_save_dump_to(g_cfg.dump_path, aof_off);
    if (rc == 0) persist_mark_snapshot_success(g_dirty_counter);
    return rc;
}

int persist_recover_in_progress(void) {
    return g_persist_recovering;
}

int persist_recover(void) {
    long long begin_ms = kvs_now_ms();
    long long dump_begin_ms;
    long long aof_begin_ms;

    g_persist_recovering = 1;
    g_recover_last_total_ms = 0;
    g_recover_last_dump_ms = 0;
    g_recover_last_aof_ms = 0;
    g_recover_mmap_attempts = 0;
    g_recover_mmap_success = 0;
    g_recover_mmap_fallbacks = 0;
    g_recover_last_mmap_bytes = 0;
    g_recover_last_fread_bytes = 0;
    g_recover_last_tail_bytes = 0;

    dump_begin_ms = kvs_now_ms();
    unsigned long long aof_offset = replay_dump_file(g_cfg.dump_path);
    g_recover_last_dump_ms = kvs_now_ms() - dump_begin_ms;

    aof_begin_ms = kvs_now_ms();
    if (!g_aof_disabled) {
        replay_file(g_cfg.aof_path, aof_offset);
    }
    g_recover_last_aof_ms = kvs_now_ms() - aof_begin_ms;

    g_recover_last_total_ms = kvs_now_ms() - begin_ms;
    g_dirty_counter = 0;
    g_last_snapshot_ms = kvs_now_ms();
    g_bgsave_last_end_ms = g_last_snapshot_ms;

    kvs_active_expire_cycle(1000000);

    g_persist_recovering = 0;

    return 0;
}

int persist_bgsave_start(void) {
    if (g_bgsave_pid > 0) return 1;

    /* C1 修复：fork 前先 drain，使 g_aof_write_offset（子进程写进 dump 头的偏移）
     * == submitted == durable frontier，dump 快照与头偏移吻合（理由同 persist_save_dump）。 */
    persist_drain_pending();

    unsigned long long snap_dirty = g_dirty_counter;
    unsigned long long aof_off = (unsigned long long)g_aof_write_offset;
    long long start_ms = kvs_now_ms();
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%ld", g_cfg.dump_path, (long)getpid());

    pid_t pid = fork();
    if (pid < 0) {
        g_bgsave_status = 3;
        return -1;
    }
    if (pid == 0) {
        int rc = persist_save_dump_to(tmp_path, aof_off);
        if (rc == 0 && rename(tmp_path, g_cfg.dump_path) != 0) rc = -1;
        if (rc != 0) unlink(tmp_path);
        _exit(rc == 0 ? 0 : 1);
    }

    g_bgsave_pid = pid;
    g_bgsave_status = 1;
    g_bgsave_last_start_ms = start_ms;
    g_bgsave_base_dirty = snap_dirty;
    return 0;
}

int persist_bgsave_poll(void) {
    if (g_bgsave_pid <= 0) return 0;
    int status = 0;
    pid_t rc = waitpid(g_bgsave_pid, &status, WNOHANG);
    if (rc == 0) return 0;
    if (rc < 0) {
        g_bgsave_status = 3;
        g_bgsave_pid = -1;
        g_bgsave_last_end_ms = kvs_now_ms();
        return -1;
    }

    g_bgsave_last_end_ms = kvs_now_ms();
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        g_bgsave_status = 2;
        persist_mark_snapshot_success(g_bgsave_base_dirty);
    } else {
        g_bgsave_status = 3;
    }
    g_bgsave_pid = -1;
    return 1;
}

int persist_bgsave_in_progress(void) {
    return g_bgsave_pid > 0 ? 1 : 0;
}

const char *persist_bgsave_state_name(void) {
    switch (g_bgsave_status) {
        case 1: return "running";
        case 2: return "ok";
        case 3: return "err";
        default: return "idle";
    }
}

int persist_bgrewriteaof_start(void) {
    if (g_bgrewrite_pid > 0) return 1;

    if (persist_force_aof_flush() != 0 && g_aof_fd >= 0) return -1;

    snprintf(g_rewrite_tmp_path, sizeof(g_rewrite_tmp_path), "%s.rewrite.tmp.%ld", g_cfg.aof_path, (long)getpid());

    pthread_mutex_lock(&g_rewrite_buf_lock);
    free_rewrite_buffer_locked();
    pthread_mutex_unlock(&g_rewrite_buf_lock);

    pid_t pid = fork();
    if (pid < 0) {
        g_bgrewrite_status = 3;
        g_rewrite_tmp_path[0] = '\0';
        return -1;
    }
    if (pid == 0) {
        int rc = persist_write_aof_snapshot_to(g_rewrite_tmp_path);
        _exit(rc == 0 ? 0 : 1);
    }

    g_bgrewrite_pid = pid;
    g_bgrewrite_status = 1;
    return 0;
}

int persist_bgrewriteaof_poll(void) {
    if (g_bgrewrite_pid <= 0) return 0;

    int status = 0;
    pid_t rc = waitpid(g_bgrewrite_pid, &status, WNOHANG);
    if (rc == 0) return 0;
    if (rc < 0) {
        g_bgrewrite_status = 3;
        g_bgrewrite_pid = -1;
        unlink(g_rewrite_tmp_path);
        g_rewrite_tmp_path[0] = '\0';
        return -1;
    }

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        if (finalize_rewrite_parent() == 0) g_bgrewrite_status = 2;
        else {
            g_bgrewrite_status = 3;
            unlink(g_rewrite_tmp_path);
        }
    } else {
        g_bgrewrite_status = 3;
        unlink(g_rewrite_tmp_path);
    }

    g_bgrewrite_pid = -1;
    g_rewrite_tmp_path[0] = '\0';
    return 1;
}

int persist_bgrewriteaof_in_progress(void) {
    return g_bgrewrite_pid > 0 ? 1 : 0;
}

const char *persist_bgrewriteaof_state_name(void) {
    switch (g_bgrewrite_status) {
        case 1: return "running";
        case 2: return "ok";
        case 3: return "err";
        default: return "idle";
    }
}

void persist_note_write(void) {
    g_dirty_counter++;
}

unsigned long long persist_dirty_count(void) {
    return g_dirty_counter;
}

long long persist_last_snapshot_ms(void) {
    return g_last_snapshot_ms;
}

int persist_register_autosnap_rule(long long seconds, long long changes) {
    if (seconds <= 0 || changes <= 0) return -1;
    for (int i = 0; i < g_cfg.autosnap_rule_count; ++i) {
        if (g_cfg.autosnap_rules[i].seconds == seconds) {
            g_cfg.autosnap_rules[i].changes = changes;
            return 0;
        }
    }
    if (g_cfg.autosnap_rule_count >= KVS_AUTOSNAP_RULES_MAX) return -1;
    g_cfg.autosnap_rules[g_cfg.autosnap_rule_count].seconds = seconds;
    g_cfg.autosnap_rules[g_cfg.autosnap_rule_count].changes = changes;
    g_cfg.autosnap_rule_count++;
    return 0;
}

void persist_clear_autosnap_rules(void) {
    g_cfg.autosnap_rule_count = 0;
    memset(g_cfg.autosnap_rules, 0, sizeof(g_cfg.autosnap_rules));
}

int persist_build_autosnap_text(char *buf, size_t cap) {
    int n = snprintf(buf, cap,
        "autosnap_rules=%d\n"
        "dirty=%llu\n"
        "last_snapshot_ms=%lld\n"
        "bgsave=%s\n"
        "bgsave_pid=%ld\n"
        "aof_fsync=%s\n"
        "aof_rewrite=%s\n"
        "aof_rewrite_pid=%ld\n"
        "recover_total_ms=%lld\n"
        "recover_dump_ms=%lld\n"
        "recover_aof_ms=%lld\n"
        "recover_mmap_attempts=%llu\n"
        "recover_mmap_success=%llu\n"
        "recover_mmap_fallbacks=%llu\n"
        "recover_mmap_bytes=%llu\n"
        "recover_fread_bytes=%llu\n"
        "recover_tail_bytes=%llu\n",
        g_cfg.autosnap_rule_count,
        (unsigned long long)g_dirty_counter,
        g_last_snapshot_ms,
        persist_bgsave_state_name(),
        (long)g_bgsave_pid,
        persist_aof_policy_name(),
        persist_bgrewriteaof_state_name(),
        (long)g_bgrewrite_pid,
        g_recover_last_total_ms,
        g_recover_last_dump_ms,
        g_recover_last_aof_ms,
        g_recover_mmap_attempts,
        g_recover_mmap_success,
        g_recover_mmap_fallbacks,
        g_recover_last_mmap_bytes,
        g_recover_last_fread_bytes,
        g_recover_last_tail_bytes);
    if (n < 0 || (size_t)n >= cap) return -1;
    size_t pos = (size_t)n;
    for (int i = 0; i < g_cfg.autosnap_rule_count; ++i) {
        n = snprintf(buf + pos, cap - pos, "rule_%d=%lld:%lld\n", i,
            g_cfg.autosnap_rules[i].seconds, g_cfg.autosnap_rules[i].changes);
        if (n < 0 || (size_t)n >= cap - pos) return -1;
        pos += (size_t)n;
    }
    return (int)pos;
}

int persist_build_recover_text(char *buf, size_t cap) {
    int n = snprintf(buf, cap,
        "recover_total_ms=%lld\n"
        "recover_dump_ms=%lld\n"
        "recover_aof_ms=%lld\n"
        "recover_mmap_attempts=%llu\n"
        "recover_mmap_success=%llu\n"
        "recover_mmap_fallbacks=%llu\n"
        "recover_mmap_bytes=%llu\n"
        "recover_fread_bytes=%llu\n"
        "recover_tail_bytes=%llu\n",
        g_recover_last_total_ms,
        g_recover_last_dump_ms,
        g_recover_last_aof_ms,
        g_recover_mmap_attempts,
        g_recover_mmap_success,
        g_recover_mmap_fallbacks,
        g_recover_last_mmap_bytes,
        g_recover_last_fread_bytes,
        g_recover_last_tail_bytes);
    if (n < 0 || (size_t)n >= cap) return -1;
    return n;
}

int persist_autosnap_cron(void) {
    persist_bgsave_poll();
    persist_bgrewriteaof_poll();

    if (g_cfg.role != ROLE_MASTER) return 0;
    if (g_bgsave_pid > 0) return 0;
    if (g_cfg.autosnap_rule_count <= 0) return 0;

    long long now = kvs_now_ms();
    long long last_ms = g_last_snapshot_ms > 0 ? g_last_snapshot_ms : g_bgsave_last_end_ms;
    if (last_ms <= 0) last_ms = now;

    for (int i = 0; i < g_cfg.autosnap_rule_count; ++i) {
        long long sec = g_cfg.autosnap_rules[i].seconds;
        long long changes = g_cfg.autosnap_rules[i].changes;
        if ((long long)g_dirty_counter >= changes && now - last_ms >= sec * 1000) {
            return persist_bgsave_start();
        }
    }
    return 0;
}
