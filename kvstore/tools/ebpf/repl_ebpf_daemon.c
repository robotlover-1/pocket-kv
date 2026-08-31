/*
 * repl_ebpf_daemon.c — eBPF 独立守护进程
 *
 * 该进程负责加载 eBPF BPF 对象、挂载 BPF maps 到 bpffs、
 * 附加 BPF 程序到 sockmap，并保持运行以维持 BPF 程序的生命周期。
 *
 * kvstore 主进程不再直接初始化 eBPF，而是通过 pinned maps 与
 * 本守护进程通信，实现 eBPF 与主进程的解耦。
 *
 * 编译:
 *   gcc -Wall -Wextra -O2 -I../../include \
 *       -o repl_ebpf_daemon repl_ebpf_daemon.c \
 *       -lbpf -lelf -lz
 *
 * 用法:
 *   # 先确保 bpffs 已挂载:
 *   mount -t bpf bpf /sys/fs/bpf/
 *   mkdir -p /sys/fs/bpf/kvstore
 *
 *   # 启动守护进程 (后台运行):
 *   ./repl_ebpf_daemon --obj build/replication/bpf/repl_sockmap.bpf.o \
 *       --pin /sys/fs/bpf/kvstore \
 *       --redirect --redirect-key 0
 *
 *   # 启动 kvstore (eBPF 会自动通过 pinned maps 连接):
 *   ./kvstore --config kvstore-master.conf
 *
 *   # 停止守护进程:
 *   kill <pid>
 *
 * 配置文件示例 (kvstore-ebpf.conf):
 *   ebpf_enabled=1
 *   ebpf_obj_path=build/replication/bpf/repl_sockmap.bpf.o
 *   ebpf_pin_path=/sys/fs/bpf/kvstore
 *   ebpf_redirect=1
 *   ebpf_redirect_key=0
 *   ebpf_forward=0
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <getopt.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>

static volatile int g_running = 1;

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

static int parse_config_value(char *line, char **key, char **value) {
    char *eq;
    if (line[0] == '\0' || line[0] == '#') return -1;
    eq = strchr(line, '=');
    if (!eq) return -1;
    *eq = '\0';
    *key = line;
    *value = eq + 1;
    /* trim */
    char *p = *key;
    while (*p == ' ' || *p == '\t') p++;
    *key = p;
    p = *value;
    while (*p == ' ' || *p == '\t') p++;
    *value = p;
    /* trim trailing */
    for (char *e = *key + strlen(*key) - 1; e >= *key && (*e == ' ' || *e == '\t' || *e == '\r' || *e == '\n'); e--) *e = '\0';
    for (char *e = *value + strlen(*value) - 1; e >= *value && (*e == ' ' || *e == '\t' || *e == '\r' || *e == '\n'); e--) *e = '\0';
    return 0;
}

static int pin_map_fd(int fd, const char *pin_path, const char *name) {
    char path[512];
    if (fd < 0) return -1;
    snprintf(path, sizeof(path), "%s/%s", pin_path, name);
    /* 先删除旧的 pin 文件 */
    unlink(path);
    if (bpf_obj_pin(fd, path) != 0) {
        fprintf(stderr, "pin %s failed: %s\n", path, strerror(errno));
        return -1;
    }
    /* 设置 0644 权限，使非 root 进程也能通过 bpf_obj_get() 访问 */
    chmod(path, 0644);
    printf("  pinned: %s\n", path);
    return 0;
}

static void raise_memlock(void) {
    struct rlimit rl = {RLIM_INFINITY, RLIM_INFINITY};
    setrlimit(RLIMIT_MEMLOCK, &rl);
}

int main(int argc, char **argv) {
    const char *obj_path = NULL;
    const char *pin_path = NULL;
    int redirect = 0;
    int redirect_key = 0;
    int forward = 0;
    int daemonize = 0;
    const char *config_path = NULL;

    static const struct option long_opts[] = {
        {"obj",       required_argument, 0, 'o'},
        {"pin",       required_argument, 0, 'p'},
        {"redirect",  no_argument,       0, 'r'},
        {"redirect-key", required_argument, 0, 'k'},
        {"forward",   no_argument,       0, 'f'},
        {"config",    required_argument, 0, 'c'},
        {"daemon",    no_argument,       0, 'd'},
        {"help",      no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "o:p:rk:fc:dh", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'o': obj_path = optarg; break;
            case 'p': pin_path = optarg; break;
            case 'r': redirect = 1; break;
            case 'k': redirect_key = atoi(optarg); break;
            case 'f': forward = 1; break;
            case 'c': config_path = optarg; break;
            case 'd': daemonize = 1; break;
            case 'h':
            default:
                printf("用法: %s [选项]\n", argv[0]);
                printf("选项:\n");
                printf("  -o, --obj PATH       eBPF BPF 对象文件路径\n");
                printf("  -p, --pin PATH       BPF maps pin 目录 (如 /sys/fs/bpf/kvstore)\n");
                printf("  -r, --redirect       启用 BPF_F_INGRESS redirect\n");
                printf("  -k, --redirect-key N  redirect key (默认 0)\n");
                printf("  -f, --forward        启用跨机器转发模式\n");
                printf("  -c, --config FILE    从配置文件读取参数\n");
                printf("  -d, --daemon         后台运行\n");
                printf("  -h, --help           显示此帮助\n");
                printf("\n示例:\n");
                printf("  # 前台运行\n");
                printf("  %s -o build/replication/bpf/repl_sockmap.bpf.o \\\n", argv[0]);
                printf("        -p /sys/fs/bpf/kvstore -r -k 0\n");
                printf("\n  # 后台运行\n");
                printf("  %s -o build/replication/bpf/repl_sockmap.bpf.o \\\n", argv[0]);
                printf("        -p /sys/fs/bpf/kvstore -r -k 0 -d\n");
                printf("\n  # 使用配置文件\n");
                printf("  %s -c kvstore-ebpf.conf\n", argv[0]);
                return (opt == 'h') ? 0 : 1;
        }
    }

    /* 从配置文件加载参数 */
    if (config_path) {
        FILE *fp = fopen(config_path, "r");
        if (!fp) {
            fprintf(stderr, "无法打开配置文件: %s\n", config_path);
            return 1;
        }
        char line[1024];
        while (fgets(line, sizeof(line), fp)) {
            char *key, *value;
            if (parse_config_value(line, &key, &value) != 0) continue;
            if (strcmp(key, "ebpf_obj_path") == 0) obj_path = strdup(value);
            else if (strcmp(key, "ebpf_pin_path") == 0) pin_path = strdup(value);
            else if (strcmp(key, "ebpf_redirect") == 0) redirect = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
            else if (strcmp(key, "ebpf_redirect_key") == 0) redirect_key = atoi(value);
            else if (strcmp(key, "ebpf_forward") == 0) forward = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
        }
        fclose(fp);
    }

    if (!obj_path || !pin_path) {
        fprintf(stderr, "请指定 --obj 和 --pin (或使用 --config)\n");
        return 1;
    }

    /* 后台运行 */
    if (daemonize) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); return 1; }
        if (pid > 0) {
            printf("eBPF daemon started (pid=%d)\n", pid);
            return 0;
        }
        setsid();
        /* 关闭 stdio */
        fclose(stdin);
        fclose(stdout);
        fclose(stderr);
        /* 重定向到 /dev/null */
        open("/dev/null", O_RDONLY);  /* stdin */
        open("/dev/null", O_WRONLY);  /* stdout */
        open("/dev/null", O_WRONLY);  /* stderr */
    }

    /* 注册信号处理 */
    signal(SIGTERM, handle_signal);
    signal(SIGINT, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    raise_memlock();

    printf("eBPF daemon starting...\n");
    printf("  obj: %s\n", obj_path);
    printf("  pin: %s\n", pin_path);
    printf("  redirect: %d, key: %d, forward: %d\n", redirect, redirect_key, forward);

    /* 确保 pin 目录存在 */
    if (mkdir(pin_path, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "创建 pin 目录失败: %s\n", strerror(errno));
        return 1;
    }

    /* 加载 BPF 对象 */
    struct bpf_object *obj = bpf_object__open_file(obj_path, NULL);
    if (libbpf_get_error(obj)) {
        fprintf(stderr, "打开 BPF 对象失败: %s\n", obj_path);
        return 1;
    }

    /* 可选：设置 libbpf 日志级别（只打印错误） */
    libbpf_set_print(NULL);

    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "加载 BPF 对象失败\n");
        bpf_object__close(obj);
        return 1;
    }
    printf("  BPF object loaded\n");

    /* 查找 maps */
    int sock_map_fd = bpf_object__find_map_fd_by_name(obj, "sock_map");
    int stats_map_fd = bpf_object__find_map_fd_by_name(obj, "stats_map");
    int control_map_fd = bpf_object__find_map_fd_by_name(obj, "control_map");
    if (sock_map_fd < 0 || stats_map_fd < 0 || control_map_fd < 0) {
        fprintf(stderr, "查找 BPF maps 失败\n");
        bpf_object__close(obj);
        return 1;
    }
    printf("  Maps found: sock_map=%d stats_map=%d control_map=%d\n",
           sock_map_fd, stats_map_fd, control_map_fd);

    /* 查找并附加 sk_msg 程序 */
    struct bpf_program *prog = bpf_object__find_program_by_name(obj, "kvstore_repl_sk_msg");
    if (!prog) {
        fprintf(stderr, "查找 sk_msg 程序失败\n");
        bpf_object__close(obj);
        return 1;
    }
    int prog_fd = bpf_program__fd(prog);
    if (prog_fd < 0) {
        fprintf(stderr, "获取程序 fd 失败\n");
        bpf_object__close(obj);
        return 1;
    }
    printf("  sk_msg prog fd: %d\n", prog_fd);

    /* 附加到 sockmap */
    if (bpf_prog_attach(prog_fd, sock_map_fd, BPF_SK_MSG_VERDICT, 0) != 0) {
        fprintf(stderr, "附加 sk_msg 程序失败: %s\n", strerror(errno));
        bpf_object__close(obj);
        return 1;
    }
    printf("  sk_msg program attached\n");

    /* 更新 control map */
    __u32 key, value;

    key = 0; value = redirect ? 1u : 0u;
    bpf_map_update_elem(control_map_fd, &key, &value, BPF_ANY);

    key = 1; value = (__u32)(redirect_key >= 0 ? redirect_key : 0);
    bpf_map_update_elem(control_map_fd, &key, &value, BPF_ANY);

    key = 4; /* redirect ingress flag */
    value = forward ? 0u : (redirect ? 1u : 0u);
    bpf_map_update_elem(control_map_fd, &key, &value, BPF_ANY);

    printf("  control map updated\n");

    /* Pin maps 到 bpffs */
    if (pin_map_fd(sock_map_fd, pin_path, "sock_map") != 0) { bpf_object__close(obj); return 1; }
    if (pin_map_fd(stats_map_fd, pin_path, "stats_map") != 0) { bpf_object__close(obj); return 1; }
    if (pin_map_fd(control_map_fd, pin_path, "control_map") != 0) { bpf_object__close(obj); return 1; }
    printf("  Maps pinned to %s\n", pin_path);

    printf("eBPF daemon ready (pid=%d)\n", getpid());

    /* 保持运行 */
    while (g_running) {
        pause();
    }

    printf("eBPF daemon shutting down...\n");
    bpf_object__close(obj);
    printf("Done.\n");
    return 0;
}
