CC=gcc
CLANG?=clang
ENABLE_RDMA?=1
ENABLE_EBPF?=1
ENABLE_KPROBE_RDMA?=1
BPF_CFLAGS?=-O2 -g -target bpf -D__TARGET_ARCH_x86 -I/usr/include/x86_64-linux-gnu
BPF_KPROBE_CFLAGS?=-O2 -g -target bpf -D__TARGET_ARCH_x86 -I/usr/include/x86_64-linux-gnu -I./include
# kprobe-rdma 依赖 RDMA
ifeq ($(ENABLE_KPROBE_RDMA),1)
ENABLE_RDMA ?= 1
endif
CFLAGS=-Wall -Wextra -O2 -march=native -funroll-loops -I./include -I./NtyCo/core -I./liburing/src/include $(if $(filter 1,$(ENABLE_RDMA)),-DKVS_ENABLE_RDMA=1,-DKVS_ENABLE_RDMA=0) $(if $(filter 1,$(ENABLE_EBPF)),-DKVS_ENABLE_EBPF=1,-DKVS_ENABLE_EBPF=0) $(if $(filter 1,$(ENABLE_KPROBE_RDMA)),-DKVS_ENABLE_KPROBE_RDMA=1,-DKVS_ENABLE_KPROBE_RDMA=0)
LDFLAGS=-lpthread -ldl -L./NtyCo -lntyco -L./liburing/src -luring -ldl -lm $(if $(filter 1,$(ENABLE_RDMA)),-lrdmacm -libverbs,) $(if $(filter 1,$(ENABLE_EBPF)),-lbpf -lelf -lz,) $(if $(filter 1,$(ENABLE_KPROBE_RDMA)),-lbpf -lelf -lz,)

SRC_DIR=src
INC_DIR=include/kvstore
TEST_HOST?=127.0.0.1
TEST_PORT?=5000
REPL_RDMA_SMOKE_HOST?=$(TEST_HOST)
REPL_MASTER_PORT?=5379
REPL_SLAVE_PORT?=5380
REPL_TRANSPORT?=tcp
REPL_RECONNECT_WAIT?=3
REPL_RESTART_WAIT?=4
REPL_BENCH_MASTER_PORT?=5188
REPL_BENCH_SLAVE_PORT?=5189
REPL_BENCH_PRELOAD?=5000
REPL_BENCH_TAIL?=1000
REPL_PROFILE_MASTER_PORT?=5200
REPL_PROFILE_SLAVE_PORT?=5201
REPL_EBPF_SYNC_MASTER_PORT?=5240
REPL_EBPF_SYNC_SLAVE_PORT?=5241
REPL_EBPF_SYNC_COUNT?=64
REPL_RDMA_UNSUPPORTED_MASTER_PORT?=5210
REPL_RDMA_UNSUPPORTED_SLAVE_PORT?=5211
REPL_RDMA_SMOKE_MASTER_PORT?=5220
REPL_RDMA_SMOKE_SLAVE_PORT?=5222
REPL_RDMA_STRESS_MASTER_PORT?=5230
REPL_RDMA_STRESS_SLAVE_PORT?=5232
REPL_RDMA_STRESS_PRELOAD?=128
REPL_RDMA_STRESS_TAIL_WRITES?=64
REPL_RDMA_STRESS_RESTART_ROUNDS?=3
REPL_RDMA_SOAK_SECONDS?=60
REPL_RDMA_LONG_SOAK_SECONDS?=1800
REPL_RDMA_SOAK_ALLOW_FAILURE?=0
REPL_RDMA_SOAK_RECONNECT_INTERVAL?=10
REPL_RDMA_SOAK_WRITE_INTERVAL_MS?=200
REPL_RDMA_TUNABLE_RECV_SLOTS?=0
REPL_RDMA_TUNABLE_CHUNK_SIZE?=0
REPL_RDMA_TUNABLE_QP_WR_DEPTH?=0
REPL_RDMA_FORCE_FALLBACK?=0
RDMA_PINGPONG_HOST?=$(REPL_RDMA_SMOKE_HOST)
RDMA_PINGPONG_PORT?=18515
RDMA_PINGPONG_DEV?=rxe0
RDMA_PINGPONG_IB_PORT?=1
RDMA_PINGPONG_GID_IDX?=1
MASS_TTL_KEYS?=10000
MASS_TTL_SECONDS?=15
MASS_TTL_BATCH?=1000
MASS_TTL_SAMPLE?=20
URING_PERSIST_COUNT?=10000
URING_PERSIST_PORT?=5140
URING_PERSIST_APPEND_FSYNC?=always
MMAP_RECOVER_COUNT?=10000
MMAP_RECOVER_PORT?=5142
MMAP_RECOVER_ENGINE?=hash
MMAP_RECOVER_APPEND_FSYNC?=everysec
FULL_DUMP_DEMO_COUNT?=100000
FULL_DUMP_DEMO_PORT?=5150
INCR_AOF_DEMO_COUNT?=100000
INCR_AOF_DEMO_PORT?=5152
REPL_SYNC_DEMO_MASTER_PORT?=5160
REPL_SYNC_DEMO_SLAVE_PORT?=5162
REPL_SYNC_DEMO_PRE?=50000
REPL_SYNC_DEMO_POST?=50000
REPL_SYNC_DEMO_FULLSYNC?=rdma
REPL_SYNC_DEMO_REALTIME?=ebpf
RDMA_DEV?=rxe0
REPL_SYNC_DEMO_RDMA_DEV?=$(RDMA_DEV)
REPL_BENCH_PRELOAD?=50000
REPL_BENCH_TAIL?=5000

SRCS=$(SRC_DIR)/main/kvstore.c \
     $(SRC_DIR)/core/reactor.c \
     $(SRC_DIR)/core/proactor.c \
     $(SRC_DIR)/core/ntyco.c \
     $(SRC_DIR)/storage/kvs_array.c \
     $(SRC_DIR)/storage/kvs_hash.c \
     $(SRC_DIR)/storage/kvs_rbtree.c \
     $(SRC_DIR)/storage/kvs_skiptable.c \
     $(SRC_DIR)/storage/kvs_doc.c \
     $(SRC_DIR)/storage/kvs_vector.c \
     $(SRC_DIR)/memory/kvs_mem.c \
     $(SRC_DIR)/expire/kvs_expire.c \
     $(SRC_DIR)/persistence/kvs_persist.c \
     $(SRC_DIR)/replication/kvs_repl.c \
     $(SRC_DIR)/replication/kvs_fullsync.c \
     $(SRC_DIR)/replication/kvs_repl_ebpf.c \
     $(SRC_DIR)/replication/kvs_repl_kprobe.c \
     $(SRC_DIR)/replication/kvs_sentinel.c \
     $(SRC_DIR)/utils/hash.c

OBJS=$(patsubst $(SRC_DIR)/%.c, build/%.o, $(SRCS))
BPF_SRCS=$(SRC_DIR)/replication/bpf/repl_sockmap.bpf.c \
          $(SRC_DIR)/replication/bpf/repl_kprobe.bpf.c
BPF_OBJS=$(patsubst $(SRC_DIR)/%.bpf.c, build/%.bpf.o, $(BPF_SRCS))
KVS_BPF_OBJS=$(if $(filter 1,$(ENABLE_EBPF)),$(BPF_OBJS),)
KVS_KPROBE_BPF_OBJS=$(if $(filter 1,$(ENABLE_KPROBE_RDMA)),build/replication/bpf/repl_kprobe.bpf.o build/replication/bpf/repl_client_capture.bpf.o,)

all: build_dir kvstore ebpf-proxy

NtyCo/libntyco.a:
	$(MAKE) -C NtyCo

kvstore: NtyCo/libntyco.a $(KVS_BPF_OBJS) $(KVS_KPROBE_BPF_OBJS) $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

build/replication/kvs_repl_ebpf.o: $(SRC_DIR)/replication/kvs_repl_ebpf.c $(INC_DIR)/kvstore.h
	@mkdir -p $(dir $@)
	$(CC) $(filter-out -O2,$(CFLAGS)) -O0 -c $< -o $@

build/%.o: $(SRC_DIR)/%.c $(INC_DIR)/kvstore.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

build/%.bpf.o: $(SRC_DIR)/%.bpf.c
	@mkdir -p $(dir $@)
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@

build_dir:
	@mkdir -p build/main build/core build/storage build/memory build/expire build/persistence build/replication build/replication/bpf build/utils

# ---- Comprehensive C test client ----
TEST_KVSTORE_SRC=tests/test_kvstore.c
TEST_KVSTORE_BIN=test_kvstore
TEST_HOST ?= 127.0.0.1
TEST_PORT ?= 5000

$(TEST_KVSTORE_BIN): $(TEST_KVSTORE_SRC) $(INC_DIR)/kvstore.h
	$(CC) $(CFLAGS) -o $@ $<

check-kvstore: $(TEST_KVSTORE_BIN)
	python3 ./tools/tests/run_with_kvstore.py --bin ./kvstore --host $(TEST_HOST) --port $(TEST_PORT) -- ./$(TEST_KVSTORE_BIN) {HOST} {PORT}

# ---- 全量同步传输无关协议单元测试 ----
TEST_FULLSYNC_SRC=tests/test_fullsync_protocol.c
TEST_FULLSYNC_BIN=tests/test_fullsync_protocol

$(TEST_FULLSYNC_BIN): $(TEST_FULLSYNC_SRC) src/replication/kvs_fullsync.c include/kvstore/replication/fullsync.h
	$(CC) $(CFLAGS) -I./include -o $@ $(TEST_FULLSYNC_SRC) src/replication/kvs_fullsync.c

test_fullsync_protocol: $(TEST_FULLSYNC_BIN)
	./$(TEST_FULLSYNC_BIN)

# ---- 5w+5w 主从同步测试 ----
TEST_REPL_5W5W_SRC=tests/test_repl_5w5w.c
TEST_REPL_5W5W_BIN=tests/test_repl_5w5w

$(TEST_REPL_5W5W_BIN): $(TEST_REPL_5W5W_SRC) $(INC_DIR)/kvstore.h
	$(CC) $(CFLAGS) -o $@ $<

# ---- kprobe 主从转发 QPS 测试 ----
TEST_KPROBE_REPL_QPS_SRC=tests/perf/test_kprobe_repl_qps.c
TEST_KPROBE_REPL_QPS_BIN=tests/perf/test_kprobe_repl_qps

$(TEST_KPROBE_REPL_QPS_BIN): $(TEST_KPROBE_REPL_QPS_SRC)
	$(CC) $(CFLAGS) -o $@ $< -lbpf -lelf -lz -lpthread

# ---- ebpf-proxy 独立进程架构 QPS 测试 ----
TEST_EBPF_PROXY_QPS_SRC=tests/perf/test_ebpf_proxy_qps.c
TEST_EBPF_PROXY_QPS_BIN=tests/perf/test_ebpf_proxy_qps

$(TEST_EBPF_PROXY_QPS_BIN): $(TEST_EBPF_PROXY_QPS_SRC)
	$(CC) $(CFLAGS) -o $@ $< -lbpf -lelf -lz -lpthread -lm

test_ebpf_proxy_qps: $(TEST_EBPF_PROXY_QPS_BIN)

# ---- eBPF HSET QPS 测试 (使用 redis-benchmark) ----
TEST_EBPF_HSET_QPS_SRC=tests/perf/test_ebpf_hset_qps.c
TEST_EBPF_HSET_QPS_BIN=tests/perf/test_ebpf_hset_qps

$(TEST_EBPF_HSET_QPS_BIN): $(TEST_EBPF_HSET_QPS_SRC)
	$(CC) $(CFLAGS) -o $@ $< -lbpf -lelf -lz -lpthread -lm

test_ebpf_hset_qps: $(TEST_EBPF_HSET_QPS_BIN)

# ---- TCP sink (slave 端计数程序) ----
TCPSINK_SRC=tests/perf/tcpsink.c
TCPSINK_BIN=tests/perf/tcpsink

$(TCPSINK_BIN): $(TCPSINK_SRC)
	$(CC) $(CFLAGS) -o $@ $< -lpthread

tcpsink: $(TCPSINK_BIN)

# ---- eBPF 独立守护进程 ----
EBPF_DAEMON_SRC=tools/ebpf/repl_ebpf_daemon.c
EBPF_DAEMON_BIN=tools/ebpf/repl_ebpf_daemon

$(EBPF_DAEMON_BIN): $(EBPF_DAEMON_SRC)
	$(CC) $(CFLAGS) -o $@ $< -lbpf -lelf -lz

# ---- ebpf-proxy 独立代理进程 ----
EBPF_PROXY_SRC = src/ebpf_proxy/main.c src/ebpf_proxy/proxy_cache.c src/ebpf_proxy/proxy_slave.c
EBPF_PROXY_BIN = build/ebpf_proxy
EBPF_PROXY_CFLAGS = -Wall -Wextra -O2 -I./include -I./third_party/libbpf/include
EBPF_LIBBPF_A = third_party/libbpf/lib/libbpf.a

$(EBPF_PROXY_BIN): $(EBPF_PROXY_SRC) $(EBPF_LIBBPF_A)
	@mkdir -p build
	$(CC) $(EBPF_PROXY_CFLAGS) -o $@ $(EBPF_PROXY_SRC) $(EBPF_LIBBPF_A) -lelf -lz -lpthread

ebpf-proxy: $(EBPF_PROXY_BIN)

# ---- client_capture BPF 对象文件 ----
CLIENT_CAPTURE_BPF_SRC = src/replication/bpf/repl_client_capture.bpf.c
CLIENT_CAPTURE_BPF_OBJ = build/replication/bpf/repl_client_capture.bpf.o

$(CLIENT_CAPTURE_BPF_OBJ): $(CLIENT_CAPTURE_BPF_SRC)
	@mkdir -p build/replication/bpf
	$(CLANG) $(BPF_KPROBE_CFLAGS) -c $< -o $@

client_capture_bpf: $(CLIENT_CAPTURE_BPF_OBJ)

# ---- 持久化演示测试 ----
TEST_PERSIST_DUMP_SRC=tests/test_persist_dump_demo.c
TEST_PERSIST_DUMP_BIN=test_persist_dump_demo
TEST_PERSIST_AOF_SRC=tests/test_persist_aof_demo.c
TEST_PERSIST_AOF_BIN=test_persist_aof_demo

$(TEST_PERSIST_DUMP_BIN): $(TEST_PERSIST_DUMP_SRC)
	$(CC) $(CFLAGS) -o $@ $<

$(TEST_PERSIST_AOF_BIN): $(TEST_PERSIST_AOF_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# ---- 新增测试程序 ----
TEST_URING_PERSIST_SRC=tests/test_uring_persist.c
TEST_URING_PERSIST_BIN=test_uring_persist
TEST_MMAP_RECOVER_SRC=tests/test_mmap_recover.c
TEST_MMAP_RECOVER_BIN=test_mmap_recover
TEST_REPL_BASIC_SRC=tests/test_repl_basic.c
TEST_REPL_BASIC_BIN=test_repl_basic

$(TEST_URING_PERSIST_BIN): $(TEST_URING_PERSIST_SRC)
	$(CC) $(CFLAGS) -o $@ $<

$(TEST_MMAP_RECOVER_BIN): $(TEST_MMAP_RECOVER_SRC)
	$(CC) $(CFLAGS) -o $@ $<

$(TEST_REPL_BASIC_BIN): $(TEST_REPL_BASIC_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# ---- 大量 TTL 测试 ----
TEST_MASS_TTL_SRC=tests/test_mass_ttl.c
TEST_MASS_TTL_BIN=test_mass_ttl

$(TEST_MASS_TTL_BIN): $(TEST_MASS_TTL_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# ---- 批量流水线测试 ----
TEST_BATCH_SRC=tests/test_batch.c
TEST_BATCH_BIN=test_batch

$(TEST_BATCH_BIN): $(TEST_BATCH_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# ---- GAP 全量同步补发测试 ----
TEST_REPL_GAP_SRC=tests/test_repl_gap.c
TEST_REPL_GAP_BIN=test_repl_gap

$(TEST_REPL_GAP_BIN): $(TEST_REPL_GAP_SRC)
	$(CC) $(CFLAGS) -o $@ $<

check-repl-gap: $(TEST_REPL_GAP_BIN)
	./$(TEST_REPL_GAP_BIN) --master-port $(REPL_MASTER_PORT) --slave-port $(REPL_SLAVE_PORT) --pre-count 3000 --post-count 1000

# ---- bpf_probe_read_user 验证 ----
VERIFY_BPF_READ_USER_SRC=tests/verify_bpf_read_user.c
VERIFY_BPF_READ_USER_BIN=tests/verify_bpf_read_user

$(VERIFY_BPF_READ_USER_BIN): $(VERIFY_BPF_READ_USER_SRC)
	$(CC) $(CFLAGS) -o $@ $< -lbpf -lelf -lz

# 回归：客户端在 AOF 批 fsync 在途时断开 → persist_purge_conn 不得 UAF/崩溃
PERSIST_PURGE_PORT?=5174
check-persist-purge:
	bash ./tools/persist/test_persist_purge_conn.sh $(TEST_HOST) $(PERSIST_PURGE_PORT)

# 回归(C1)：SAVE/BGSAVE 必须先 drain 再取 dump 头偏移，保证恢复不重放 [fsynced,submitted]
PERSIST_SAVE_FRONTIER_PORT?=5176
check-persist-save-frontier:
	bash ./tools/persist/test_persist_save_durable_frontier.sh $(TEST_HOST) $(PERSIST_SAVE_FRONTIER_PORT)

check-uring-persist-c: $(TEST_URING_PERSIST_BIN)
	./$(TEST_URING_PERSIST_BIN) --port $(URING_PERSIST_PORT) --count $(URING_PERSIST_COUNT) --appendfsync $(URING_PERSIST_APPEND_FSYNC)

check-mmap-recover-c: $(TEST_MMAP_RECOVER_BIN)
	./$(TEST_MMAP_RECOVER_BIN) --port $(MMAP_RECOVER_PORT) --count $(MMAP_RECOVER_COUNT) --engine $(MMAP_RECOVER_ENGINE) --appendfsync $(MMAP_RECOVER_APPEND_FSYNC)

check-repl-basic: $(TEST_REPL_BASIC_BIN)
	./$(TEST_REPL_BASIC_BIN) --master-port $(REPL_MASTER_PORT) --slave-port $(REPL_SLAVE_PORT) --count $(PRELOAD_COUNT) --repl-transport $(REPL_TRANSPORT)

clean:
	rm -rf build kvstore kvstore.dump kvstore.aof $(TEST_KVSTORE_BIN) $(TEST_REPL_5W5W_BIN) $(EBPF_DAEMON_BIN) $(TEST_PERSIST_DUMP_BIN) $(TEST_PERSIST_AOF_BIN) $(TEST_URING_PERSIST_BIN) $(TEST_MMAP_RECOVER_BIN) $(TEST_REPL_BASIC_BIN) $(TEST_MASS_TTL_BIN) $(TEST_BATCH_BIN) $(TEST_REPL_GAP_BIN)
	rm -f kvstore-master.dump kvstore-master.aof kvstore-slave.dump kvstore-slave.aof

check-resp:
	python3 ./tools/tests/run_with_kvstore.py --bin ./kvstore --host $(TEST_HOST) --port $(TEST_PORT) -- bash ./tests/integration/test_resp_nc_strict.sh {HOST} {PORT}

check-ttl:
	python3 ./tools/tests/run_with_kvstore.py --bin ./kvstore --host $(TEST_HOST) --port $(TEST_PORT) -- bash ./tests/integration/test_resp_ttl_nc.sh {HOST} {PORT}

check-persist:
	python3 ./tools/tests/run_with_kvstore.py --bin ./kvstore --host $(TEST_HOST) --port $(TEST_PORT) -- bash ./tools/persist/test_resp_persist_nc.sh {HOST} {PORT}

check-doc:
	python3 ./tools/tests/run_with_kvstore.py --bin ./kvstore --host $(TEST_HOST) --port $(TEST_PORT) -- bash ./tests/integration/test_doc_nc.sh {HOST} {PORT}

BULK_COUNT?=10000
check-bulk-1w:
	python3 ./tools/tests/run_bulk_1w_tests.py --count $(BULK_COUNT)

check-mass-ttl:
	python3 ./tools/persist/run_mass_ttl_validation.py --host $(TEST_HOST) --port $(TEST_PORT) --keys $(MASS_TTL_KEYS) --ttl $(MASS_TTL_SECONDS) --batch $(MASS_TTL_BATCH) --sample $(MASS_TTL_SAMPLE)

check-uring-persist:
	python3 ./tools/persist/run_uring_persist_bench.py --bin ./kvstore --host $(TEST_HOST) --port $(URING_PERSIST_PORT) --count $(URING_PERSIST_COUNT) --appendfsync $(URING_PERSIST_APPEND_FSYNC)

check-mmap-recover:
	python3 ./tools/persist/run_mmap_recover_bench.py --bin ./kvstore --host $(TEST_HOST) --port $(MMAP_RECOVER_PORT) --count $(MMAP_RECOVER_COUNT) --engine $(MMAP_RECOVER_ENGINE) --appendfsync $(MMAP_RECOVER_APPEND_FSYNC)

check-repl:
	-kill $$(lsof -ti:$(REPL_MASTER_PORT)) 2>/dev/null
	-kill $$(lsof -ti:$(REPL_SLAVE_PORT)) 2>/dev/null
	-sleep 0.5
	MASTER_PORT=$(REPL_MASTER_PORT) SLAVE_PORT=$(REPL_SLAVE_PORT) RECONNECT_WAIT=$(REPL_RECONNECT_WAIT) RESTART_WAIT=$(REPL_RESTART_WAIT) bash ./tools/repl/run_repl_fullsync_test.sh

check-repl-metrics:
	python3 ./tools/repl/run_repl_metrics_bench.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_BENCH_MASTER_PORT) --slave-port $(REPL_BENCH_SLAVE_PORT) --preload-count $(REPL_BENCH_PRELOAD) --tail-count $(REPL_BENCH_TAIL)

check-repl-profile:
	python3 ./tools/repl/run_repl_profile.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_PROFILE_MASTER_PORT) --slave-port $(REPL_PROFILE_SLAVE_PORT) --preload-count $(REPL_BENCH_PRELOAD) --tail-count $(REPL_BENCH_TAIL)

check-repl-ebpf:
	python3 ./tools/repl/run_repl_profile.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_PROFILE_MASTER_PORT) --slave-port $(REPL_PROFILE_SLAVE_PORT) --preload-count $(REPL_BENCH_PRELOAD) --tail-count $(REPL_BENCH_TAIL) --ebpf

check-repl-ebpf-env:
	python3 ./tools/repl/run_repl_ebpf_env_probe.py

check-repl-ebpf-sync:
	python3 ./tools/repl/run_repl_ebpf_sync_test.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_EBPF_SYNC_MASTER_PORT) --slave-port $(REPL_EBPF_SYNC_SLAVE_PORT) --count $(REPL_EBPF_SYNC_COUNT) --cleanup-leftovers --timeout 60

check-repl-ebpf-sync-required:
	python3 ./tools/repl/run_repl_ebpf_sync_test.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_EBPF_SYNC_MASTER_PORT) --slave-port $(REPL_EBPF_SYNC_SLAVE_PORT) --count $(REPL_EBPF_SYNC_COUNT) --require-ebpf --cleanup-leftovers --timeout 60

check-repl-ebpf-redirect:
	python3 ./tools/repl/run_repl_ebpf_sync_test.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_EBPF_SYNC_MASTER_PORT) --slave-port $(REPL_EBPF_SYNC_SLAVE_PORT) --count $(REPL_EBPF_SYNC_COUNT) --require-ebpf --redirect --cleanup-leftovers --timeout 60

check-repl-rdma-unsupported:
	python3 ./tools/repl/run_repl_rdma_unsupported.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_RDMA_UNSUPPORTED_MASTER_PORT) --slave-port $(REPL_RDMA_UNSUPPORTED_SLAVE_PORT)

check-repl-rdma-smoke:
	-kill $$(lsof -ti:$(REPL_RDMA_SMOKE_MASTER_PORT)) 2>/dev/null
	-kill $$(lsof -ti:$(REPL_RDMA_SMOKE_SLAVE_PORT)) 2>/dev/null
	-sleep 0.5
	ENABLE_RDMA=1 python3 ./tools/repl/run_repl_rdma_smoke.py --bin ./kvstore --master-port $(REPL_RDMA_SMOKE_MASTER_PORT) --slave-port $(REPL_RDMA_SMOKE_SLAVE_PORT) --rdma-dev $(RDMA_DEV)

check-repl-rdma-stress:
	-kill $$(lsof -ti:$(REPL_RDMA_STRESS_MASTER_PORT)) 2>/dev/null
	-kill $$(lsof -ti:$(REPL_RDMA_STRESS_SLAVE_PORT)) 2>/dev/null
	-sleep 0.5
	ENABLE_RDMA=1 python3 ./tools/repl/run_repl_rdma_stress.py --bin ./kvstore --master-port $(REPL_RDMA_STRESS_MASTER_PORT) --slave-port $(REPL_RDMA_STRESS_SLAVE_PORT) --preload $(REPL_RDMA_STRESS_PRELOAD) --tail-writes $(REPL_RDMA_STRESS_TAIL_WRITES) --restart-rounds $(REPL_RDMA_STRESS_RESTART_ROUNDS) --wait 3 --rdma-recv-slots $(REPL_RDMA_TUNABLE_RECV_SLOTS) --rdma-chunk-size $(REPL_RDMA_TUNABLE_CHUNK_SIZE) --rdma-qp-wr-depth $(REPL_RDMA_TUNABLE_QP_WR_DEPTH) --rdma-dev $(RDMA_DEV)

check-repl-rdma-soak:
	-kill $$(lsof -ti:$(REPL_RDMA_STRESS_MASTER_PORT)) 2>/dev/null
	-kill $$(lsof -ti:$(REPL_RDMA_STRESS_SLAVE_PORT)) 2>/dev/null
	-sleep 0.5
	ENABLE_RDMA=1 python3 ./tools/repl/run_repl_rdma_stress.py --bin ./kvstore --master-port $(REPL_RDMA_STRESS_MASTER_PORT) --slave-port $(REPL_RDMA_STRESS_SLAVE_PORT) --preload $(REPL_RDMA_STRESS_PRELOAD) --tail-writes $(REPL_RDMA_STRESS_TAIL_WRITES) --restart-rounds $(REPL_RDMA_STRESS_RESTART_ROUNDS) --soak-seconds $(REPL_RDMA_SOAK_SECONDS) --soak-reconnect-interval $(REPL_RDMA_SOAK_RECONNECT_INTERVAL) --soak-write-interval-ms $(REPL_RDMA_SOAK_WRITE_INTERVAL_MS) --rdma-recv-slots $(REPL_RDMA_TUNABLE_RECV_SLOTS) --rdma-chunk-size $(REPL_RDMA_TUNABLE_CHUNK_SIZE) --rdma-qp-wr-depth $(REPL_RDMA_TUNABLE_QP_WR_DEPTH) --rdma-dev $(RDMA_DEV) $(if $(filter 1,$(REPL_RDMA_SOAK_ALLOW_FAILURE)),--allow-soak-failure,) $(if $(filter 1,$(REPL_RDMA_FORCE_FALLBACK)),--force-fallback,)

check-repl-rdma-long-soak:
	ENABLE_RDMA=1 python3 ./tools/repl/run_repl_rdma_stress.py --bin ./kvstore --master-port $(REPL_RDMA_STRESS_MASTER_PORT) --slave-port $(REPL_RDMA_STRESS_SLAVE_PORT) --preload $(REPL_RDMA_STRESS_PRELOAD) --tail-writes $(REPL_RDMA_STRESS_TAIL_WRITES) --restart-rounds $(REPL_RDMA_STRESS_RESTART_ROUNDS) --soak-seconds $(REPL_RDMA_LONG_SOAK_SECONDS) --soak-reconnect-interval $(REPL_RDMA_SOAK_RECONNECT_INTERVAL) --soak-write-interval-ms $(REPL_RDMA_SOAK_WRITE_INTERVAL_MS) --rdma-recv-slots $(REPL_RDMA_TUNABLE_RECV_SLOTS) --rdma-chunk-size $(REPL_RDMA_TUNABLE_CHUNK_SIZE) --rdma-qp-wr-depth $(REPL_RDMA_TUNABLE_QP_WR_DEPTH) --rdma-dev $(RDMA_DEV)

check-repl-rdma-fallback:
	ENABLE_RDMA=1 python3 ./tools/repl/run_repl_rdma_stress.py --bin ./kvstore --master-port $(REPL_RDMA_STRESS_MASTER_PORT) --slave-port $(REPL_RDMA_STRESS_SLAVE_PORT) --preload $(REPL_RDMA_STRESS_PRELOAD) --tail-writes $(REPL_RDMA_STRESS_TAIL_WRITES) --restart-rounds 1 --force-fallback

check-repl-rdma-soak-skip:
	@echo "Skipping RDMA soak check"

check-rdma-standalone-probe:
	python3 ./tools/rdma/run_rdma_standalone_probe.py

check-rdma-pingpong-smoke:
	python3 ./tools/rdma/run_rdma_pingpong_smoke.py --host $(RDMA_PINGPONG_HOST) --port $(RDMA_PINGPONG_PORT) --ib-dev $(RDMA_PINGPONG_DEV) --ib-port $(RDMA_PINGPONG_IB_PORT) --gid-idx $(RDMA_PINGPONG_GID_IDX)

# ---- 10w-level demo targets ----
check-demo-full-dump:
	python3 ./tools/persist/run_full_dump_10w_demo.py --bin ./kvstore --host $(TEST_HOST) --port $(FULL_DUMP_DEMO_PORT) --count $(FULL_DUMP_DEMO_COUNT)

check-demo-incr-aof:
	python3 ./tools/persist/run_incr_aof_10w_demo.py --bin ./kvstore --host $(TEST_HOST) --port $(INCR_AOF_DEMO_PORT) --count $(INCR_AOF_DEMO_COUNT)

check-demo-repl-sync:
	python3 ./tools/repl/run_repl_sync_10w_demo.py --bin ./kvstore --host $(TEST_HOST) --master-port $(REPL_SYNC_DEMO_MASTER_PORT) --slave-port $(REPL_SYNC_DEMO_SLAVE_PORT) --pre-count $(REPL_SYNC_DEMO_PRE) --post-count $(REPL_SYNC_DEMO_POST) --fullsync-transport $(REPL_SYNC_DEMO_FULLSYNC) --realtime-transport $(REPL_SYNC_DEMO_REALTIME) --rdma-dev $(REPL_SYNC_DEMO_RDMA_DEV)

check-all:
	python3 ./tools/tests/run_all_tests.py

check-all-quick:
	python3 ./tools/tests/run_all_tests.py --skip-rdma --skip-ebpf --skip-demo --skip-repl

# ---- 1w+ 大容量功能测试 ----
CHECK_10W_PORT?=5300
CHECK_10W_COUNT?=10000

check-10w:
	python3 ./tools/tests/run_10w_checks.py --bin ./kvstore --host $(TEST_HOST) --port $(CHECK_10W_PORT) --count $(CHECK_10W_COUNT)

check: check-resp check-ttl check-persist check-doc

.PHONY: all clean build_dir check-kvstore check-repl-5w5w test_persist_dump_demo test_persist_aof_demo check check-resp check-ttl check-persist check-doc check-bulk-1w check-all check-all-quick check-mass-ttl check-uring-persist check-mmap-recover check-repl check-repl-metrics check-repl-profile check-repl-ebpf check-repl-ebpf-env check-repl-ebpf-sync check-repl-ebpf-sync-required check-repl-ebpf-redirect check-repl-rdma-unsupported check-repl-rdma-smoke check-repl-rdma-stress check-repl-rdma-soak check-repl-rdma-soak-skip check-rdma-standalone-probe check-rdma-pingpong-smoke check-demo-full-dump check-demo-incr-aof check-demo-repl-sync test_uring_persist test_mmap_recover test_repl_basic check-uring-persist-c check-mmap-recover-c check-repl-basic check-persist-purge check-persist-save-frontier tools/ebpf/repl_ebpf_daemon ebpf-proxy client_capture_bpf
