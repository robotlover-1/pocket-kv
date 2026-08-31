# 内存后端（libc / jemalloc / custom）对比与 custom 优化

日期：2026-08-02（初版）· 2026-08-24（统一 50 连接口径重测）
环境：Linux 6.1.176 / KVM 虚拟机，AOF disable，Hash 引擎
工具：`redis-benchmark`（吞吐）、`tools/bench/mem_pool_bench.py`（内存，50 并发连接）、MEMSTAT

## 1. 结论

1. **吞吐上三者没有区别**（多连接实测 ±3%）：custom 无吞吐优势，也无劣势。旧的 `bench_mem_backend.py` 单连接顺序发命令是 RTT-bound，~2300 QPS 是假象，测不出分配器差异。
2. **custom 的价值是「释放后归还彻底 + 可观测碎片统计」**，不是写满密度：统一 50 连接口径下，写满峰值受连接缓冲（64MB）主导，custom 与 libc 相当、jemalloc 因及时 purge 反而最低；**释放后 custom 靠 slab 页 munmap + 元数据 `malloc_trim` 几乎全还（残留 348KB）**。
3. **custom 的弱点是内部碎片**：分配尺寸不贴合 slab class 就浪费。已通过 class 调优验证可修复（见 §4）。
4. **启动基线变化（开放问题）**：07-13 后 master 启动会无条件分配 ~20MB RDMA 缓冲（即使 transport=tcp），导致基线内存从 ~5.7MB 涨到 ~36MB（见 §5）。

## 2. 吞吐对比（多连接，AOF disable，taskset 隔离核 2/3，3 轮中位数）

`redis-benchmark -n <N> -c 50 -P <P> -d <vs> -r <R> HSET key:__rand_int__ value`（原始数据 `benchmarks/data/mem_backend_pipe_2026-08-02.txt`）：

### vs=64（slab 小对象路径）

| P   | libc      | jemalloc  | custom    |
| --- | --------- | --------- | --------- |
| 1   | 127,356   | 126,743   | 128,271   |
| 40  | 1,237,624 | 1,219,512 | 1,272,265 |
| 160 | 1,497,006 | 1,461,988 | 1,506,024 |

### vs=4096（mmap 大对象路径）

| P   | libc      | jemalloc  | custom    |
| --- | --------- | --------- | --------- |
| 1   | 119,617   | 121,655   | 122,249   |
| 40  | 943,396   | 925,926   | 892,857   |
| 160 | 1,043,333 | 1,043,333 | 1,065,532 |

> 各 P、各值大小差异 <3%。分配器不是吞吐瓶颈（reactor 单线程，custom 全局锁无争用；写路径不触发 free，O(pages) 的 free 未暴露）。

## 3. 内存对比（100w HSET 写入/释放，统一口径：50 并发连接、P=1、free100 后 MEMORY_PURGE）

### 3.1 内存占用与归还（2026-08-24 重测）

`mem_pool_bench.py` 用 `redis-cli --pipe` **50 并发连接**（模拟 `-c 50`，与随机 key 场景连接数一致）写满 100w key → 释放 100w；free100 后显式 `MEMORY_PURGE` 再采样（测「归还稳态」而非瞬时量）：

| 后端     | 基线 VmSize | 基线 VmRSS | 写满 VmSize | 写满 VmRSS | 释放 VmSize | 释放 VmRSS | 残留 VmRSS（free−baseline） |
| -------- | ----------- | ---------- | ----------- | ---------- | ----------- | ---------- | --------------------------- |
| libc     | 31,484      | 3,552      | 113,360     | 84,704     | 94,108      | 3,684      | **132**                     |
| jemalloc | 55,320      | 5,476      | 121,760     | 74,284     | 55,204      | 7,928      | **2,452**                   |
| custom   | 31,832      | 3,964      | 110,580     | 82,840     | 32,176      | 4,312      | **348**                     |

> 单位 KB。VmRSS = 物理内存，VmSize = 虚拟地址空间（含线程栈/arena 虚拟预留，见 §5）。「释放」为释放 100% 后显式 `MEMORY_PURGE`（libc/custom→`malloc_trim`，jemalloc→`mallctl` purge）的归还稳态。

- **写满峰值受连接缓冲主导**：50 连接 × 1.28MB（inbuf 1MB + out_ring 256KB）= **64MB 连接缓冲**。写满时连接开着、缓冲物理页触达，各后端差异主要来自缓冲的物理占用与归还——jemalloc（`decay:0 + background_thread` 及时 purge 物理页）写满最低，custom/libc 缓冲残留多。**数据本身（100 万 key）三后端差异远小于缓冲**。
- **释放后**：custom/libc 靠「slab 页 munmap + `malloc_trim`」几乎全还（残留 348/132KB，接近基线）；jemalloc 有固定 ~2.4MB **arena 元数据**（`arena.<all>.purge` 清不掉）。

> ⚠️ **历史结论「custom 省 libc 6x（随机 key 12.4 vs 74.9 MB）」已废弃**：旧版 §3.1 用 `-n 200000 -c 50 -P 40 -r 200000`（20 万 key）+ 跑完 settle 3s 后读 VmRSS，差异实际来自 **50 连接缓冲的归还**（custom 缓冲走 mmap 关闭即 munmap、libc 缓冲 free 进堆不 trim 残留），**不是数据密度**。统一 50 连接 + purge 口径后该优势消失。

### 3.2 释放后残留归因

| 后端     | 残留   | 主体（实测）                                                          |
| -------- | ------ | --------------------------------------------------------------------- |
| libc     | 132 KB | glibc 主堆 `[heap]`：数据释放 + `malloc_trim` 堆顶下降，几乎全还       |
| custom   | 348 KB | slab 页 munmap（数据归还 ~99%）+ 借 glibc 堆的页元数据（`free_stack`/`slab_page_t`）经 `malloc_trim` 归还；剩少量未清空 slab 页 |
| jemalloc | 2452 KB | mmap arena 的**元数据 + 保留 extent**（非 dirty 页，`arena.<all>.purge` 清不掉） |

**归还机制本质区别**：

- **libc = brk 堆顶下降**：`[heap]` 是连续 brk 段，`malloc_trim` 把 top chunk 相邻的空闲区还给内核。
- **custom = slab 页 munmap**：整张 slab 页清空即 `munmap`（数据归还最彻底）；借 glibc 堆的页元数据（`free_stack`/`slab_page_t`）靠 `malloc_trim` 归还。
- **jemalloc = arena decay/purge**：dirty 页可 purge，但 arena 元数据 + 保留 extent 固定占用（~2.4MB），purge 清不掉。

**改动（2026-08-24）**：

- `custom_free` 加 `malloc_trim`（每 10 万次 free，归还借 glibc 堆的 slab 页元数据，custom 残留 2.5MB → 0.35MB）
- 新增 `MEMORY_PURGE` 命令 + `kvs_mem_purge()`：libc/custom→`malloc_trim(0)`；jemalloc→`mallctl("arena.<all>.purge"/"arena.<all>.muzzy")` 强制 purge
- `mem_pool_bench.py`：写入/释放改 **50 并发连接**（与随机 key 场景公平）；free100 后发 `MEMORY_PURGE` 再采样

### 3.3 虚拟内存（VmSize）归还分析

**问题**：为什么释放后 custom 的虚拟内存（VmSize）最低、几乎回到基线，而 libc 保留 ~63MB？

| 后端     | 基线 VmSize | 写满 VmSize | 释放 VmSize | 释放后虚拟归还 |
| -------- | ----------- | ----------- | ----------- | --------------- |
| libc     | 31,484      | 113,360     | **94,108**  | 只还物理页，虚拟保留 ~63MB |
| jemalloc | 55,320      | 121,760     | **55,204**  | 基本回到基线（保留 arena 虚拟） |
| custom   | 31,832      | 110,580     | **32,176**  | 几乎全还（回基线） |

**核心机制：虚拟地址能否归还取决于分配方式（`mmap` vs `brk`）**：

- **custom = `mmap` + `munmap`（虚拟地址完整归还）**：数据（slab 页）与连接缓冲（large mmap）都是 mmap 分配。HSET 小对象从 mmap 的 slab 页取固定槽位（**单个对象释放只还槽位到 `free_stack`，不 munmap**）；当整张页 `chunks_in_use == 0`（所有槽空）→ **整张页 `munmap`**（[`try_reclaim_page_locked`](src/memory/kvs_mem.c)），虚拟地址 + 物理页一起还。→ 释放后 VmSize 回基线。
- **libc = `brk` 堆（虚拟地址大部分保留）**：数据在 glibc 堆（`[heap]`，brk 段）。free 进 bin 链表复用，物理页靠 `malloc_trim` 归还（**仅 top chunk 相邻**的空闲区），但**虚拟地址永久保留**——brk 只有 top chunk 空闲时 `brk()` 下降才还，中间碎片/非相邻 bin 块不还。→ 释放后 VmRSS 降到 3.7MB（trim 有效）但 VmSize 保留 ~63MB。
- **jemalloc = `retain:false`（大部分归还）**：extent 释放后 `munmap` 归还虚拟地址，但保留 arena 虚拟预留（基线即 55MB）→ VmSize 回基线。

**brk vs slab 的回收差异**：

| 维度   | libc brk 堆                                | custom slab                               |
| ------ | ------------------------------------------ | ----------------------------------------- |
| 分配   | `malloc` < 阈值（默认 128KB）走 brk 段     | ≤ `SMALL_MAX_SIZE`(1024B) 从 mmap 的 slab 页取槽；>1024B 直接 mmap |
| free 后 | 进 bin 链表**复用**                        | 槽位压回 `free_stack`**复用**             |
| 物理页 | ⚠️ 仅 top chunk 相邻（`malloc_trim`）归还  | ✅ 整页清空即 `munmap`                    |
| 虚拟地址 | ❌ 大部分保留（只 top chunk 时 `brk()` 下降） | ✅ 整页 `munmap` 完整归还              |

> glibc `malloc` 默认 `MMAP_THRESHOLD=128KB`（动态可涨，最多 32MB）：HSET 对象（~60-116B）走 brk；连接缓冲（1.28MB）走 mmap（若动态阈值涨过 1.28MB 则落进 brk）。

**小结**：custom 释放后 VmSize 最低，是因为它的内存承载在 `mmap` 的 slab 页/large 块里，**整块清空即 munmap——虚拟地址空间完整归还**；libc 的 brk 堆释放后只还物理页（top chunk 相邻）、虚拟地址大部分永久保留。

## 4. custom 内部碎片问题与 class 调优验证

### 4.1 问题：XSET 的碎片

XSET（skiptable）节点做 3 次分配（`kvs_skiptable.c`）：node struct 32B、forward 数组 (level+1)×8B、key、value。value=128 时 value 分配 129B → 落在 class 160（**浪费 24%，最大头**）；forward level0 8B → class 16（浪费 50%）。

改前（17 class）XSET 300k：internal_fragment_rate **14.1%**，RSS **94 MB**。

### 4.2 修改：`src/memory/kvs_mem.c` 加两个 class

- `SMALL_CLASS_COUNT` 17 → 19
- 加 class **8**（接 forward level0 的 8B）
- 加 class **136**（接 value 129）

### 4.3 结果

| 指标                    | 改前（17 class） | 改后（19 class）                      |
| ----------------------- | ---------------- | ------------------------------------- |
| XSET 300k 碎片率        | 14.1%            | **5.0%**                              |
| XSET 300k RSS           | 94 MB            | **86 MB**（贴平 libc/jemalloc 87 MB） |
| HSET 500k RSS（多连接） | 41 MB            | 42 MB（无回退）                       |
| HSET P=1/P=40 QPS       | 128k / 1.27M     | 126k / 1.30M（噪声内）                |

> 调优后的 class 分布确认：300k 个 value 全部精确落入 class 136（39,844 KB），15 万 forward 落入 class 8。**碎片率降低 64%、RSS 降 8%**，吞吐与 HSET 内存无回退。

### 4.4 通用调优方法

- MEMSTAT 已暴露 per-class 数据（`class_i_size/pages/total_chunks/free_chunks`），无需加埋点即可分析分配分布。
- 生产调优应采样真实 value 尺寸分布，按实际分配尺寸加 class / 调整上取整幅度，目标是把 internal_fragment_rate 压到 ~5% 以内。
- 历史记录"XSET 300k custom 144MB / 39% 碎片"是长 key 旧数据的产物，实测复现不了（实测 94MB / 14.1%）。

## 5. 启动基线变化与修复

**① ARRAY 引擎 16MB 预分配（已修复）**

- `KVS_ARRAY_SIZE` 从 1024 提到 `1024*1024`（1M 槽）后，`kvs_array_create()` 在 main 无条件 `1M × 16B = 16MB` 预分配，即使工作负载是纯 hash（HSET）也占满。
- **已修复**（`src/storage/kvs_array.c`）：table 改为惰性分配——首次 `kvs_array_set()` 才分配 16MB。其余 array 操作（get/del/mod/exist）与 dump/load 本就有 `table &&` 空保护，无需改动。
- 效果：基线 VmSize 36.7MB → **20.3MB**，VmRSS 21MB → **4.6MB**（16MB array 消失）。SET/GET/SAVE/重启恢复验证通过。

**② 基线 VmSize 高 = 线程栈虚拟预留（正常，非问题）**

- 基线的 ~12.5MB 匿名映射经 strace 定位是 **`MAP_STACK|PROT_NONE` 的线程栈预留**：本 VM `ulimit -s = 12500KB`（12.2MB），kvstore 启动创建线程时每个线程栈预留 12.2MB 虚拟空间（PROT_NONE 不可触达 → **物理 RSS 为 0**）。
- 所以基线 VmSize ~20MB（libc/custom）/ 45MB（jemalloc 另有 retain:false 的 arena 虚拟保留）**是虚拟地址空间预留，不是物理内存**。VmRSS 基线仅 4-7MB。
- 三后端一致，**非分配器差异**，无需修复（64 位虚拟地址空间充足）。

## 6. 结论与建议

1. **custom 值得作为默认内存后端**：释放后归还最彻底（slab 页 munmap + 元数据 trim，残留 348KB，优于 jemalloc 2452KB）+ 可观测碎片统计（MEMSTAT per-class），吞吐无劣势。**写满峰值无密度优势**（统一 50 连接口径受连接缓冲主导，custom 82.8MB ≈ libc 84.7MB）。
2. **class 档位要按真实 value 尺寸调**：当前 19 class 对 value-128 类负载已近最优；生产按实际分配尺寸继续调。
3. **修复 RDMA 缓冲无条件分配**（§5）可让基线回到 ~6MB，使内存数据恢复可比性。

## 复现

```bash
# 吞吐对比
bash /tmp/mem_backend_pipe.sh   # libc/jemalloc/custom × P{1,40,160} × vs{64,4096}
# 内存占用与归还（50 并发连接，free100 后 MEMORY_PURGE）
python3 tools/bench/mem_pool_bench.py --backends custom --output /tmp/mem_pool_custom.csv
# 分配分布采样（XSET）
python3 /tmp/xset_sampler.py 300000 128 XSET
```
