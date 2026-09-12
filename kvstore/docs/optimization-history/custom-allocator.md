# custom 分配器优化历程

custom slab 分配器（`src/memory/kvs_mem.c`）从初始实现到当前的优化历史，按内存占用维度演进。

> 当前数据与内存后端对比见 [`../memory-backend-analysis.md`](../data_analysis/memory-backend-analysis.md)；benchmark 原始数据在 `benchmarks/data/`。

四次优化将峰值 VmSize 从 97 MB 降到 70 MB，释放率从 0% 提升到 97%。

## Phase 1：空闲页回收（释放率 0% → 86%）

**问题**：`custom_free` 将 chunk 放回 `free_list`，但 slab 页（mmap）从不 munmap。删除 100w 数据后 95 MB 物理内存纹丝不动。

**方案**：per-page 追踪 `chunks_in_use`，整页空闲时 munmap 归还 OS。

**关键代码**：

```c
// slab_page_t 新增字段
size_t chunks_total;   // 该页总 chunk 数
size_t chunks_in_use;  // 当前已分配数（0 = 全空闲 → 回收）

// custom_free 递减后触发
if (pg->chunks_in_use == 0)
    try_reclaim_page_locked(&g_mem.classes[idx], pg);
```

**阈值保护**：至少保留 2 页的 free chunk 缓冲，防止 alloc/free 反复触发 mmap/munmap 颠簸。

**效果**：释放 100% 后 VmRSS 从 95 MB 降至 12 MB（仅比基线高 ~8 MB）。

## Phase 2：密集 slab class（内部碎片 100% → ≤25%）

**问题**：8 级 class `{32,64,128,256,384,512,768,1024}` 间距 2×，实际 46 字节请求落入 64B class 浪费 39%，80 字节请求落入 128B class 浪费 60%。

**方案**：参考 jemalloc（~1.19× 间距）和 TCMalloc（~1.12× 间距），扩展为 17 级 `{16,24,32,...,1024}`，~1.25× 几何级数，内部碎片上限控制在 25%。

**关键改动**：

```
SMALL_CLASS_COUNT  8 → 17
class sizes:  {32,64,128,...}  →  {16,24,32,40,56,72,96,128,160,200,256,320,400,512,640,800,1024}
                    2× 间距                        ~1.25× 间距
```

**效果**：46 字节请求 64B → 56B class（浪费 18B → 10B），每条目省 8 字节。峰值 VmSize 97 MB → 89 MB（-8.1%）。

```c
// class_index_for：遍历 class 数组找第一个 ≥ sz 的 class
// 分配时：chunk_total = sizeof(small_chunk_t) + class_size 决定每页 chunk 数
```

## Phase 3：压缩 chunk header（24B → 16B）

**问题**：`small_chunk_t` 有未使用 `reserved` 字段，且 `magic`/`class_idx`/`request_size` 全部宽存储，sizeof=24 字节。

**方案**：去掉 `reserved`，`magic` uint32→uint8（0xC0DEC0DE→0xCC），`class_idx` uint16→uint8，`request_size` uint32→uint16（max=1024）。`next` 指针放最前面确保 8 字节对齐，sizeof 从 24 降到 16。

```c
// 旧 (24B)                           新 (16B)
typedef struct small_chunk_s {        typedef struct small_chunk_s {
    uint32_t magic;     // 4B              struct small_chunk_s *next; // 8B
    uint16_t class_idx; // 2B              uint16_t request_size;       // 2B
    uint16_t reserved;  // 2B (废)          uint8_t  magic;              // 1B
    uint32_t request_size; // 4B           uint8_t  class_idx;          // 1B
    struct small_chunk_s *next; // 8B  } small_chunk_t;                 // 16B
} small_chunk_t;                     // 24B
```

**效果**：每条目 `chunk_total = 16 + 56 = 72B`（vs 旧 80B），省 8 字节 × 1M = 8 MB。峰值 VmSize 89 MB → **81 MB**（与 libc 差距 11%）。

## Phase 4：free_stack 索引（16B → 4B）

| 阶段                               | 峰值 VmSize   | vs libc   | bytes/条目 | 释放率    |
| ---------------------------------- | ------------- | --------- | ---------- | --------- |
| 初始 (8 class, 24B header, 无回收) | 96,592 KB     | +32%      | 98.9       | 0%        |
| P1: 空闲页回收                     | 96,592 KB     | +32%      | 98.9       | 86%       |
| P2: 17 级密集 class                | 88,764 KB     | +22%      | 90.9       | 86%       |
| P3: 压缩 header 24→16B            | 80,956 KB     | +11%      | 82.9       | 85%       |
| **P4: free_stack 索引 16→4B**     | **80,696 KB** | **-2.5%** | **82.6**   | **74.3%** |
| *libc 基线*                        | *82,764 KB*   | —        | *84.7*     | *32.4%*   |

**P4 已反超 libc**（80.7 MB < 82.8 MB，82.6 bytes/条目 < 84.7 bytes/条目）。用 per-page `uint16_t` 索引栈（LIFO）替代链表 `next` 指针，`small_chunk_t` 从 16B 压缩到 4B（request_size:2 + magic:1 + class_idx:1）。每条目 `chunk_total = 4 + 56 = 60B`，比 libc 的 malloc 元数据更紧凑。释放后残留 ~20 MB（基线 ~4 MB），来自 free_stack 元数据 + 阈值缓冲页。

**原理**：free list 链表将空闲 chunk 链接起来，每个 chunk 需要 8 字节 `next` 指针。用 per-page `uint16_t free_stack[]` 替代——slab 页最多 ~1000 chunks，2 字节足够索引。分配时 `free_stack[--free_top]` 弹出索引，释放时 `free_stack[free_top++] = ci` 压入索引，均为 O(1)。额外收益：`total_free_chunks = Σ page->free_top`（O(page_count)，比遍历链表 O(free_chunks) 快得多）。

## 2026-08-02 补充（P5：class 8/136 + array 惰性分配）

- **class 调优**（`src/memory/kvs_mem.c`）：17→19 级，加 class `8`（接 forward 8B，消除 50% 浪费）和 `136`（接 value 129，消除 24% 浪费）。效果：XSET 300k 内部碎片率 **14.1%→5.0%**、RSS **94→86MB**（贴平 libc/jemalloc 87MB），HSET 与吞吐无回退。skiptable 节点做 3 次分配，value 129 是主碎片源（详见 [`../memory-backend-analysis.md`](../data_analysis/memory-backend-analysis.md) §4）。
- **array 惰性分配**（`src/storage/kvs_array.c`）：`KVS_ARRAY_SIZE` 提到 1M 槽后 `kvs_array_create` 无条件预分配 16MB。改为首次 SET 才分配（`table=NULL` 惰性），基线 VmSize 36.7→**20.3MB**、VmRSS 21→**4.6MB**，SET/SAVE/恢复验证通过（见 [`../memory-backend-analysis.md`](../data_analysis/memory-backend-analysis.md) §5）。
