# SAVE 性能结果分析

> 从 README 迁移的 SAVE 结果分析。README 只保留 SAVE 数据表。

## 结果分析

### ① SAVE 耗时与数据量正相关，呈近似线性


| 数据量 | 平均每次 SAVE | dump 文件大小 | 每条目 dump 字节 |
| ------ | ------------- | ------------- | ---------------- |
| 1000   | **3.6ms**     | 19.5 KB       | 19.5 B           |
| 1万    | **4.0ms**     | 196 KB        | 19.6 B           |
| 10万   | **10.8ms**    | 1.96 MB       | 19.6 B           |
| 100万  | **123.1ms**   | 19.6 MB       | 19.6 B           |

SAVE 耗时主要由 **遍历开销 + 缓冲 flush 次数** 决定。4MB 写缓冲使系统调用数从每 key 6-7 次降至每 4MB 1 次：

- ≤10 万 keys（~2MB dump）只触发 **1 次 write syscall**（未满缓冲，最终 flush 一次）
- 100 万 keys（~19.6MB dump）触发 **~5 次 write syscall**（19.6MB / 4MB）
- 小数据量（≤1万）的 ~3-4ms 地板时间来自 hash 桶遍历（4096+ slot 扫描）和 4MB buffer 分配

```c
int persist_save_dump(void) {
    // ① 打开文件 O_TRUNC
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0644);

    // ② 遍历引擎 → 4MB 用户态写缓冲（memcpy）→ 满时 flush write()
    kvs_dump_to_fd(fd);

    // ③ io_uring 异步 fsync（不阻塞）
    persist_fsync_fd(fd);
    close(fd);
}
```

`kvs_dump_to_fd()` 使用 4MB 用户态写缓冲：遍历 Hash 引擎 → `memcpy` 到缓冲 → 缓冲满或结束才 `write()`。100 万 key 只需 ~5 次 write 系统调用（vs 优化前 ~650 万次）。

### ② SAVE 开销大幅降低

- 100w→SAVE×1：写入 ~8s（100w / 125k QPS）+ SAVE 0.123s，SAVE 时间 123ms（遍历 + 缓冲 flush），开销占比仅 **1.5%**（优化前 33%）
- 1k→SAVE×10：单次 SAVE 3.6ms，1 千 key 写入 ~8ms，SAVE 占单批总时 ~31%，但小数据量 SAVE 绝对开销可忽略

### ③ 最佳策略：低频 BGSAVE + AOF

- **BGSAVE** 用于周期性全量备份（每小时或每 10 万次写入），fork 子进程异步执行，不阻塞主线程
- AOF everysec 用于增量持久化（最多丢 1 秒数据）
- 避免 SAVE（同步阻塞）——100 万 key 时阻塞主线程 ~123ms

生产环境应始终使用 **BGSAVE** 替代 SAVE：

```bash
# 配置自动 BGSAVE 规则（kvstore.conf）
autosnap 60 10000    # 60 秒内有 10000 次写入 → 自动 BGSAVE
autosnap 3600 0      # 每小时至少 BGSAVE 一次

# 或运行时动态配置
redis-cli -p 5160 SNAPRULE 60 10000
redis-cli -p 5160 SNAPRULES
```
