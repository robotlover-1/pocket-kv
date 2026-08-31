#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except Exception as e:
    raise SystemExit(f"matplotlib is required: {e}")

METRICS = [
    ("qps", "QPS", "qps_grouped.png"),
    ("vmrss_kb", "VmRSS (KB)", "vmrss_grouped.png"),
    ("vmsize_kb", "VmSize (KB)", "vmsize_grouped.png"),
    ("mem_gap_kb", "VmSize - VmRSS (KB)", "memgap_grouped.png"),
    ("internal_fragment_rate", "Internal Fragmentation Rate", "internal_fragment_rate_grouped.png"),
    ("page_utilization", "Page Utilization", "page_utilization_grouped.png"),
    ("fallback_alloc_calls", "Fallback Alloc Calls", "fallback_grouped.png"),
]

BACKEND_ORDER = ["libc", "jemalloc", "custom"]


def load_rows(csv_path: Path):
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise RuntimeError(f"no rows found in {csv_path}")
    return rows


def scenario_tuple(row):
    return (
        str(row.get("cmd", "")).upper(),
        int(float(row.get("ops", 0) or 0)),
        int(float(row.get("value_size", 0) or 0)),
        str(row.get("run_label", "")),
    )


def scenario_label(sc):
    cmd, ops, value_size, run_label = sc
    return f"{run_label}\n{cmd} ops={ops:,}\nvalue={value_size}B"


def filter_rows(rows, cmd: str):
    if cmd:
        rows = [r for r in rows if str(r.get("cmd", "")).upper() == cmd.upper()]
    if not rows:
        raise RuntimeError("no rows matched the selected filters")
    return rows


def to_float(v):
    if v in (None, ""):
        return 0.0
    return float(v)


def aggregate(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(scenario_tuple(row), row.get("backend", "unknown"))].append(row)
    scenarios = sorted({k[0] for k in grouped}, key=lambda x: (x[0], x[1], x[2], x[3]))
    backends = sorted({k[1] for k in grouped}, key=lambda x: BACKEND_ORDER.index(x) if x in BACKEND_ORDER else 99)
    data = {metric: {backend: [] for backend in backends} for metric, _, _ in METRICS}
    for sc in scenarios:
        for backend in backends:
            rows_here = grouped.get((sc, backend), [])
            for metric, _, _ in METRICS:
                vals = [to_float(r.get(metric, 0)) for r in rows_here]
                data[metric][backend].append(sum(vals) / len(vals) if vals else 0.0)
    return scenarios, backends, data


def plot_grouped(scenarios, backends, data, metric, ylabel, out_path: Path):
    values = data[metric]
    if metric == "fallback_alloc_calls" and all(all(v == 0 for v in values[b]) for b in backends):
        return
    n = len(scenarios)
    x = list(range(n))
    width = 0.22 if len(backends) <= 3 else 0.8 / max(len(backends), 1)
    offsets = [((i - (len(backends)-1)/2) * width) for i in range(len(backends))]
    plt.figure(figsize=(max(10, 2.6 * n), 6))
    ax = plt.gca()
    for i, backend in enumerate(backends):
        xpos = [xi + offsets[i] for xi in x]
        vals = values[backend]
        bars = ax.bar(xpos, vals, width=width, label=backend)
        ymax = max(vals) if vals else 0.0
        offset = ymax * 0.01 if ymax > 0 else 0.01
        for bar, v in zip(bars, vals):
            label = f"{v:.2f}" if abs(v) < 1000 else f"{v:.0f}"
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + offset, label, ha="center", va="bottom", fontsize=8, rotation=0)
    ax.set_xticks(x)
    ax.set_xticklabels([scenario_label(sc) for sc in scenarios])
    ax.set_ylabel(ylabel)
    ax.set_title(f"{ylabel} grouped by scenario and memory backend")
    ax.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close()


def write_summary(out_dir: Path, scenarios, backends, data):
    p = out_dir / "grouped_summary.txt"
    with p.open("w", encoding="utf-8") as f:
        for idx, sc in enumerate(scenarios):
            f.write(scenario_label(sc).replace("\n", " | ") + "\n")
            for backend in backends:
                f.write(
                    f"  {backend}: qps={data['qps'][backend][idx]:.2f}, vmrss_kb={data['vmrss_kb'][backend][idx]:.2f}, "
                    f"vmsize_kb={data['vmsize_kb'][backend][idx]:.2f}, mem_gap_kb={data['mem_gap_kb'][backend][idx]:.2f}, "
                    f"internal_fragment_rate={data['internal_fragment_rate'][backend][idx]:.6f}, page_utilization={data['page_utilization'][backend][idx]:.6f}, "
                    f"fallback_alloc_calls={data['fallback_alloc_calls'][backend][idx]:.2f}\n"
                )
            f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Plot grouped bar charts from cumulative kvstore benchmark CSV.")
    parser.add_argument("--csv", default="bench_results_all.csv")
    parser.add_argument("--outdir", default="bench_grouped_plots")
    parser.add_argument("--cmd", default="", help="optional command filter, e.g. HSET")
    args = parser.parse_args()

    rows = filter_rows(load_rows(Path(args.csv)), args.cmd)
    scenarios, backends, data = aggregate(rows)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for metric, ylabel, filename in METRICS:
        plot_grouped(scenarios, backends, data, metric, ylabel, out_dir / filename)
    write_summary(out_dir, scenarios, backends, data)
    print(f"wrote grouped plots to {out_dir.resolve()}")
    for p in sorted(out_dir.iterdir()):
        print(p.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
