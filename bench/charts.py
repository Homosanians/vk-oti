import argparse
import json
import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = pathlib.Path("/opt")
RES, DOCS = BASE / "results", BASE / "docs"
COL = {"PARQUET": "#2a78d6", "ORC": "#eb6834"}
SINGLE = "#2a78d6"
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
CODECS = ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]

plt.rcParams.update({
    "figure.facecolor": SURF, "axes.facecolor": SURF, "axes.edgecolor": GRID,
    "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2, "text.color": INK,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
})


def grouped(rows, key, title, ylabel, fname, fmt="{:.0f}"):
    fig, ax = plt.subplots(figsize=(8, 4.2))
    w = 0.38
    for i, f in enumerate(["PARQUET", "ORC"]):
        vals = [next((r[key] for r in rows if r["format"] == f and r["codec"] == c), None) for c in CODECS]
        xs = [j + (i - 0.5) * (w + 0.02) for j in range(len(CODECS))]
        bars = ax.bar([x for x, v in zip(xs, vals) if v is not None], [v for v in vals if v is not None],
                      width=w, color=COL[f], label=f, edgecolor=SURF, linewidth=1)
        for b in bars:
            ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                        ha="center", va="bottom", fontsize=8, color=INK2, xytext=(0, 2),
                        textcoords="offset points")
        for x, v in zip(xs, vals):
            if v is None:
                ax.text(x, 0, "не\nподдерж.", ha="center", va="bottom", fontsize=7, color=INK2)
    ax.set_xticks(range(len(CODECS)), CODECS)
    ax.set_ylabel(ylabel)
    ax.set_title(title, loc="left", fontsize=12, color=INK)
    ax.legend(frameon=False, loc="upper right")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(DOCS / fname, dpi=140)
    plt.close(fig)


def tradeoff(rows, sf):
    fig, ax = plt.subplots(figsize=(8, 4.8))
    for f in ["PARQUET", "ORC"]:
        rs = [r for r in rows if r["format"] == f]
        ax.scatter([r["size_mb"] / 1024 for r in rs], [r["trino_q10_cold"] for r in rs], s=60,
                   color=COL[f], label=f, edgecolor=SURF, linewidth=2, zorder=3)
        for r in rs:
            # Подпись GZIP сдвинута, чтобы не налезала на ZSTD.
            off = (-34, -12) if r["codec"] == "GZIP" else (6, 4)
            ax.annotate(r["codec"], (r["size_mb"] / 1024, r["trino_q10_cold"]), fontsize=8, color=INK2,
                        xytext=off, textcoords="offset points")
    ax.set_xlabel("Размер customer+orders+lineitem, ГиБ")
    ax.set_ylabel("Q10 в Trino, холодный прогон (чтение с диска), с")
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    ax.set_title(f"Компромисс «размер и скорость», sf{sf} (левее и ниже лучше)", loc="left", fontsize=12)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(DOCS / f"tradeoff_sf{sf}.png", dpi=140)
    plt.close(fig)


def hbars(labels, vals, title, xlabel, fname, fmt="{:.2f}"):
    fig, ax = plt.subplots(figsize=(8, 0.45 * len(labels) + 1.2))
    bars = ax.barh(labels[::-1], vals[::-1], color=SINGLE, height=0.55, edgecolor=SURF)
    for b in bars:
        ax.annotate(fmt.format(b.get_width()), (b.get_width(), b.get_y() + b.get_height() / 2),
                    va="center", fontsize=8, color=INK2, xytext=(3, 0), textcoords="offset points")
    ax.set_xlabel(xlabel)
    ax.set_title(title, loc="left", fontsize=12)
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    fig.savefig(DOCS / fname, dpi=140)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    sf = ap.parse_args().sf
    DOCS.mkdir(exist_ok=True)
    rows = json.loads((RES / f"summary_sf{sf}.json").read_text())
    for r in rows:
        r["size_gb"] = r["size_mb"] / 1024
    grouped(rows, "size_gb", f"Размер данных по кодекам, sf{sf}", "ГиБ", f"size_sf{sf}.png", "{:.1f}")
    grouped(rows, "write_s", f"Время записи (CTAS 3 таблиц), sf{sf}", "секунды", f"write_sf{sf}.png")
    grouped(rows, "trino_q10_warm", f"Q10 в Trino, медиана тёплых прогонов, sf{sf}", "секунды",
            f"trino_q10_sf{sf}.png", "{:.1f}")
    grouped(rows, "trino_q10_cold", f"Q10 в Trino, холодный прогон (page cache сброшен), sf{sf}", "секунды",
            f"trino_q10_cold_sf{sf}.png", "{:.1f}")
    grouped(rows, "trino_agg_scan_warm", f"agg_scan (TPC-H Q1) в Trino, медиана, sf{sf}", "секунды",
            f"trino_scan_sf{sf}.png", "{:.1f}")
    if any("spark_q10_warm" in r for r in rows):
        grouped(rows, "spark_q10_warm", f"Q10 в Spark, медиана тёплых прогонов, sf{sf}", "секунды",
                f"spark_q10_sf{sf}.png", "{:.1f}")
    tradeoff(rows, sf)

    meta = RES / f"metadata_sf{sf}.json"
    if meta.exists():
        v = json.loads(meta.read_text())["variants"]
        hbars([x["variant"] for x in v], [x["warm_planning_median_ms"] for x in v],
              f"Время планирования запроса к orders, sf{sf}", "мс (медиана 5)", f"meta_planning_sf{sf}.png", "{:.0f}")
        hbars([x["variant"] for x in v], [x["files"] for x in v],
              f"Число файлов данных orders, sf{sf}", "файлов", f"meta_files_sf{sf}.png", "{:.0f}")
    cache = RES / f"cache_sf{sf}.json"
    if cache.exists():
        c = json.loads(cache.read_text())["trino"]["q10"]
        hbars(list(c), [c[k]["elapsed_s"] for k in c], f"Q10 в Trino по уровням кэша, sf{sf}",
              "секунды", f"cache_q10_sf{sf}.png")
    print("charts ->", sorted(p.name for p in DOCS.glob(f"*sf{sf}.png")))


if __name__ == "__main__":
    main()
