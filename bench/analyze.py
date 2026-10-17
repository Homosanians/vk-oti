import argparse
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
PRICE_STORAGE_GB_MONTH = 0.023
PRICE_VM_HOUR = 0.768
PRICE_SCAN_TB = 5.0
RUNS_PER_MONTH = 1000
# Проверенные размеры текстовых .tbl от dbgen на sf1, байт.
RAW_TEXT_SF1 = {"customer": 24_346_144, "orders": 171_952_161, "lineitem": 759_863_287, "nation": 2_224}
MB = 2**20


def load(sf):
    path = ROOT / "results" / f"bench_sf{sf}.jsonl"
    recs = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
    order = {(f, c): i for i, (f, c) in enumerate(
        [(f, c) for f in ["PARQUET", "ORC"] for c in ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]])}
    return sorted(recs, key=lambda r: order[(r["format"], r["codec"])])


def unsupported(recs):
    return [f"{r['format']}/{r['codec']}: {r['unsupported']}" for r in recs if r.get("unsupported")]


def derive(r, sf, none_bytes):
    w = r["write"]
    size = sum(t["bytes"] for t in w.values())
    raw = sf * sum(v for k, v in RAW_TEXT_SF1.items() if k != "nation")
    write_s = sum(t["write_s"] for t in w.values())
    d = {
        "format": r["format"], "codec": r["codec"],
        "size_mb": size / MB, "files": sum(t["files"] for t in w.values()),
        "ratio_vs_none": none_bytes / size if none_bytes else None,
        "ratio_vs_text": raw / size,
        "write_s": write_s, "write_cpu_s": sum(t["write_cpu_s"] for t in w.values()),
        "write_mb_s": raw / MB / write_s,
        "meta_kb": r["file_meta"]["meta_bytes_total"] / 1024,
        "meta_pct": r["file_meta"]["meta_share_pct"],
        "groups": r["file_meta"]["row_groups_or_stripes"],
        "codec_in_file": ",".join(r["file_meta"]["codecs"]),
    }
    for q, v in r["trino"].items():
        d[f"trino_{q}_warm"] = v["warm_median_s"]
        d[f"trino_{q}_cold"] = v["cold_s"]
        d[f"trino_{q}_cpu"] = v["warm_cpu_median_s"]
        d[f"trino_{q}_read_mb"] = v["physical_input_bytes"] / MB
    for q, v in (r.get("spark") or {}).items():
        if not isinstance(v, dict):
            continue
        d[f"spark_{q}_warm"] = v["warm_median_s"]
        d[f"spark_{q}_cold"] = v["cold_s"]
    d["cost_storage"] = size / 2**30 * PRICE_STORAGE_GB_MONTH
    d["cost_vm"] = RUNS_PER_MONTH * (d["trino_q10_warm"] + d["trino_agg_scan_warm"]) / 3600 * PRICE_VM_HOUR
    d["cost_scan"] = RUNS_PER_MONTH * (d["trino_q10_read_mb"] + d["trino_agg_scan_read_mb"]) / 2**20 * PRICE_SCAN_TB
    return d


def table(rows, cols):
    head = "| " + " | ".join(c[0] for c in cols) + " |"
    sep = "|" + "|".join("---" if fmt is None else "---:" for _, _, fmt in cols) + "|"
    body = []
    for r in rows:
        cells = []
        for _, key, fmt in cols:
            v = r.get(key)
            cells.append("н/д" if v is None else (fmt.format(v) if fmt else str(v)))
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([head, sep] + body)


def best(rows, key, lo=True):
    r = (min if lo else max)(rows, key=lambda x: x[key])
    return f"{r['format']}/{r['codec']} ({r[key]:.2f})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    a = ap.parse_args()
    recs = load(a.sf)
    skipped = unsupported(recs)
    recs = [r for r in recs if not r.get("unsupported")]
    none = {r["format"]: sum(t["bytes"] for t in r["write"].values()) for r in recs if r["codec"] == "NONE"}
    rows = [derive(r, a.sf, none.get(r["format"])) for r in recs]
    has_spark = any("spark_q10_warm" in r for r in rows)

    out = [f"# Сводка бенчмарка, sf{a.sf}\n",
           f"Таблицы customer + orders + lineitem; исходный текст TPC-H примерно "
           f"{a.sf * sum(RAW_TEXT_SF1.values()) / 2**30:.1f} ГиБ.\n",
           "## 1. Размер и запись\n",
           table(rows, [("Формат", "format", None), ("Кодек", "codec", None),
                        ("Кодек в файле", "codec_in_file", None),
                        ("Размер, МБ", "size_mb", "{:,.0f}"), ("Файлов", "files", "{}"),
                        ("Сжатие к NONE, раз", "ratio_vs_none", "{:.2f}"), ("Сжатие к тексту, раз", "ratio_vs_text", "{:.2f}"),
                        ("Запись, с", "write_s", "{:.1f}"), ("CPU записи, с", "write_cpu_s", "{:.0f}"),
                        ("Запись, МБ/с текста", "write_mb_s", "{:.0f}")]),
           "\n## 2. Trino: время запросов (медиана 5 тёплых / холодный), с\n",
           table(rows, [("Формат", "format", None), ("Кодек", "codec", None),
                        ("Q10 тёпл.", "trino_q10_warm", "{:.2f}"), ("Q10 хол.", "trino_q10_cold", "{:.2f}"),
                        ("Q10 CPU", "trino_q10_cpu", "{:.1f}"), ("Q10 прочитано, МБ", "trino_q10_read_mb", "{:,.0f}"),
                        ("agg_scan тёпл.", "trino_agg_scan_warm", "{:.2f}"),
                        ("agg_scan хол.", "trino_agg_scan_cold", "{:.2f}"),
                        ("agg_scan CPU", "trino_agg_scan_cpu", "{:.1f}"),
                        ("agg_narrow тёпл.", "trino_agg_narrow_warm", "{:.2f}"),
                        ("agg_narrow хол.", "trino_agg_narrow_cold", "{:.2f}"),
                        ("agg_narrow прочитано, МБ", "trino_agg_narrow_read_mb", "{:,.0f}")])]
    if has_spark:
        out += ["\n## 3. Spark: время запросов (медиана 3 тёплых / первый прогон), с\n",
                table(rows, [("Формат", "format", None), ("Кодек", "codec", None),
                             ("Q10 тёпл.", "spark_q10_warm", "{:.2f}"), ("Q10 1-й", "spark_q10_cold", "{:.2f}"),
                             ("agg_scan тёпл.", "spark_agg_scan_warm", "{:.2f}"),
                             ("agg_scan 1-й", "spark_agg_scan_cold", "{:.2f}"),
                             ("agg_narrow тёпл.", "spark_agg_narrow_warm", "{:.2f}"),
                             ("agg_narrow 1-й", "spark_agg_narrow_cold", "{:.2f}")])]
    out += ["\n## 4. Метаданные внутри файлов lineitem\n",
            table(rows, [("Формат", "format", None), ("Кодек", "codec", None),
                         ("Row groups / stripes", "groups", "{}"), ("Футеры, КБ", "meta_kb", "{:,.1f}"),
                         ("Доля от данных, %", "meta_pct", "{:.4f}")]),
            f"\n## 5. Стоимость в месяц, $ (хранение {PRICE_STORAGE_GB_MONTH}/ГБ; ВМ {PRICE_VM_HOUR}/ч; "
            f"скан {PRICE_SCAN_TB}/ТБ; по {RUNS_PER_MONTH} запросов Q10 и agg_scan)\n",
            table(rows, [("Формат", "format", None), ("Кодек", "codec", None),
                         ("Хранение", "cost_storage", "{:.3f}"), ("Своя ВМ", "cost_vm", "{:.3f}"),
                         ("Serverless (за байты)", "cost_scan", "{:.3f}")]),
            "\n## 6. Лидеры\n",
            f"* меньше всего места: {best(rows, 'size_mb')} МБ",
            f"* быстрее всего запись: {best(rows, 'write_s')} с",
            f"* быстрее всего Q10 (Trino, тёпл.): {best(rows, 'trino_q10_warm')} с",
            f"* быстрее всего agg_scan (Trino, тёпл.): {best(rows, 'trino_agg_scan_warm')} с",
            f"* быстрее всего Q10 (Trino, холодный): {best(rows, 'trino_q10_cold')} с"]
    if has_spark:
        out.append(f"* быстрее всего Q10 (Spark, тёпл.): {best(rows, 'spark_q10_warm')} с")
    if skipped:
        out += ["\n## 7. Не поддерживается движком\n"] + [f"* {x}" for x in skipped]
    (ROOT / "results" / f"summary_sf{a.sf}.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    (ROOT / "results" / f"summary_sf{a.sf}.json").write_text(json.dumps(rows, indent=1), encoding="utf-8")
    print("\n".join(out))


if __name__ == "__main__":
    main()
