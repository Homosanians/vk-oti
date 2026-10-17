import argparse
import json
import pathlib
import time

from trino_client import query, run_script, scalar

ROOT = pathlib.Path(__file__).resolve().parent.parent

LAYER_CODEC = {"silver": "ZSTD", "gold": "SNAPPY"}


def table_stats(fqn):
    cat, sch, tbl = fqn.split(".")
    rows = scalar(f"SELECT count(*) FROM {fqn}")
    files, size = query(
        f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM {cat}.{sch}."{tbl}$files"'
    )["rows"][0]
    snaps = scalar(f'SELECT count(*) FROM {cat}.{sch}."{tbl}$snapshots"')
    return {"table": fqn, "rows": rows, "files": files, "bytes": size,
            "mb": round(size / 2**20, 2), "snapshots": snaps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--bronze-codec", default="ZSTD")
    ap.add_argument("--bronze-format", default="PARQUET")
    ap.add_argument("--skip-bronze", action="store_true")
    ap.add_argument("--skip-validate", action="store_true")
    a = ap.parse_args()
    params = {"SF": a.sf, "BRONZE_FORMAT": a.bronze_format, "BRONZE_CODEC": a.bronze_codec}
    out = {"sf": a.sf, "bronze_format": a.bronze_format, "bronze_codec": a.bronze_codec,
           "layer_codec": LAYER_CODEC, "steps": {}, "tables": []}

    steps = [("bronze", "01_bronze.sql", a.bronze_codec),
             ("silver", "02_silver.sql", LAYER_CODEC["silver"]),
             ("gold", "03_gold.sql", LAYER_CODEC["gold"])]
    for layer, script, codec in steps:
        if layer == "bronze" and a.skip_bronze:
            continue
        print(f"== {layer} ({codec})", flush=True)
        t0 = time.perf_counter()
        res = run_script(ROOT / "sql" / script, params,
                         session={f"{layer}.compression_codec": codec})
        out["steps"][layer] = {
            "codec": codec,
            "wall_s": round(time.perf_counter() - t0, 1),
            "statements": [{"sql": " ".join(s.split())[:120], "elapsed_s": r["elapsed_s"],
                            "cpu_s": r["cpu_s"]} for s, r in res],
        }

    for fqn in ["bronze.lakehouse.raw_nation", "bronze.lakehouse.raw_customer",
                "bronze.lakehouse.raw_orders", "bronze.lakehouse.raw_lineitem",
                "silver.lakehouse.dim_customer", "silver.lakehouse.fact_order_line",
                "silver.lakehouse.dq_report",
                "gold.lakehouse.returned_revenue_by_customer_quarter",
                "gold.lakehouse.q10_top20_returned_customers"]:
        st = table_stats(fqn)
        print(f"  {st['table']:<55} rows={st['rows']:>12,} files={st['files']:>5} {st['mb']:>10.1f} MB")
        out["tables"].append(st)

    out["dq_report"] = query("SELECT rule, failed_rows FROM silver.lakehouse.dq_report")["rows"]
    out["gold_q10"] = query("SELECT * FROM gold.lakehouse.q10_top20_returned_customers "
                            "ORDER BY revenue DESC")
    if not a.skip_validate:
        print("== validate gold vs tpch reference", flush=True)
        diff = run_script(ROOT / "sql" / "04_validate.sql", params)[-1][1]
        out["validation"] = {"diff_rows": diff["rows"], "elapsed_s": diff["elapsed_s"],
                             "ok": len(diff["rows"]) == 0}
        print("  VALIDATION", "OK" if not diff["rows"] else f"FAILED: {diff['rows']}")

    path = ROOT / "results" / f"pipeline_sf{a.sf}.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("saved", path)


if __name__ == "__main__":
    main()
