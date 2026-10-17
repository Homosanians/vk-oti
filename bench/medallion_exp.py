import json
import pathlib
import statistics

from benchmark import drop_os_caches
from queries import Q10
from trino_client import query

ROOT = pathlib.Path(__file__).resolve().parent.parent
SILVER_Q10 = (ROOT / "sql" / "03_gold.sql").read_text(encoding="utf-8")
SILVER_Q10 = SILVER_Q10[SILVER_Q10.index("SELECT\n  c.custkey"):].rstrip().rstrip(";")

VARIANTS = {
    "bronze": Q10.format(p="bronze.lakehouse.raw_"),
    "silver": SILVER_Q10,
    "gold": "SELECT * FROM gold.lakehouse.q10_top20_returned_customers ORDER BY revenue DESC",
}


def main():
    out = {}
    for name, sql in VARIANTS.items():
        drop_os_caches()
        cold = query(sql)
        warm = [query(sql) for _ in range(5)]
        out[name] = {
            "cold_s": cold["elapsed_s"], "cold_cpu_s": cold["cpu_s"],
            "cold_read_mb": round(cold["physical_input_bytes"] / 2**20, 1),
            "warm_median_s": statistics.median(w["elapsed_s"] for w in warm),
            "warm_cpu_median_s": statistics.median(w["cpu_s"] for w in warm),
            "rows": len(cold["rows"]),
        }
        r = out[name]
        print(f"  {name:<7} cold={r['cold_s']:7.2f}s (read {r['cold_read_mb']:8.1f} MB, cpu {r['cold_cpu_s']:6.1f}s) "
              f"warm={r['warm_median_s']:6.2f}s cpu={r['warm_cpu_median_s']:6.1f}s rows={r['rows']}", flush=True)
    (ROOT / "results" / "medallion_sf50.json").write_text(json.dumps(out, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
