import argparse
import json
import pathlib
import statistics
import subprocess
import time

from benchmark import drop_os_caches, sh
from queries import QUERIES
from trino_client import query

ROOT = pathlib.Path(__file__).resolve().parent.parent
NAMES = ["q10", "agg_scan"]


def wait_trino():
    while True:
        try:
            if query("SELECT 1")["rows"]:
                return
        except Exception:
            time.sleep(2)


def one(sql):
    r = query(sql)
    return {"elapsed_s": r["elapsed_s"], "cpu_s": r["cpu_s"], "physical_input_bytes": r["physical_input_bytes"]}


def warm5(sql):
    rs = [query(sql) for _ in range(5)]
    return {"elapsed_s": statistics.median(r["elapsed_s"] for r in rs),
            "cpu_s": statistics.median(r["cpu_s"] for r in rs),
            "physical_input_bytes": rs[0]["physical_input_bytes"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--no-spark", action="store_true")
    ap.add_argument("--schema", default="lakehouse", help="схема Bronze с таблицами raw_*")
    a = ap.parse_args()
    out = {"sf": a.sf, "trino": {}, "spark": None}

    for name in NAMES:
        plain = QUERIES[name].format(p=f"bronze.{a.schema}.raw_")
        cached = QUERIES[name].format(p=f"bronze_cached.{a.schema}.raw_")
        res = {}
        sh("docker restart trino"); wait_trino()
        drop_os_caches()
        res["jvm_cold"] = one(plain)
        drop_os_caches()
        res["os_cold"] = one(plain)
        res["os_warm"] = warm5(plain)
        sh(["docker", "exec", "trino", "sh", "-c", "rm -rf /tmp/trino-cache/*"])
        sh("docker restart trino"); wait_trino()
        drop_os_caches()
        res["fs_cache_fill"] = one(cached)
        drop_os_caches()
        res["fs_cache_hit"] = one(cached)
        res["fs_cache_warm"] = warm5(cached)
        out["trino"][name] = res
        for k, v in res.items():
            print(f"  trino {name:<9} {k:<14} {v['elapsed_s']:7.2f}s  cpu={v['cpu_s']:7.1f}s "
                  f"read={v['physical_input_bytes'] / 2**20:9.1f} MB", flush=True)
    floor = []
    for _ in range(3):
        drop_os_caches()
        floor.append(one("SELECT count(*) FROM bronze.lakehouse.raw_nation")["elapsed_s"])
    out["os_cold_floor_s"] = statistics.median(floor)
    print(f"  cold floor (raw_nation after drop_caches): {out['os_cold_floor_s']:.2f}s", flush=True)
    out["trino_cache_dir_bytes"] = int(sh("docker exec trino du -sb /tmp/trino-cache").split()[0])

    if not a.no_spark:
        drop_os_caches()
        r = subprocess.run("docker exec spark spark-submit /opt/jobs/bench_queries.py "
                           f"--prefix {a.schema}.raw_ --runs 3 --cache",
                           shell=True, check=True, capture_output=True, text=True).stdout
        out["spark"] = json.loads(r.strip().splitlines()[-1])
        print("  spark", out["spark"], flush=True)

    path = ROOT / "results" / f"cache_sf{a.sf}.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("saved", path)


if __name__ == "__main__":
    main()
