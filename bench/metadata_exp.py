import argparse
import json
import pathlib
import statistics
import time

from benchmark import drop_os_caches, sh
from trino_client import query, query_timing, scalar

ROOT = pathlib.Path(__file__).resolve().parent.parent
S = "bronze.bench"
Q = """SELECT orderpriority, count(*) AS cnt, sum(totalprice) AS total
FROM {t} WHERE orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-06-30'
GROUP BY orderpriority ORDER BY orderpriority"""
SESSION = {"bronze.compression_codec": "ZSTD"}


def prefix_bytes(path):
    out = sh("docker exec lakehouse-minio sh -c "
             f"\"mc alias set m http://localhost:9000 minioadmin minioadmin >/dev/null && mc du --json m/{path}\"")
    r = json.loads(out.strip().splitlines()[-1])
    return r["size"], r.get("objects")


def measure(name, tbl):
    t = f"{S}.{tbl}"
    files, data = query(f'SELECT count(*), sum(file_size_in_bytes) FROM {S}."{tbl}$files"')["rows"][0]
    manifests, man_bytes = query(f'SELECT count(*), sum(length) FROM {S}."{tbl}$manifests"')["rows"][0]
    snaps = scalar(f'SELECT count(*) FROM {S}."{tbl}$snapshots"')
    loc = scalar(f'SELECT file_path FROM {S}."{tbl}$files" LIMIT 1').rsplit("/data/", 1)[0]
    meta_bytes, meta_objs = prefix_bytes(loc.replace("s3://", "") + "/metadata")
    sql = Q.format(t=t)
    drop_os_caches()
    cold = query(sql)
    cold_t = query_timing(cold["query_id"])
    warm = [query(sql) for _ in range(5)]
    warm_plan = [query_timing(w["query_id"])["planning_ms"] for w in warm]
    rec = {
        "variant": name, "files": files, "data_bytes": data, "avg_file_kb": round(data / files / 1024, 1),
        "manifests": manifests, "manifest_bytes": man_bytes, "snapshots": snaps,
        "metadata_dir_bytes": meta_bytes, "metadata_dir_objects": meta_objs,
        "cold_s": cold["elapsed_s"], "cold_planning_ms": cold_t["planning_ms"],
        "warm_median_s": statistics.median(w["elapsed_s"] for w in warm),
        "warm_planning_median_ms": statistics.median(warm_plan),
        "warm_cpu_median_s": statistics.median(w["cpu_s"] for w in warm),
    }
    print(f"  {name:<24} files={files:>6} manifests={manifests:>4} snaps={snaps:>3} "
          f"meta={meta_bytes / 2**20:7.2f} MB avg_file={rec['avg_file_kb']:9.1f} KB "
          f"cold={rec['cold_s']:6.2f}s warm={rec['warm_median_s']:6.2f}s "
          f"plan={rec['warm_planning_median_ms']} ms", flush=True)
    return rec


def maintain(tbl):
    t0 = time.perf_counter()
    t = f"{S}.{tbl}"
    query(f"ALTER TABLE {t} EXECUTE optimize", session=SESSION)
    query(f"ALTER TABLE {t} EXECUTE optimize_manifests")
    query(f"ALTER TABLE {t} EXECUTE expire_snapshots(retention_threshold => '0s')")
    query(f"ALTER TABLE {t} EXECUTE remove_orphan_files(retention_threshold => '0s')")
    return round(time.perf_counter() - t0, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--source", default="bronze.lakehouse.raw_", help="префикс таблиц-источников")
    a = ap.parse_args()
    src = f"{a.source}orders"
    out = {"sf": a.sf, "query": Q.format(t="<table>"), "variants": [], "load_s": {}}

    for tbl in ["meta_one_ctas", "meta_many_commits", "meta_tiny_files"]:
        query(f"DROP TABLE IF EXISTS {S}.{tbl}")

    print("== load", flush=True)
    r = query(f"CREATE TABLE {S}.meta_one_ctas WITH (format='PARQUET') AS SELECT * FROM {src}", session=SESSION)
    out["load_s"]["one_ctas"] = r["elapsed_s"]

    months = [m[0] for m in query(f"SELECT DISTINCT date_trunc('month', orderdate) FROM {src} ORDER BY 1")["rows"]]
    query(f"CREATE TABLE {S}.meta_many_commits WITH (format='PARQUET') AS SELECT * FROM {src} WITH NO DATA")
    t0 = time.perf_counter()
    for m in months:
        query(f"INSERT INTO {S}.meta_many_commits SELECT * FROM {src} "
              f"WHERE date_trunc('month', orderdate) = DATE '{m}'", session=SESSION)
    out["load_s"]["many_commits"] = round(time.perf_counter() - t0, 1)
    out["many_commits_inserts"] = len(months)

    r = query(f"CREATE TABLE {S}.meta_tiny_files WITH (format='PARQUET') AS SELECT * FROM {src}",
              session={**SESSION, "bronze.target_max_file_size": "2MB"})
    out["load_s"]["tiny_files"] = r["elapsed_s"]
    print("  load times:", out["load_s"], flush=True)

    print("== measure", flush=True)
    for name, tbl in [("one_ctas", "meta_one_ctas"), ("many_commits", "meta_many_commits"),
                      ("tiny_files", "meta_tiny_files")]:
        out["variants"].append(measure(name, tbl))
    for name, tbl in [("many_commits", "meta_many_commits"), ("tiny_files", "meta_tiny_files")]:
        secs = maintain(tbl)
        rec = measure(f"{name}_maintained", tbl)
        rec["maintenance_s"] = secs
        out["variants"].append(rec)

    for tbl in ["meta_one_ctas", "meta_many_commits", "meta_tiny_files"]:
        query(f"DROP TABLE {S}.{tbl}")
    path = ROOT / "results" / f"metadata_sf{a.sf}.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("saved", path)


if __name__ == "__main__":
    main()
