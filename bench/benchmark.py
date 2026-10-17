import argparse
import json
import pathlib
import statistics
import subprocess
import time

from queries import QUERIES
from trino_client import TrinoError, query, scalar

ROOT = pathlib.Path(__file__).resolve().parent.parent
FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]
TABLES = ["customer", "orders", "lineitem"]
WARM_RUNS = 5
# При 12 потоках на sf50 не хватает памяти запроса, а на HDD 8 writer'ов медленнее 4.
WRITE_SESSION = {"max_drivers_per_task": 6, "task_max_writer_count": 4}
SOURCE = "bronze.lakehouse.raw_"


def sh(cmd):
    """Список аргументов запускается без shell, чтобы не упереться в кавычки cmd.exe на Windows."""
    return subprocess.run(cmd, shell=isinstance(cmd, str), check=True, capture_output=True, text=True).stdout


def drop_os_caches():
    sh(["docker", "run", "--rm", "--privileged", "alpine:3.20",
        "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])


def bucket_bytes(prefix="bronze"):
    out = sh("docker exec lakehouse-minio sh -c "
             f"\"mc alias set m http://localhost:9000 minioadmin minioadmin >/dev/null && mc du --json m/{prefix}\"")
    return json.loads(out.strip().splitlines()[-1])["size"]


def cell_prefix(fmt, codec):
    return f"bronze.bench.{fmt.lower()}_{codec.lower()}_"


def write_cell(fmt, codec):
    p = cell_prefix(fmt, codec)
    res = {}
    for t in TABLES:
        query(f"DROP TABLE IF EXISTS {p}{t}")
        r = query(f"CREATE TABLE {p}{t} WITH (format = '{fmt}') AS "
                  f"SELECT * FROM {SOURCE}{t}",
                  session={"bronze.compression_codec": codec, **WRITE_SESSION})
        files, size, recs = query(
            f"SELECT count(*), sum(file_size_in_bytes), sum(record_count) "
            f"FROM bronze.bench.\"{fmt.lower()}_{codec.lower()}_{t}$files\"")["rows"][0]
        res[t] = {"write_s": r["elapsed_s"], "write_cpu_s": r["cpu_s"], "files": files,
                  "bytes": size, "rows": recs}
        print(f"    write {t:<9} {r['elapsed_s']:7.1f}s cpu={r['cpu_s']:8.1f}s "
              f"{size / 2**20:9.1f} MB files={files}", flush=True)
    return res


def time_queries(fmt, codec):
    p = cell_prefix(fmt, codec)
    res = {}
    for name, sql in QUERIES.items():
        sql = sql.format(p=p)
        drop_os_caches()
        cold = query(sql)
        warm = [query(sql) for _ in range(WARM_RUNS)]
        el = [w["elapsed_s"] for w in warm]
        res[name] = {
            "cold_s": cold["elapsed_s"], "cold_cpu_s": cold["cpu_s"],
            "warm_median_s": statistics.median(el), "warm_min_s": min(el), "warm_max_s": max(el),
            "warm_cpu_median_s": statistics.median(w["cpu_s"] for w in warm),
            "physical_input_bytes": warm[0]["physical_input_bytes"],
            "peak_memory_bytes": warm[0]["peak_memory_bytes"],
            "result_rows": len(warm[0]["rows"]),
            "result_head": warm[0]["rows"][:1],
        }
        print(f"    trino {name:<10} cold={cold['elapsed_s']:6.2f}s warm_med={res[name]['warm_median_s']:6.2f}s "
              f"read={warm[0]['physical_input_bytes'] / 2**20:8.1f} MB", flush=True)
    return res


def spark_queries(fmt, codec, attempts=2):
    """Spark в local-режиме иногда падает: повторяем, затем записываем ошибку."""
    cmd = (f"docker exec spark spark-submit --driver-java-options -Dlog4j.rootCategory=ERROR "
           f"/opt/jobs/bench_queries.py --prefix bench.{fmt.lower()}_{codec.lower()}_ --runs 3")
    for attempt in range(1, attempts + 1):
        drop_os_caches()
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if p.returncode == 0:
            break
        err = [x for x in p.stderr.splitlines() if "Exception" in x or "Error" in x][-5:]
        print(f"    spark: попытка {attempt} упала: {' | '.join(err)[:300]}", flush=True)
    else:
        return {"error": err}
    res = json.loads(p.stdout.strip().splitlines()[-1])
    res["attempts"] = attempt
    for name, r in res.items():
        if name == "attempts":
            continue
        print(f"    spark {name:<10} cold={r['cold_s']:6.2f}s warm_med={r['warm_median_s']:6.2f}s", flush=True)
    return res


def footer_metadata(fmt, codec):
    loc = scalar(f"SELECT file_path FROM bronze.bench.\"{fmt.lower()}_{codec.lower()}_lineitem$files\" LIMIT 1")
    tbl_dir = loc.rsplit("/data/", 1)[0]
    out = sh(f"docker compose -f {ROOT / 'docker-compose.yml'} run --rm -T tools "
             f"python /opt/bench/file_meta.py --format {fmt} --dir {tbl_dir}")
    return json.loads(out.strip().splitlines()[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--cells", help="например parquet:zstd,orc:gzip (по умолчанию все)")
    ap.add_argument("--no-spark", action="store_true")
    ap.add_argument("--keep", action="store_true", help="не удалять таблицы ячейки")
    ap.add_argument("--source", default="bronze.lakehouse.raw_", help="префикс таблиц-источников")
    a = ap.parse_args()
    global SOURCE
    SOURCE = a.source

    cells = [(f, c) for f in FORMATS for c in CODECS]
    if a.cells:
        want = {tuple(x.upper().split(":")) for x in a.cells.split(",")}
        cells = [x for x in cells if x in want]
    path = ROOT / "results" / f"bench_sf{a.sf}.jsonl"
    done = set()
    if path.exists():
        done = {(r["format"], r["codec"]) for r in map(json.loads, path.read_text().splitlines())}

    for fmt, codec in cells:
        if (fmt, codec) in done:
            print(f"skip {fmt}/{codec} (уже есть)")
            continue
        print(f"== {fmt} / {codec}", flush=True)
        t0 = time.time()
        rec = {"sf": a.sf, "format": fmt, "codec": codec, "ts": time.strftime("%Y-%m-%d %H:%M:%S")}
        try:
            rec["write"] = write_cell(fmt, codec)
        except TrinoError as e:
            if "NOT_SUPPORTED" not in str(e):
                raise
            print(f"    НЕ ПОДДЕРЖИВАЕТСЯ: {str(e).splitlines()[0]}", flush=True)
            for t in TABLES:
                query(f"DROP TABLE IF EXISTS {cell_prefix(fmt, codec)}{t}")
            rec["unsupported"] = str(e).splitlines()[0]
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            continue
        rec["trino"] = time_queries(fmt, codec)
        if not a.no_spark:
            rec["spark"] = spark_queries(fmt, codec)
        rec["file_meta"] = footer_metadata(fmt, codec)
        print(f"    meta: codec={rec['file_meta'].get('codecs')} "
              f"footer={rec['file_meta'].get('meta_bytes_total', 0) / 2**10:.0f} KB", flush=True)
        if not a.keep:
            for t in TABLES:
                query(f"DROP TABLE {cell_prefix(fmt, codec)}{t}")
        rec["cell_wall_s"] = round(time.time() - t0, 1)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    print("bronze bucket after run:", round(bucket_bytes() / 2**30, 2), "GiB")


if __name__ == "__main__":
    main()
