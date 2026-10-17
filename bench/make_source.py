import argparse
import json
import pathlib

from trino_client import query

ROOT = pathlib.Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    sf = ap.parse_args().sf
    schema = f"bronze.src_sf{sf}"
    query(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    out = {"sf": sf, "schema": schema, "tables": {}}
    for t in ["nation", "customer", "orders", "lineitem"]:
        query(f"DROP TABLE IF EXISTS {schema}.raw_{t}")
        r = query(f"CREATE TABLE {schema}.raw_{t} WITH (format = 'PARQUET') AS "
                  f"SELECT x.*, 'tpch.sf{sf}.{t}' AS _source, "
                  f"CAST(current_timestamp AS timestamp(6) with time zone) AS _loaded_at "
                  f"FROM tpch.sf{sf}.{t} AS x", session={"bronze.compression_codec": "ZSTD"})
        rows, size = query(f'SELECT sum(record_count), sum(file_size_in_bytes) '
                           f'FROM {schema}."raw_{t}$files"')["rows"][0]
        out["tables"][t] = {"rows": rows, "bytes": size, "write_s": r["elapsed_s"]}
        print(f"  {schema}.raw_{t:<9} rows={rows:>12,} {size / 2**20:9.1f} MB {r['elapsed_s']:7.1f}s", flush=True)
    (ROOT / "results" / f"source_sf{sf}.json").write_text(json.dumps(out, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
