import argparse
import json
import struct

import pyarrow.fs as pafs
import pyarrow.orc as po
import pyarrow.parquet as pq

# Parquet: длина футера лежит в хвосте файла перед PAR1. ORC: последний байт файла это длина PostScript.
ORC_KIND = {0: "NONE", 1: "ZLIB", 2: "SNAPPY", 3: "LZO", 4: "LZ4", 5: "ZSTD"}


def varint(buf, i):
    shift = val = 0
    while True:
        b = buf[i]; i += 1
        val |= (b & 0x7F) << shift
        if b < 0x80:
            return val, i
        shift += 7


def orc_postscript(tail):
    ps_len = tail[-1]
    ps = tail[-1 - ps_len:-1]
    i, fields = 0, {}
    while i < len(ps):
        key, i = varint(ps, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            fields[field], i = varint(ps, i)
        elif wire == 2:
            n, i = varint(ps, i); i += n
        else:
            break
    return ps_len, fields.get(1, 0), fields.get(5, 0), ORC_KIND.get(fields.get(2, 0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--format", required=True)
    ap.add_argument("--dir", required=True)
    a = ap.parse_args()
    fs = pafs.S3FileSystem(access_key="minioadmin", secret_key="minioadmin",
                           endpoint_override="minio:9000", scheme="http", region="us-east-1")
    base = a.dir.replace("s3://", "")
    infos = [f for f in fs.get_file_info(pafs.FileSelector(base + "/data", recursive=True))
             if f.type == pafs.FileType.File]
    total = meta = groups = 0
    codecs = set()
    for f in infos:
        total += f.size
        with fs.open_input_file(f.path) as fh:
            tail = fh.read_at(min(f.size, 64 * 1024), max(0, f.size - 64 * 1024))
            if a.format.upper() == "PARQUET":
                (flen,) = struct.unpack("<i", tail[-8:-4])
                meta += flen + 8
                md = pq.ParquetFile(fh).metadata
                groups += md.num_row_groups
                codecs |= {md.row_group(0).column(c).compression for c in range(md.num_columns)}
            else:
                ps_len, footer_len, metadata_len, kind = orc_postscript(tail)
                meta += footer_len + metadata_len + ps_len + 1
                of = po.ORCFile(fh)
                groups += of.nstripes
                codecs.add(kind)
    print(json.dumps({"files": len(infos), "data_bytes": total, "meta_bytes_total": meta,
                      "meta_share_pct": round(100 * meta / total, 4) if total else 0,
                      "row_groups_or_stripes": groups, "codecs": sorted(codecs)}))


if __name__ == "__main__":
    main()
