#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONIOENCODING=utf-8
PSF=${PIPELINE_SF:-50}
BSF=${BENCH_SF:-10}
SRC="bronze.src_sf$BSF.raw_"

echo "##### $(date '+%F %T') sf1: матрица форматов/кодеков (эффект малого объёма)"
[ -f results/pipeline_sf1.json ] || python bench/pipeline.py --sf 1 --bronze-codec ZSTD
python bench/benchmark.py --sf 1
[ -f results/cache_sf1.json ] || python bench/cache_exp.py --sf 1

echo "##### $(date '+%F %T') sf$PSF: медальон Bronze -> Silver -> Gold + сверка"
[ -f results/pipeline_sf$PSF.json ] || python bench/pipeline.py --sf "$PSF" --bronze-codec ZSTD

echo "##### $(date '+%F %T') sf$BSF: источник бенчмарка"
[ -f results/source_sf$BSF.json ] || python bench/make_source.py --sf "$BSF"

echo "##### $(date '+%F %T') sf$BSF: матрица форматов/кодеков"
python bench/benchmark.py --sf "$BSF" --source "$SRC"

echo "##### $(date '+%F %T') sf$BSF: опыт с метаданными"
[ -f results/metadata_sf$BSF.json ] || python bench/metadata_exp.py --sf "$BSF" --source "$SRC"

echo "##### $(date '+%F %T') sf$BSF: опыт с кэшированием"
[ -f results/cache_sf$BSF.json ] || python bench/cache_exp.py --sf "$BSF" --schema "src_sf$BSF"

echo "##### $(date '+%F %T') DONE"
