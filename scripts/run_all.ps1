param([int]$PipelineSF = 50, [int]$BenchSF = 10)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PYTHONIOENCODING = "utf-8"
$log = "results\run_all.log"

function Stage($text) {
    $line = "##### $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $text"
    Write-Host $line -ForegroundColor Cyan
    Add-Content -Path $log -Value $line -Encoding utf8
}

function Step([string]$Guard, [string[]]$PyArgs) {
    if ($Guard -and (Test-Path $Guard)) { Write-Host "skip: $Guard уже есть"; return }
    Write-Host "python $($PyArgs -join ' ')  (ход смотри: Get-Content $log -Wait -Tail 20 -Encoding UTF8)"
    # Через cmd, чтобы байты UTF-8 попали в лог без перекодировки.
    cmd /c "python $($PyArgs -join ' ') >> $log 2>&1"
    if ($LASTEXITCODE -ne 0) { throw "python $($PyArgs -join ' ') завершился с кодом $LASTEXITCODE, см. $log" }
}

Stage "sf1: матрица форматов/кодеков (эффект малого объёма)"
Step "results\pipeline_sf1.json" @("bench/pipeline.py", "--sf", "1", "--bronze-codec", "ZSTD")
Step $null @("bench/benchmark.py", "--sf", "1")
Step "results\cache_sf1.json" @("bench/cache_exp.py", "--sf", "1")

Stage "sf${PipelineSF}: медальон Bronze -> Silver -> Gold + сверка"
Step "results\pipeline_sf$PipelineSF.json" @("bench/pipeline.py", "--sf", "$PipelineSF", "--bronze-codec", "ZSTD")

Stage "sf${BenchSF}: источник бенчмарка bronze.src_sf$BenchSF"
Step "results\source_sf$BenchSF.json" @("bench/make_source.py", "--sf", "$BenchSF")
$src = "bronze.src_sf$BenchSF.raw_"

Stage "sf${BenchSF}: матрица форматов/кодеков"
Step $null @("bench/benchmark.py", "--sf", "$BenchSF", "--source", $src)

Stage "sf${BenchSF}: опыт с метаданными"
Step "results\metadata_sf$BenchSF.json" @("bench/metadata_exp.py", "--sf", "$BenchSF", "--source", $src)

Stage "sf${BenchSF}: опыт с кэшированием"
Step "results\cache_sf$BenchSF.json" @("bench/cache_exp.py", "--sf", "$BenchSF", "--schema", "src_sf$BenchSF")

Stage "DONE"
