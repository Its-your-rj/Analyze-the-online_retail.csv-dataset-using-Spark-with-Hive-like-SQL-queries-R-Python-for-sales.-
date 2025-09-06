cd E:\Sales_Analyzer

# (OPTIONAL) set correct Python for Spark in this shell
$env:PYSPARK_PYTHON = "C:\Users\user\AppData\Local\Programs\Python\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON

# make sure logs folder exists
New-Item -Path logs -ItemType Directory -Force | Out-Null

Write-Host "1) Ingest + preview"
python notebooks/01_ingest_and_preview.py > logs/01_ingest.log 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "01_ingest failed. Check logs/01_ingest.log"; exit $LASTEXITCODE }

Write-Host "2) Clean -> parquet"
python notebooks/02_clean.py > logs/02_clean.log 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "02_clean failed. Check logs/02_clean.log"; exit $LASTEXITCODE }

Write-Host "3) Queries -> CSV exports"
python notebooks/03_queries.py > logs/03_queries.log 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "03_queries failed. Check logs/03_queries.log"; exit $LASTEXITCODE }

Write-Host "4) Plots -> PNGs"
python notebooks/04_plots.py > logs/04_plots.log 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "04_plots failed. Check logs/04_plots.log"; exit $LASTEXITCODE }

Write-Host "5) R Visualizations"
Rscript notebooks/05_visualize.R > logs/05_visualize.log 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "05_visualize failed. Check logs/05_visualize.log"; exit $LASTEXITCODE }

Write-Host "✅ Pipeline finished - check logs/, python_reports/, docs/"
