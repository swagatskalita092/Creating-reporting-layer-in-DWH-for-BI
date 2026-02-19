# Run solution.sql then qa_checks.sql in Docker PostgreSQL
# Prereq: Docker Desktop running

$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot

# Start Postgres (project dir = workspace for compose)
Push-Location $ProjectRoot
docker compose up -d
Pop-Location

# Wait for DB to be ready
$max = 30
for ($i = 0; $i -lt $max; $i++) {
    $r = docker compose -f "$ProjectRoot\docker-compose.yml" exec -T postgres pg_isready -U dwh -d dwh_bi 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
}
if ($i -eq $max) { Write-Error "Postgres did not become ready"; exit 1 }

Write-Host "Running solution.sql ..." -ForegroundColor Cyan
docker compose -f "$ProjectRoot\docker-compose.yml" exec -T postgres psql -U dwh -d dwh_bi -v ON_ERROR_STOP=1 -f /workspace/solution.sql
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`nRunning qa_checks.sql ..." -ForegroundColor Cyan
docker compose -f "$ProjectRoot\docker-compose.yml" exec -T postgres psql -U dwh -d dwh_bi -v ON_ERROR_STOP=1 -f /workspace/qa_checks.sql
exit $LASTEXITCODE
