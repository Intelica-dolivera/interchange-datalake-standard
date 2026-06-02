# sync-glue.ps1
# Sincroniza codigo fuente y configuracion de Glue Jobs desde AWS al repo local.
# Solo actua sobre glue/scripts/ - no toca los JSONs de crawlers/databases en glue/
#
# Prerequisitos:
#   aws sso login --profile itx-dev
#   $env:AWS_PROFILE = "itx-dev"
#
# Uso:
#   .\scripts\sync-glue.ps1                    # sincroniza todos
#   .\scripts\sync-glue.ps1 -Group vi          # solo Visa
#   .\scripts\sync-glue.ps1 -Group mc          # solo Mastercard
#   .\scripts\sync-glue.ps1 -Job vi-calculate  # uno especifico

param(
    [ValidateSet("all","vi","mc")]
    [string]$Group = "all",
    [string]$Job = ""
)

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Prefix   = "itl-0004-itx-dev-intchg-02-glue"

$AllJobs = [ordered]@{
    "vi-calculate"   = @{ Group="vi"; Dir="glue\scripts\visa\calculate" }
    "vi-interchange" = @{ Group="vi"; Dir="glue\scripts\visa\interchange" }
    "mc-calculate"   = @{ Group="mc"; Dir="glue\scripts\mastercard\calculate" }
    "mc-interchange" = @{ Group="mc"; Dir="glue\scripts\mastercard\interchange" }
}

if ($Job -ne "" -and -not $AllJobs.Contains($Job)) {
    Write-Host "ERROR: Job '$Job' no reconocido. Opciones:" -ForegroundColor Red
    $AllJobs.Keys | ForEach-Object { Write-Host "  $_" }
    exit 1
}

$ToSync = [ordered]@{}
foreach ($Suffix in $AllJobs.Keys) {
    $Meta = $AllJobs[$Suffix]
    if ($Job -ne "") {
        if ($Suffix -eq $Job) { $ToSync[$Suffix] = $Meta }
    } elseif ($Group -eq "all" -or $Meta.Group -eq $Group) {
        $ToSync[$Suffix] = $Meta
    }
}

if ($ToSync.Count -eq 0) {
    Write-Host "No hay Jobs para sincronizar con los parametros indicados." -ForegroundColor Yellow
    exit 0
}

Write-Host "Glue Jobs a sincronizar: $($ToSync.Count)" -ForegroundColor White
$ToSync.Keys | ForEach-Object { Write-Host "  $_" }
Write-Host ""

$Results = New-Object System.Collections.ArrayList

foreach ($Suffix in $ToSync.Keys) {

    $Meta     = $ToSync[$Suffix]
    $JobName  = "$Prefix-$Suffix"
    $LocalDir = Join-Path $RepoRoot $Meta.Dir

    Write-Host "[$Suffix]" -ForegroundColor Cyan

    # 1. Configuracion
    $ConfigJson = aws glue get-job --job-name $JobName --output json 2>$null

    if (-not $ConfigJson) {
        Write-Host "  SKIP - Job no encontrado en AWS" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "NOT FOUND" })
        Write-Host ""
        continue
    }

    $Config = $ConfigJson | ConvertFrom-Json

    if (-not (Test-Path $LocalDir)) {
        New-Item -ItemType Directory -Path $LocalDir -Force | Out-Null
    }

    $ConfigJson | Out-File -FilePath (Join-Path $LocalDir "config.json") -Encoding utf8 -Force
    Write-Host "  config.json actualizado"

    # 2. DefaultArguments -> args.json
    if ($Config.Job.DefaultArguments) {
        $ArgCount = ($Config.Job.DefaultArguments.PSObject.Properties | Measure-Object).Count
        @{ DefaultArguments = $Config.Job.DefaultArguments } | ConvertTo-Json -Depth 5 | Out-File -FilePath (Join-Path $LocalDir "args.json") -Encoding utf8 -Force
        Write-Host "  args.json actualizado ($ArgCount args)"
    } else {
        @{ DefaultArguments = @{} } | ConvertTo-Json | Out-File -FilePath (Join-Path $LocalDir "args.json") -Encoding utf8 -Force
        Write-Host "  args.json creado (sin args)"
    }

    # 3. Script PySpark desde S3
    $ScriptS3Path = $Config.Job.Command.ScriptLocation
    if (-not $ScriptS3Path) {
        Write-Host "  WARN - ScriptLocation no encontrado en la config" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "CONFIG OK / SCRIPT FAILED" })
        Write-Host ""
        continue
    }

    $ScriptName  = Split-Path $ScriptS3Path -Leaf
    $ScriptLocal = Join-Path $LocalDir $ScriptName

    Write-Host "  Descargando script desde S3..."
    aws s3 cp $ScriptS3Path $ScriptLocal 2>$null

    if (-not (Test-Path $ScriptLocal)) {
        Write-Host "  WARN - No se pudo descargar el script" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "CONFIG OK / SCRIPT FAILED" })
        Write-Host ""
        continue
    }

    $SizeKB = [math]::Round((Get-Item $ScriptLocal).Length / 1KB, 1)
    Write-Host "  $ScriptName descargado ($SizeKB KB)" -ForegroundColor Green
    $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "OK" })
    Write-Host ""
}

Write-Host "========== Resumen ==========" -ForegroundColor White
$Results | Format-Table -AutoSize

$ok   = ($Results | Where-Object { $_.Status -eq "OK" }).Count
$skip = ($Results | Where-Object { $_.Status -ne "OK" }).Count
if ($skip -eq 0) {
    Write-Host "Completado: $ok OK" -ForegroundColor Green
} else {
    Write-Host "Completado: $ok OK, $skip con advertencias/errores" -ForegroundColor Yellow
}
