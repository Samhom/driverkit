param([switch]$WindowsTcpPipe)
$ErrorActionPreference = 'Stop'
$driverKitRoot = $PSScriptRoot
$driverKitArguments = @('-B', '-ntp', '-s', (Join-Path $driverKitRoot '.mvn/settings.xml'), ('-Dmaven.repo.local=' + (Join-Path $driverKitRoot '.m2-cache')))
if ($WindowsTcpPipe) { $driverKitArguments += '-DargLine=-Djdk.net.unixdomain.tmpdir=target/unavailable-unix-domain-directory' }
Push-Location -LiteralPath $driverKitRoot
try {
    & mvn @driverKitArguments clean verify
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & mvn @driverKitArguments com.spotify.fmt:fmt-maven-plugin:2.25:check
    exit $LASTEXITCODE
} finally { Pop-Location }
