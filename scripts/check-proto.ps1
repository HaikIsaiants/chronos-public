$ErrorActionPreference = "Stop"
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$temporary = [IO.Path]::GetFullPath((Join-Path $tempRoot ("chronos-proto-" + [guid]::NewGuid().ToString("N"))))
if (-not $temporary.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "invalid temporary path"
}

try {
    & (Join-Path $PSScriptRoot "generate-proto.ps1") -OutputRoot $temporary
    $manifest = Get-Content (Join-Path $root "tools\protobuf.json") -Raw | ConvertFrom-Json
    foreach ($file in $manifest.files) {
        $expected = (Get-FileHash (Join-Path $root $file) -Algorithm SHA256).Hash
        $actual = (Get-FileHash (Join-Path $temporary $file) -Algorithm SHA256).Hash
        if ($expected -ne $actual) {
            throw "$file differs from generated output"
        }
    }
} finally {
    if ([IO.Directory]::Exists($temporary)) {
        [IO.Directory]::Delete($temporary, $true)
    }
}
