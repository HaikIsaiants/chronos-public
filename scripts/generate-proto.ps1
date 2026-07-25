param([string]$OutputRoot)

$ErrorActionPreference = "Stop"
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$manifest = Get-Content (Join-Path $root "tools\protobuf.json") -Raw | ConvertFrom-Json
$protocVersion = (& protoc --version).Trim()
$goPluginVersion = (& protoc-gen-go --version).Trim()
$grpcPluginVersion = (& protoc-gen-go-grpc --version).Trim()
# $goVersion = (& go version).Trim()

if ($protocVersion -ne "libprotoc $($manifest.protoc)") {
    throw "protoc $($manifest.protoc) is required"
}
if ($goPluginVersion -notmatch "^protoc-gen-go(?:\.exe)? v$([regex]::Escape($manifest.protoc_gen_go))$") {
    throw "protoc-gen-go $($manifest.protoc_gen_go) is required"
}
if ($grpcPluginVersion -ne "protoc-gen-go-grpc $($manifest.protoc_gen_go_grpc)") {
    throw "protoc-gen-go-grpc $($manifest.protoc_gen_go_grpc) is required"
}
# if ($goVersion -notmatch " go$([regex]::Escape($manifest.go)) ") {
#     throw "Go $($manifest.go) is required"
# }
$output = $root
if ($OutputRoot) {
    $output = [IO.Path]::GetFullPath($OutputRoot)
    [IO.Directory]::CreateDirectory($output) | Out-Null
}

& protoc "--proto_path=$root" "--go_out=$output" "--go_opt=paths=source_relative" "--go-grpc_out=$output" "--go-grpc_opt=paths=source_relative" "api/chronos/v1/chronos.proto"
if ($LASTEXITCODE -ne 0) {
    throw "protoc failed"
}
