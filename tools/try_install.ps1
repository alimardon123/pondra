# install.ps1 tried on Windows as `irm … | iex` runs it (in this session), with this build's archive:
# afterwards `pondra` in this terminal is the installed one, and the folder is in the user's Path.
#   powershell -File tools\try_install.ps1 <repository> <folder to install into>
param([string]$root, [string]$dir)
$ErrorActionPreference = 'Stop'
$env:PONDRA_ARCHIVE = Join-Path $root 'dist\pondra-windows-x64.zip'
$zipHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $env:PONDRA_ARCHIVE).Hash.ToLower()

# an archive whose SHA-256 isn't the one given is refused, and nothing is installed
$env:PONDRA_INSTALL = "${dir}-refused"
$env:PONDRA_SHA256 = '0' * 64
$refused = $false
try {
    Get-Content -Raw -LiteralPath (Join-Path $root 'install.ps1') | Invoke-Expression
} catch {
    $refused = $_.Exception.Message -match 'not installed'
    if (-not $refused) { throw }
}
if (-not $refused) { throw 'install.ps1 installed an archive whose SHA-256 was wrong' }
if (Test-Path -LiteralPath (Join-Path $env:PONDRA_INSTALL 'pondra.exe')) { throw 'the refused archive left pondra.exe behind' }

# the same archive with its real SHA-256: installed
$env:PONDRA_SHA256 = $zipHash
$env:PONDRA_INSTALL = $dir
Get-Content -Raw -LiteralPath (Join-Path $root 'install.ps1') | Invoke-Expression
$found = (Get-Command pondra).Source
if ($found -ne (Join-Path $dir 'pondra.exe')) { throw "this terminal's pondra is $found" }
$path = (Get-Item 'HKCU:\Environment').GetValue('Path', '', 'DoNotExpandEnvironmentNames')
if (($path -split ';') -notcontains $dir) { throw "not in the user's Path: $path" }
& pondra --version
Write-Host 'install.ps1 ok'
