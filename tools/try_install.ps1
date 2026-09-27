# install.ps1 tried on Windows as `irm … | iex` runs it (in this session), with this build's archive:
# afterwards `pondra` in this terminal is the installed one, and the folder is in the user's Path.
#   powershell -File tools\try_install.ps1 <repository> <folder to install into>
param([string]$root, [string]$dir)
$ErrorActionPreference = 'Stop'
$env:PONDRA_ARCHIVE = Join-Path $root 'dist\pondra-windows-x64.zip'
$env:PONDRA_INSTALL = $dir
Get-Content -Raw -LiteralPath (Join-Path $root 'install.ps1') | Invoke-Expression
$found = (Get-Command pondra).Source
if ($found -ne (Join-Path $dir 'pondra.exe')) { throw "this terminal's pondra is $found" }
$path = (Get-Item 'HKCU:\Environment').GetValue('Path', '', 'DoNotExpandEnvironmentNames')
if (($path -split ';') -notcontains $dir) { throw "not in the user's Path: $path" }
& pondra --version
Write-Host 'install.ps1 ok'
