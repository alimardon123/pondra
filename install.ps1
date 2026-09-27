# Pondra for Windows, with nothing to set up: the latest release's pondra.exe into
# %LOCALAPPDATA%\Programs\pondra, and that folder on your PATH, for this terminal and the next.
#
#   irm https://github.com/alimardon123/pondra/releases/latest/download/install.ps1 | iex
#
# $env:PONDRA_VERSION = "0.22.1" for a given release; $env:PONDRA_INSTALL = "<folder>" for another
# place; $env:PONDRA_ARCHIVE = "<file or URL>" for a build of your own (CI tries this script that way).
& {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'  # (Windows PowerShell downloads slowly while drawing progress)
    $releases = 'https://github.com/alimardon123/pondra/releases'
    $at = if ($env:PONDRA_VERSION) { "download/v$env:PONDRA_VERSION" } else { 'latest/download' }
    $archive = if ($env:PONDRA_ARCHIVE) { $env:PONDRA_ARCHIVE } else { "$releases/$at/pondra-windows-x64.zip" }
    $dir = if ($env:PONDRA_INSTALL) { $env:PONDRA_INSTALL } else { Join-Path $env:LOCALAPPDATA 'Programs\pondra' }

    $tmp = Join-Path ([IO.Path]::GetTempPath()) ('pondra-' + [guid]::NewGuid())
    New-Item -ItemType Directory -Force -Path $tmp, $dir | Out-Null
    try {
        $zip = Join-Path $tmp 'pondra.zip'
        if ($archive -match '^https?://') {
            [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor 'Tls12'
            Invoke-WebRequest -Uri $archive -OutFile $zip -UseBasicParsing
        } else {
            Copy-Item -LiteralPath $archive -Destination $zip
        }
        Expand-Archive -LiteralPath $zip -DestinationPath $tmp -Force
        Copy-Item -LiteralPath (Join-Path $tmp 'pondra.exe') -Destination (Join-Path $dir 'pondra.exe') -Force
    } finally {
        Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }

    # On PATH: yours (kept as stored, %VARIABLES% and all), and this terminal's.
    $key = Get-Item -Path 'HKCU:\Environment'
    $old = $key.GetValue('Path', '', 'DoNotExpandEnvironmentNames')
    if (($old -split ';') -notcontains $dir) {
        $new = (@($old -split ';' | Where-Object { $_ }) + $dir) -join ';'
        Set-ItemProperty -Path 'HKCU:\Environment' -Name 'Path' -Value $new -Type ExpandString
        # (a variable set and removed tells Windows the environment changed: terminals opened next see it)
        [Environment]::SetEnvironmentVariable('PONDRA_INSTALLED', '1', 'User')
        [Environment]::SetEnvironmentVariable('PONDRA_INSTALLED', $null, 'User')
    }
    if (($env:Path -split ';') -notcontains $dir) { $env:Path = "$dir;$env:Path" }

    & (Join-Path $dir 'pondra.exe') --version
    Write-Host "Installed in $dir, on your PATH: try ``pondra`` (a SQL shell on .\lake)"
}
