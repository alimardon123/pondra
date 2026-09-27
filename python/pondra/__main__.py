"""`python -m pondra …`: the pondra binary pip installed, run with these arguments from wherever
pip put it, so it works when that folder isn't on PATH (pip's user installs on Windows and macOS).
`python -m pondra --add-to-path` puts the folder on your PATH, for the terminals you open next."""
import os
import shutil
import signal
import subprocess
import sys

from .client import binary


def add_to_path(folder):
    """The folder on this user's PATH: in the registry on Windows, in the shell's startup file elsewhere."""
    if os.name == "nt":
        import ctypes
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment", 0, winreg.KEY_READ | winreg.KEY_WRITE) as k:
            try:
                old, kind = winreg.QueryValueEx(k, "Path")  # (as stored: %VARIABLES% stay as they are)
            except FileNotFoundError:
                old, kind = "", winreg.REG_EXPAND_SZ
            if folder.lower() in (p.lower().rstrip("\\") for p in old.split(";")):
                return f"{folder} is on your PATH already"
            winreg.SetValueEx(k, "Path", 0, kind, ";".join(p for p in (old.rstrip(";"), folder) if p))
        from ctypes import wintypes as w
        tell = ctypes.windll.user32.SendMessageTimeoutW  # (so terminals opened from now on see it)
        tell.argtypes = [w.HWND, w.UINT, w.WPARAM, w.LPCWSTR, w.UINT, w.UINT, ctypes.c_void_p]
        tell(0xFFFF, 0x1A, 0, "Environment", 2, 5000, None)  # (HWND_BROADCAST, WM_SETTINGCHANGE)
        return f"{folder} is on your PATH now: open a new terminal and run `pondra`"
    shell = os.path.basename(os.environ.get("SHELL", ""))
    rc = os.path.expanduser({"zsh": "~/.zshrc", "bash": "~/.bash_profile" if sys.platform == "darwin" else "~/.bashrc"}.get(shell, "~/.profile"))
    line = f'export PATH="{folder}:$PATH"  # pondra'
    if os.path.exists(rc) and line in open(rc, encoding="utf-8").read():
        return f"{folder} is on your PATH already ({rc})"
    with open(rc, "a", encoding="utf-8") as f:
        f.write(f"\n{line}\n")
    return f"{folder} is on your PATH now ({rc}): open a new terminal and run `pondra`"


def main():
    exe, args = binary(), sys.argv[1:]
    if args == ["--add-to-path"]:
        print(add_to_path(os.path.dirname(exe)))
        return 0
    if sys.stderr.isatty() and not shutil.which("pondra"):
        print(f"(pondra is in {os.path.dirname(exe)}, which isn't on your PATH: `python -m pondra --add-to-path` puts it there)", file=sys.stderr)
    if os.name != "nt":
        os.execv(exe, [exe, *args])  # (this process becomes pondra)
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # (Ctrl+C is pondra's to handle)
    return subprocess.call([exe, *args])


if __name__ == "__main__":
    sys.exit(main())
