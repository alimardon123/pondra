## Install

| You have | Run | Then |
|---|---|---|
| Windows | `irm https://github.com/alimardon123/pondra/releases/latest/download/install.ps1 \| iex` | `pondra` |
| Linux, macOS | `curl -fsSL https://github.com/alimardon123/pondra/releases/latest/download/install.sh \| sh` | `pondra` |
| Python | `pip install pondra` (add `pyarrow` for pandas, Polars and Arrow) | `pondra`, `python -m pondra`, or `import pondra` |
| Node | `npm install -g pondra` | `pondra`, or `npx pondra` with no install |

The files below: the binary alone for each platform (`pondra-<platform>.tar.gz` / `.zip`, what the
installers download), the Python wheels and the npm packages. What changed is in the commits
below and in [`docs/prototype-status.md`](https://github.com/alimardon123/pondra/blob/main/docs/prototype-status.md).
