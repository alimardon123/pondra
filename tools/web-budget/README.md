# The page's download budget

`tools/web_check.mjs --sizes` fails when the page's module, at brotli 11, is over the sum of the first lines of this
folder's `.txt` files. A pull request that moves the line adds a file of its own and never edits another's, so two in the
merge train never touch the same line.

- Name it `YYYY-MM-DD-what.txt`. Its first line is a signed number of bytes (`+1200`, `-4000`; `_` or `,` between digits is
  fine); below it, what the bytes buy, measured on CI against main's module.
- Keep at least 3 KB of room above main's module: a change of layout alone moves brotli by 1-2 KB (once by 62 KB, 11-09).
- The line moves down only with cuts measured in the module's own wasm bytes.
