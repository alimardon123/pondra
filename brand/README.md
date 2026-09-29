# Pondra's brand

Everything that shows Pondra's logo or colours reads them from here, so changing one of these files
changes every place:

| File | What it is | Where it goes |
|---|---|---|
| `mark.svg` | The mark: one colour (`color`, drawn with `currentColor`), lighter on a dark background | The console's header and browser-tab icon (built into the binary, `src/console.rs`); the docs site's header and favicon (`site/astro.config.mjs`); the READMEs and package pages, through the docs site |
| `colors.css` | The mark's colours and the accent, on light and dark | The docs site's accent (`site/src/styles/pondra.css`) and the console's (`src/console.html`) |

To change the logo, replace `mark.svg`: keep `class="pondra-mark"` and `color="…"` on its root, draw
with `currentColor`, and keep the one `<style>` that lightens it on dark backgrounds. Then run
`python3 tools/brand_check.py`, which CI runs too: it fails if a copy of the mark is kept anywhere
else, or if the console, the site's header or its favicon don't show this one.
