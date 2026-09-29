// The documentation website (ADR-030): Starlight, published to GitHub Pages by .github/workflows/pages.yml.
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import fs from 'node:fs';

// Pondra's mark, from its one source (brand/: ADR-032). The header gets it in the site's light and
// dark colours (it follows the site's switch), the favicon as it is (it follows the system's).
// Both are made here, on every build: nothing in site/ is a copy to keep in step.
const brand = (f) => new URL(`../brand/${f}`, import.meta.url);
const mark = fs.readFileSync(brand('mark.svg'), 'utf8');
const colors = fs.readFileSync(brand('colors.css'), 'utf8');
const colour = (name) => colors.match(new RegExp(`--${name}:\\s*(#[0-9a-fA-F]{6})`))[1];
const made = (f) => new URL(`./src/assets/brand/${f}`, import.meta.url);
fs.mkdirSync(made(''), { recursive: true });
for (const [f, c] of [['mark-light.svg', colour('pondra-mark')], ['mark-dark.svg', colour('pondra-mark-dark')]]) {
  fs.writeFileSync(made(f), mark.replace(/<style>[\s\S]*?<\/style>\n?/, '').replace(/ color="#[0-9a-fA-F]{6}"/, ` color="${c}"`));
}
fs.writeFileSync(new URL('./public/favicon.svg', import.meta.url), mark);

export default defineConfig({
  site: 'https://alimardon123.github.io',
  base: '/pondra',
  integrations: [
    starlight({
      title: 'Pondra',
      description: 'A streamhouse in one binary: SQL, Python and streaming over a lake on your disk or in a bucket.',
      logo: { light: './src/assets/brand/mark-light.svg', dark: './src/assets/brand/mark-dark.svg', alt: 'Pondra' },
      favicon: '/favicon.svg',
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/alimardon123/pondra' }],
      editLink: { baseUrl: 'https://github.com/alimardon123/pondra/edit/main/site/' },
      customCss: ['../brand/colors.css', './src/styles/pondra.css'],
      sidebar: [
        { label: 'Start here', items: [{ autogenerate: { directory: 'start' } }] },
        { label: 'Guides', items: [{ autogenerate: { directory: 'guides' } }] },
        { label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
        { label: 'Concepts', items: [{ autogenerate: { directory: 'concepts' } }] },
      ],
    }),
  ],
  vite: { server: { fs: { allow: ['..'] } } }, // (brand/, beside site/)
});
