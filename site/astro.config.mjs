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
fs.mkdirSync(new URL('./public/', import.meta.url), { recursive: true }); // (a clean checkout has no public/: the favicon is all it holds)
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
      expressiveCode: { themes: ['github-dark-default', 'github-light-default'], defaultProps: { wrap: true, hangingIndent: 2, overridesByLang: { 'text,txt,plaintext,ansi': { wrap: false } } }, styleOverrides: { borderRadius: '0.6rem', codeFontSize: '0.85rem', frames: { shadowColor: 'transparent' } } },
      components: { Header: './src/components/Header.astro', Sidebar: './src/components/Sidebar.astro' },
      routeMiddleware: './src/tabs.ts',
      // The three tabs (src/tabs.ts): each top-level group is one, with its own sidebar. site/PAGES.md
      // says what each page covers; a new page goes in the group of its kind (site/STYLE.md).
      sidebar: [
        { label: 'Docs', items: [
          { label: 'Get started', items: ['start/install', 'start/first-lake', 'start/features', 'start/tour', 'start/shell', 'start/console'] },
          { label: 'Data', collapsed: true, items: ['guides/tables', 'guides/load-data', 'guides/query', 'guides/change-rows', 'guides/past', 'guides/files-and-other-lakes'] },
          { label: 'Streams', collapsed: true, items: ['guides/views', 'guides/windows', 'guides/stream-joins', 'guides/flows', 'guides/kafka'] },
          { label: 'Python and code', collapsed: true, items: ['guides/sql-and-python', 'guides/dataframes', 'guides/pyspark', 'guides/functions-and-procedures', 'guides/scripts', 'guides/workspace', 'guides/tasks', 'guides/notebooks'] },
          { label: 'Teams', collapsed: true, items: ['guides/environments', 'guides/projects', 'guides/git', 'guides/sharing', 'guides/security'] },
          { label: 'Connect', collapsed: true, items: ['guides/agents', 'guides/dbt-and-bi', 'guides/other-engines', 'guides/ai-and-files'] },
          { label: 'Operate', collapsed: true, items: ['guides/clusters', 'guides/object-storage', 'guides/server', 'guides/deploy', 'guides/upgrades', 'guides/observability'] },
          { label: 'How it works', collapsed: true, items: ['concepts/how-it-works', 'concepts/lake-format', 'concepts/keyed-tables', 'concepts/freshness-and-guarantees', 'concepts/failures', 'concepts/performance', 'concepts/compared', 'concepts/design-notes'] },
        ] },
        { label: 'SQL', items: [
          'sql',
          { label: 'Language', items: ['sql/statements', 'sql/functions', 'sql/table-options'] },
          { label: 'Catalog', items: ['sql/system-tables', 'sql/glossary'] },
        ] },
        { label: 'APIs', items: [
          'reference',
          { label: 'Clients', items: ['reference/python', 'reference/frames', 'reference/pyspark', 'reference/javascript', 'reference/names'] },
          { label: 'Command line', items: ['reference/cli'] },
          { label: 'Protocols', items: ['reference/http', 'reference/postgres', 'reference/flight', 'reference/kafka-protocol', 'reference/iceberg-rest', 'reference/mcp'] },
          { label: 'Configuration', items: ['reference/environment'] },
        ] },
      ],
    }),
  ],
  // Pages that moved when the site took its three tabs (2026-10): old links still land.
  redirects: {
    '/reference/sql': '/pondra/sql/statements/',
    '/reference/functions': '/pondra/sql/functions/',
    '/reference/table-options': '/pondra/sql/table-options/',
    '/reference/system-columns': '/pondra/sql/system-tables/',
    '/reference/glossary': '/pondra/sql/glossary/',
    '/guides': '/pondra/start/features/',
    '/guides/streaming': '/pondra/guides/views/',
  },
  vite: { server: { fs: { allow: ['..'] } } }, // (brand/, beside site/)
});
