// The documentation website (ADR-030): Starlight, published to GitHub Pages by .github/workflows/pages.yml.
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://alimardon123.github.io',
  base: '/pondra',
  integrations: [
    starlight({
      title: 'Pondra',
      description: 'A streamhouse in one binary: SQL, Python and streaming over a lake on your disk or in a bucket.',
      logo: { src: './src/assets/logo.svg' },
      favicon: '/favicon.svg',
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/alimardon123/pondra' }],
      editLink: { baseUrl: 'https://github.com/alimardon123/pondra/edit/main/site/' },
      customCss: ['./src/styles/pondra.css'],
      sidebar: [
        { label: 'Start here', items: [{ autogenerate: { directory: 'start' } }] },
        { label: 'Guides', items: [{ autogenerate: { directory: 'guides' } }] },
        { label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
        { label: 'Concepts', items: [{ autogenerate: { directory: 'concepts' } }] },
      ],
    }),
  ],
});
