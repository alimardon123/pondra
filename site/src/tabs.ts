// The site's three tabs (Docs, SQL, APIs): each is a top-level group of the sidebar in
// astro.config.mjs. A page shows only its own tab's sidebar, its previous and next links stay inside
// that tab, and the header (components/Header.astro) draws every tab, the page's own marked.
import { defineRouteMiddleware } from '@astrojs/starlight/route-data';

type Entry = { type: 'link'; href: string; label: string; isCurrent: boolean } | { type: 'group'; label: string; entries: Entry[] };

const links = (entries: Entry[]): Extract<Entry, { type: 'link' }>[] =>
  entries.flatMap((e) => (e.type === 'link' ? [e] : links(e.entries)));

export const onRequest = defineRouteMiddleware((context) => {
  const route = context.locals.starlightRoute;
  const groups = (route.sidebar as Entry[]).filter((e): e is Extract<Entry, { type: 'group' }> => e.type === 'group');
  const here = groups.find((g) => links(g.entries).some((l) => l.isCurrent));
  (context.locals as any).tabs = groups.map((g) => ({ label: g.label, href: links(g.entries)[0]?.href, current: g === here }));
  if (!here) return;
  route.sidebar = here.entries as typeof route.sidebar;
  const own = links(here.entries);
  const at = own.findIndex((l) => l.isCurrent);
  const step = (l?: { href: string; label: string }) => l && { href: l.href, label: l.label, isCurrent: false, type: 'link' as const, attrs: {}, badge: undefined };
  if (route.entry.data.prev === undefined) route.pagination.prev = step(own[at - 1]) as typeof route.pagination.prev;
  if (route.entry.data.next === undefined) route.pagination.next = step(own[at + 1]) as typeof route.pagination.next;
});
