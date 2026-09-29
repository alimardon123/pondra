import { defineCollection, z } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

// `setup`: SQL that tools/docs_check.py runs before a page's examples (it isn't shown).
export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema({ extend: z.object({ setup: z.string().optional() }) }) }),
};
