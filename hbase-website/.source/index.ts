/// <reference types="vite/client" />
import { fromConfig } from 'fumadocs-mdx/runtime/vite';
import type * as Config from '../source.config';

export const create = fromConfig<typeof Config>();

export const docs = {
  doc: create.doc("docs", "app/pages/_docs/docs/_mdx", import.meta.glob(["./**/*.mdx"], {
    "query": {
      "collection": "docs"
    },
    "base": "./../app/pages/_docs/docs/_mdx"
  })),
  meta: create.meta("docs", "app/pages/_docs/docs/_mdx", import.meta.glob(["./**/*.{json,yaml}"], {
    "import": "default",
    "base": "./../app/pages/_docs/docs/_mdx",
    "query": {
      "collection": "docs"
    }
  }))
};