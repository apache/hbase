//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

import {
  type AnyOrama,
  create,
  load,
  type Orama,
  type SearchParams,
  search as searchOrama,
  getByID
} from "@orama/orama";
import {
  type AdvancedDocument,
  type advancedSchema,
  type SimpleDocument,
  type simpleSchema
} from "./create-db";
import { createContentHighlighter, type SortedResult } from "fumadocs-core/search";
import type { ExportedData } from "fumadocs-core/search/server";
import { removeUndefined } from "./utils";

export interface StaticOptions {
  /**
   * Where to download exported search indexes (URL)
   *
   * @defaultValue '/api/search'
   */
  from?: string;

  initOrama?: (locale?: string) => AnyOrama | Promise<AnyOrama>;

  /**
   * Filter results with specific tag(s).
   */
  tag?: string | string[];

  /**
   * Filter by locale (unsupported at the moment)
   */
  locale?: string;
}

const cache = new Map<string, Promise<Database>>();

// locale -> db
type Database = Map<
  string,
  {
    type: "simple" | "advanced";
    db: AnyOrama;
  }
>;

async function loadDB({
  from = "/api/search",
  initOrama = (locale) => create({ schema: { _: "string" }, language: locale })
}: StaticOptions): Promise<Database> {
  const cacheKey = from;
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  async function init() {
    const res = await fetch(from);

    if (!res.ok)
      throw new Error(
        `failed to fetch exported search indexes from ${from}, make sure the search database is exported and available for client.`
      );

    const data = (await res.json()) as ExportedData;
    const dbs: Database = new Map();

    if (data.type === "i18n") {
      await Promise.all(
        Object.entries(data.data).map(async ([k, v]) => {
          const db = await initOrama(k);

          load(db, v);
          dbs.set(k, {
            type: v.type,
            db
          });
        })
      );

      return dbs;
    }

    const db = await initOrama();
    load(db, data);
    dbs.set("", {
      type: data.type,
      db
    });
    return dbs;
  }

  const result = init();
  cache.set(cacheKey, result);
  return result;
}

export async function search(query: string, options: StaticOptions) {
  const { tag, locale } = options;

  const db = (await loadDB(options)).get(locale ?? "");

  if (!db) return [];
  if (db.type === "simple") return searchSimple(db as unknown as Orama<typeof simpleSchema>, query);

  return searchAdvanced(db.db as Orama<typeof advancedSchema>, query, tag);
}

export async function searchSimple(
  db: Orama<typeof simpleSchema>,
  query: string,
  params: Partial<SearchParams<Orama<typeof simpleSchema>, SimpleDocument>> = {}
): Promise<SortedResult[]> {
  const highlighter = createContentHighlighter(query);
  const result = await searchOrama(db, {
    term: query,
    tolerance: 1,
    ...params,
    boost: {
      title: 2,
      ...("boost" in params ? params.boost : undefined)
    }
  });

  return result.hits.map<SortedResult>((hit) => ({
    type: "page",
    content: hit.document.title,
    breadcrumbs: hit.document.breadcrumbs,
    contentWithHighlights: highlighter.highlight(hit.document.title),
    id: hit.document.url,
    url: hit.document.url
  }));
}

export async function searchAdvanced(
  db: Orama<typeof advancedSchema>,
  query: string,
  tag: string | string[] = [],
  {
    mode = "fulltext",
    ...override
  }: Partial<SearchParams<Orama<typeof advancedSchema>, AdvancedDocument>> = {}
): Promise<SortedResult[]> {
  if (typeof tag === "string") tag = [tag];

  let params = {
    ...override,
    mode,
    where: removeUndefined({
      tags:
        tag.length > 0
          ? {
              containsAll: tag
            }
          : undefined,
      ...override.where
    }),
    groupBy: {
      properties: ["page_id"],
      maxResult: 8,
      ...override.groupBy
    }
  } as SearchParams<typeof db, AdvancedDocument>;

  if (query.length > 0) {
    params = {
      ...params,
      term: query,
      properties: mode === "fulltext" ? ["content"] : ["content", "embeddings"]
    } as SearchParams<typeof db, AdvancedDocument>;
  }

  const highlighter = createContentHighlighter(query);
  const result = await searchOrama(db, params);
  const list: SortedResult[] = [];
  for (const item of result.groups ?? []) {
    const pageId = item.values[0] as string;

    const page = getByID(db, pageId);
    if (!page) continue;

    list.push({
      id: pageId,
      type: "page",
      content: page.content,
      breadcrumbs: page.breadcrumbs,
      contentWithHighlights: highlighter.highlight(page.content),
      url: page.url
    });

    for (const hit of item.result) {
      if (hit.document.type === "page") continue;

      list.push({
        id: hit.document.id.toString(),
        content: hit.document.content,
        breadcrumbs: hit.document.breadcrumbs,
        contentWithHighlights: highlighter.highlight(hit.document.content),
        type: hit.document.type as SortedResult["type"],
        url: hit.document.url
      });
    }
  }
  return list;
}
