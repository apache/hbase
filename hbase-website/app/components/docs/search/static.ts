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
import { type AdvancedDocument, type advancedSchema } from "./create-db";
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
            db
          });
        })
      );

      return dbs;
    }

    const db = await initOrama();
    load(db, data);
    dbs.set("", {
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

  return searchAdvanced(db.db as Orama<typeof advancedSchema>, query, tag);
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

  // Helper to score match quality (exact > starts with > contains)
  const getMatchQuality = (content: string, searchTerm: string): number => {
    const lower = content.toLowerCase();
    const term = searchTerm.toLowerCase();

    if (lower === term) return 1000; // Exact match
    if (lower.startsWith(term + " ")) return 500; // Starts with term + space
    if (lower.startsWith(term)) return 400; // Starts with term
    if (new RegExp(`\\b${term}\\b`, "i").test(content)) return 300; // Whole word
    if (lower.includes(term)) return 100; // Contains
    return 0;
  };

  // Collect all groups with scoring
  const groupsWithScores: Array<{
    pageId: string;
    pageScore: number;
    matchQuality: number;
    page: any;
    hits: any[];
  }> = [];

  for (const item of result.groups ?? []) {
    const pageId = item.values[0] as string;
    const page = getByID(db, pageId);
    if (!page) continue;

    // Find the page hit to get its Orama score
    const pageHit = item.result.find((hit: any) => hit.document.type === "page");
    const pageScore = pageHit?.score || 0;
    const matchQuality = getMatchQuality(page.content, query);

    groupsWithScores.push({
      pageId,
      pageScore,
      matchQuality,
      page,
      hits: item.result
    });
  }

  // Sort groups: exact matches first, then by Orama score
  groupsWithScores.sort((a, b) => {
    // Prioritize exact matches
    if (a.matchQuality !== b.matchQuality) {
      return b.matchQuality - a.matchQuality;
    }
    // Then by Orama relevance
    return b.pageScore - a.pageScore;
  });

  const list: SortedResult[] = [];

  // Build final list from sorted groups
  for (const { pageId, page, hits } of groupsWithScores) {
    // Add page title
    list.push({
      id: pageId,
      type: "page",
      content: page.content,
      breadcrumbs: page.breadcrumbs,
      contentWithHighlights: highlighter.highlight(page.content),
      url: page.url
    });

    // Sort hits within this group: by type, then match quality, then Orama score
    const sortedHits = [...hits]
      .filter((hit: any) => hit.document.type !== "page")
      .map((hit: any) => ({
        hit,
        typeScore: hit.document.type === "heading" ? 2 : 1,
        matchQuality: getMatchQuality(hit.document.content, query)
      }))
      .sort((a, b) => {
        // Type first (heading > text)
        if (a.typeScore !== b.typeScore) return b.typeScore - a.typeScore;
        // Then match quality (exact > partial)
        if (a.matchQuality !== b.matchQuality) return b.matchQuality - a.matchQuality;
        // Then Orama relevance
        return b.hit.score - a.hit.score;
      })
      .map((item) => item.hit);

    // Add sorted hits
    for (const hit of sortedHits) {
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
