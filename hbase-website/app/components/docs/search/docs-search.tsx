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
  SearchDialog as FumaDocsSearchDialog,
  SearchDialogClose,
  SearchDialogContent,
  SearchDialogHeader,
  SearchDialogIcon,
  SearchDialogInput,
  SearchDialogList,
  SearchDialogOverlay,
  type SharedProps
} from "fumadocs-ui/components/dialog/search";
// import { useDocsSearch } from "fumadocs-core/search/client";
import { useDocsSearch } from "./use-docs-search";
import { create } from "@orama/orama";
import { useI18n } from "fumadocs-ui/contexts/i18n";
import { useMemo } from "react";

function initOrama() {
  return create({
    schema: { _: "string" },
    language: "english"
  });
}

// Intelligent scoring that prioritizes page titles over headings
export function scoreMatch(text: string, searchTerm: string, isPageTitle: boolean): number {
  if (!text) return 0;

  const lowerText = text.toLowerCase().trim();
  const lowerTerm = searchTerm.toLowerCase().trim();

  const segments = lowerText.split(/[>/|]/).map((s: string) => s.trim());
  const wordCount = lowerText.split(/\s+/).length;

  // Check match types
  const isExactMatch = lowerText === lowerTerm;
  const hasExactSegmentAlone = segments.some(
    (seg: string) => seg === lowerTerm && seg.split(/\s+/).length === 1
  );
  const hasExactSegment = segments.some((seg: string) => seg === lowerTerm);
  const isExactPlural = lowerText === lowerTerm + "s" || lowerText === lowerTerm + "es";
  const startsWithTermSpace = lowerText.startsWith(lowerTerm + " ");
  const segmentStarts = segments.some((seg: string) => seg.startsWith(lowerTerm));
  const textStarts = lowerText.startsWith(lowerTerm);

  // Check for exact word boundary AND also plural forms
  const hasWordBoundary = new RegExp(`\\b${lowerTerm}\\b`, "i").test(lowerText);
  const hasPluralBoundary =
    new RegExp(`\\b${lowerTerm}s\\b`, "i").test(lowerText) ||
    new RegExp(`\\b${lowerTerm}es\\b`, "i").test(lowerText);
  const hasWordStart = new RegExp(`\\b${lowerTerm}`, "i").test(lowerText);
  const hasSubstring = lowerText.includes(lowerTerm);

  // Scoring hierarchy: Page titles rank higher than headings
  if (isPageTitle) {
    if (isExactMatch) return wordCount === 1 ? 10000 : 9500;
    if (hasExactSegmentAlone) return 9200;
    if (hasExactSegment) return 9000;
    if (isExactPlural) return 8800;
    if (startsWithTermSpace) return 8500 - wordCount * 20;
    if (segmentStarts) return 8200 - wordCount * 20;
    if (textStarts) return 8000 - wordCount * 20;
    if (hasPluralBoundary) return 7600 - wordCount * 15;
    if (hasWordBoundary) return 7500 - wordCount * 15;
    if (hasWordStart) return 6000 - wordCount * 15;
    if (hasSubstring) return 3000 - wordCount * 10;
  } else {
    if (isExactMatch) return 7000;
    if (hasExactSegmentAlone) return 6800;
    if (hasExactSegment) return 6500;
    if (isExactPlural) return 6200;
    if (startsWithTermSpace) return 5800 - wordCount * 20;
    if (segmentStarts) return 5500 - wordCount * 20;
    if (textStarts) return 5200 - wordCount * 20;
    if (hasPluralBoundary) return 4600 - wordCount * 15;
    if (hasWordBoundary) return 4500 - wordCount * 15;
    if (hasWordStart) return 3500 - wordCount * 15;
    if (hasSubstring) return 1500 - wordCount * 10;
  }

  return 0;
}

// Re-rank Fumadocs results with proper title/heading priority
function reRankResults(results: any[], searchTerm: string) {
  if (!results || results.length === 0 || !searchTerm) return results;

  console.log("\n========================================");
  console.log(`SEARCH DEBUG for: "${searchTerm}"`);
  console.log("========================================");
  console.log(`Received ${results.length} results from Fumadocs\n`);

  // Log first 3 raw results to see structure
  console.log("First 3 raw results:");
  results.slice(0, 3).forEach((item, i) => {
    console.log(`${i + 1}.`, {
      type: item.type,
      content: item.content,
      url: item.url,
      id: item.id,
      allKeys: Object.keys(item)
    });
  });
  console.log("");

  const scored = results.map((item) => {
    const isPageTitle = item.type === "page";
    const isHeading = item.type === "heading";
    const isText = item.type === "text";

    // Type priority scores - ensures pages > headings > text
    const typeScore = isPageTitle ? 100000 : isHeading ? 50000 : 0;

    let matchScore = 0;

    if (isPageTitle) {
      // Page titles: highest priority
      matchScore = scoreMatch(item.content || "", searchTerm, true);
    } else if (isHeading) {
      // Headings: medium priority
      matchScore = scoreMatch(item.content || "", searchTerm, false);
    } else if (isText) {
      // Text/paragraphs: lowest priority with minimal scoring
      matchScore = scoreMatch(item.content || "", searchTerm, false);
    }

    const urlBonus = (item.url || "").toLowerCase().includes(searchTerm.toLowerCase()) ? 10 : 0;

    return {
      ...item,
      _score: typeScore + matchScore + urlBonus
    };
  });

  scored.sort((a, b) => b._score - a._score);

  // Log top 10 re-ranked results
  console.log("Top 10 results after re-ranking:");
  scored.slice(0, 10).forEach((item, i) => {
    console.log(`${i + 1}. Score: ${item._score}`);
    console.log(`   Type: ${item.type}`);
    console.log(`   Content: ${item.content}`);
    console.log(`   URL: ${item.url}`);
    console.log("");
  });
  console.log("========================================\n");

  return scored.map(({ _score, ...item }) => item);
}

export function SearchDialog(props: SharedProps) {
  const { locale } = useI18n();

  const { search, setSearch, query } = useDocsSearch({
    type: "static",
    // tag: "multi-page",
    initOrama,
    locale
  });

  // Apply smart re-ranking to prioritize titles over headings
  // const reRankedResults = useMemo(() => {
  //   if (query.data === "empty" || !query.data || !Array.isArray(query.data)) {
  //     return query.data;
  //   }

  //   return reRankResults(query.data, search);
  // }, [query.data, search]);

  return (
    <FumaDocsSearchDialog
      search={search}
      onSearchChange={setSearch}
      isLoading={query.isLoading}
      {...props}
    >
      <SearchDialogOverlay />
      <SearchDialogContent>
        <SearchDialogHeader>
          <SearchDialogIcon />
          <SearchDialogInput />
          <SearchDialogClose />
        </SearchDialogHeader>
        <SearchDialogList items={query.data !== "empty" ? query.data : null} />
        {/* <SearchDialogList items={reRankedResults !== "empty" ? reRankedResults : null} /> */}
      </SearchDialogContent>
    </FumaDocsSearchDialog>
  );
}
