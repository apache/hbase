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

import { describe, it, expect } from "vitest";
import { readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { source } from "@/lib/source";
import { isValidElement } from "react";
import type { ReactNode } from "react";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SINGLE_PAGE_MDX = resolve(__dirname, "../app/pages/_docs/docs/_mdx/single-page/index.mdx");

function reactNodeToString(node: ReactNode): string {
  if (node == null || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(reactNodeToString).join("");
  if (isValidElement(node)) {
    const { children } = node.props as { children?: ReactNode };
    return reactNodeToString(children);
  }
  return "";
}

interface TOCItem {
  title: ReactNode;
  url: string;
  depth: number;
}

interface PageOccurrence {
  url: string;
  headingTitle: string;
}

/** Extract all [#id] custom heading IDs from single-page/index.mdx */
function parseSinglePageIds(): Map<string, string> {
  const content = readFileSync(SINGLE_PAGE_MDX, "utf-8");
  // Matches lines like:  # Section Title [#some-id]
  const re = /^#{1,6}\s+.*?\[#([^\]]+)\]/gm;
  const ids = new Map<string, string>(); // id -> full heading text
  let m: RegExpExecArray | null;
  while ((m = re.exec(content)) !== null) {
    const id = m[1];
    const line = m[0].trim();
    ids.set(id, line);
  }
  return ids;
}

describe("MDX Heading ID Uniqueness Validation", () => {
  it("should not have duplicate heading IDs across all documentation pages", () => {
    // Get all pages from Fumadocs source
    const pages = source.getPages();

    // Map of heading ID -> array of page occurrences
    const idToPages = new Map<string, PageOccurrence[]>();

    // Collect all heading IDs from all pages
    for (const page of pages) {
      // Skip the single-page itself
      if (page.url.includes("single-page")) continue;

      // Get the TOC (Table of Contents) - this has the actual heading IDs
      const toc = (page.data.toc || []) as TOCItem[];

      toc.forEach((heading) => {
        // heading.url is like "#prerequisites" or "#getting-started"
        const id = heading.url.replace("#", "");

        if (!idToPages.has(id)) {
          idToPages.set(id, []);
        }

        const titleString = reactNodeToString(heading.title);

        idToPages.get(id)!.push({
          url: page.url,
          headingTitle: titleString
        });
      });
    }

    // Find IDs that appear in multiple different pages
    const duplicates = Array.from(idToPages.entries())
      .filter(([, occurrences]) => {
        // Check if this ID appears in multiple DIFFERENT pages
        const uniquePages = new Set(occurrences.map((o) => o.url));
        return uniquePages.size > 1;
      })
      .map(([id, occurrences]) => ({ id, occurrences }));

    // Report duplicates with improved logging
    if (duplicates.length > 0) {
      console.error("\n❌ Duplicate heading IDs found across different pages:\n");

      duplicates.forEach(({ id, occurrences }) => {
        console.error(`\n  Heading ID: "${id}"`);
        console.error(`  Found in ${occurrences.length} locations:\n`);

        // Group by unique pages to avoid duplicates
        const uniquePages = new Map<string, string>();
        occurrences.forEach((occ) => {
          if (!uniquePages.has(occ.url)) {
            uniquePages.set(occ.url, occ.headingTitle);
          }
        });

        uniquePages.forEach((title, url) => {
          console.error(`    • ${url}#${id}`);
          console.error(`      Heading: "${title}"`);
        });
      });

      console.error(`\n💡 To fix: Rename these headings to be unique across all pages\n`);
    }

    expect(duplicates.length).toBe(0);
  });

  it("should not have multi-page heading IDs that conflict with single-page custom heading IDs", () => {
    // The single-page document (single-page/index.mdx) includes all multi-page
    // files under explicit [#id] anchors. Any multi-page heading that generates
    // the same ID would produce a duplicate anchor in the combined page.

    const singlePageIds = parseSinglePageIds();

    const pages = source.getPages().filter((p) => !p.url.includes("single-page"));

    interface Conflict {
      singlePageId: string;
      singlePageHeading: string;
      multiPageUrl: string;
      multiPageHeadingTitle: string;
    }

    const conflicts: Conflict[] = [];

    for (const page of pages) {
      const toc = (page.data.toc || []) as TOCItem[];
      for (const heading of toc) {
        const id = heading.url.replace("#", "");
        if (singlePageIds.has(id)) {
          conflicts.push({
            singlePageId: id,
            singlePageHeading: singlePageIds.get(id)!,
            multiPageUrl: page.url,
            multiPageHeadingTitle: reactNodeToString(heading.title)
          });
        }
      }
    }

    if (conflicts.length > 0) {
      console.error("\n❌ Heading ID conflicts between single-page and multi-page docs:\n");

      conflicts.forEach(
        ({ singlePageId, singlePageHeading, multiPageUrl, multiPageHeadingTitle }) => {
          console.error(`  ID: #${singlePageId}`);
          console.error(`    single-page/index.mdx: "${singlePageHeading}"`);
          console.error(`    conflicts with "${multiPageHeadingTitle}" in ${multiPageUrl}`);
          console.error();
        }
      );

      console.error(
        "💡 To fix: Add a page-specific prefix to the conflicting multi-page\n" +
          "   headings, e.g.  ## Overview  →  ## Overview [#pagename-overview]\n" +
          "   then update all /docs/pagename#old-id links to use #new-id.\n"
      );
    }

    expect(conflicts.length).toBe(0);
  });
});
