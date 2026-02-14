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
import { source } from "@/lib/source";
import type { ReactNode } from "react";

interface TOCItem {
  title: ReactNode;
  url: string;
  depth: number;
}

interface PageOccurrence {
  url: string;
  headingTitle: string;
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

        // Convert ReactNode title to string
        const titleString =
          typeof heading.title === "string" ? heading.title : String(heading.title);

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
});
