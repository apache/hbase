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
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import GithubSlugger from "github-slugger";

function extractHeadings(content: string): string[] {
  const headingRegex = /^#+\s+(.+)$/gm;
  const headings: string[] = [];
  let match;

  while ((match = headingRegex.exec(content)) !== null) {
    headings.push(match[1].trim());
  }

  return headings;
}

function getAllMdxFiles(dir: string, files: string[] = []): string[] {
  const entries = readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      getAllMdxFiles(fullPath, files);
    } else if (entry.name.endsWith(".mdx")) {
      files.push(fullPath);
    }
  }

  return files;
}

describe("MDX Heading ID Uniqueness", () => {
  it("should not have duplicate heading IDs across all MDX files", () => {
    const baseDir = "app/pages/_docs/docs/_mdx/(multi-page)";
    const mdxFiles = getAllMdxFiles(baseDir);

    const slugger = new GithubSlugger();
    const idToFiles = new Map<string, string[]>();

    // Collect all heading IDs and track which files they're in
    for (const filePath of mdxFiles) {
      const content = readFileSync(filePath, "utf-8");
      const headings = extractHeadings(content);

      // Reset slugger for each file (simulates separate compilation)
      slugger.reset();

      headings.forEach((heading) => {
        const id = slugger.slug(heading);
        const relativePath = filePath.replace(baseDir + "/", "");

        if (!idToFiles.has(id)) {
          idToFiles.set(id, []);
        }
        idToFiles.get(id)!.push(relativePath);
      });
    }

    // Find duplicates
    const duplicates = Array.from(idToFiles.entries())
      .filter(([_, files]) => files.length > 1)
      .map(([id, files]) => ({ id, files }));

    if (duplicates.length > 0) {
      console.error("\n❌ Duplicate heading IDs found:\n");
      duplicates.forEach(({ id, files }) => {
        console.error(`  ID: "${id}"`);
        files.forEach((file) => console.error(`    - ${file}`));
        console.error("");
      });
    }

    expect(duplicates).toEqual([]);
  });
});
