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
import fs from "node:fs";
import path from "node:path";

const MULTI_PAGE_DIR = path.join(process.cwd(), "app/pages/_docs/docs/_mdx/(multi-page)");

function collectMdxFiles(dir: string): string[] {
  const results: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectMdxFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith(".mdx")) {
      results.push(fullPath);
    }
  }
  return results;
}

/** Extract all footnote definition IDs (e.g. [^1]:, [^note]:) from a file. */
function parseFootnoteIds(content: string): string[] {
  const re = /^\[\^([^\]]+)\]:/gm;
  const ids: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(content)) !== null) {
    ids.push(m[1]);
  }
  return ids;
}

describe("MDX Footnote Reference Uniqueness Validation", () => {
  it("should not have duplicate footnote reference IDs across all multi-page documentation files", () => {
    const files = collectMdxFiles(MULTI_PAGE_DIR);

    // Map of footnote ID -> array of relative file paths that define it
    const idToFiles = new Map<string, string[]>();

    for (const filePath of files) {
      const content = fs.readFileSync(filePath, "utf-8");
      const ids = parseFootnoteIds(content);
      const relPath = path.relative(process.cwd(), filePath);

      for (const id of ids) {
        if (!idToFiles.has(id)) {
          idToFiles.set(id, []);
        }
        idToFiles.get(id)!.push(relPath);
      }
    }

    const duplicates = Array.from(idToFiles.entries())
      .filter(([, filePaths]) => filePaths.length > 1)
      .map(([id, filePaths]) => ({ id, filePaths }));

    if (duplicates.length > 0) {
      console.error("\n❌ Duplicate footnote reference IDs found across different files:\n");

      duplicates.forEach(({ id, filePaths }) => {
        console.error(`  Reference ID: "[^${id}]"`);
        console.error(`  Defined in ${filePaths.length} files:\n`);
        filePaths.forEach((f) => console.error(`    • ${f}`));
        console.error();
      });

      console.error(
        "💡 To fix: Footnote IDs must be unique across all multi-page docs because\n" +
          "   they are combined into a single page. Rename conflicting references to\n" +
          "   use unique identifiers, e.g. [^1] → [^version-number-1].\n"
      );
    }

    expect(duplicates.length).toBe(0);
  });
});
