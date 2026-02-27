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

const singlePagePath = path.join(process.cwd(), "app/pages/_docs/docs/_mdx/single-page/index.mdx");

interface HeadingMatch {
  lineNumber: number;
  title: string;
  anchor?: string;
  includePath: string;
}

function parseHeading(rawHeading: string) {
  const withAnchor = rawHeading.match(/^(.*?)\s*\[#([^\]]+)\]\s*$/);
  if (withAnchor) {
    return {
      title: withAnchor[1].trim(),
      anchor: withAnchor[2].trim()
    };
  }

  return {
    title: rawHeading.trim(),
    anchor: undefined
  };
}

function normalizeIncludePath(rawInclude: string) {
  return rawInclude
    .replaceAll("<include>", "")
    .replaceAll("</include>", "")
    .replace(/\s+/g, "")
    .trim();
}

function expectedSlugFromInclude(includePath: string) {
  const segments = includePath.split("/").filter(Boolean);
  const last = segments[segments.length - 1];

  if (last === "index.mdx") {
    return segments[segments.length - 2];
  }

  return last.replace(/\.mdx$/, "");
}

function collectHeadingMatches(mdx: string) {
  const lines = mdx.split("\n");
  const headingMatches: HeadingMatch[] = [];

  for (let i = 0; i < lines.length; i++) {
    const headingMatch = lines[i].match(/^(#{1,2})\s+(.*)$/);
    if (!headingMatch) {
      continue;
    }

    let includeStart = -1;
    for (let j = i + 1; j < lines.length; j++) {
      if (/^#{1,2}\s+/.test(lines[j])) {
        break;
      }
      if (lines[j].includes("<include>")) {
        includeStart = j;
        break;
      }
    }

    if (includeStart === -1) {
      continue;
    }

    let includeEnd = includeStart;
    while (includeEnd < lines.length && !lines[includeEnd].includes("</include>")) {
      includeEnd++;
    }

    const includeRaw = lines.slice(includeStart, includeEnd + 1).join("");
    const includePath = normalizeIncludePath(includeRaw);
    const { title, anchor } = parseHeading(headingMatch[2]);

    headingMatches.push({
      lineNumber: i + 1,
      title,
      anchor,
      includePath
    });
  }

  return headingMatches;
}

describe("Single-page heading anchors", () => {
  it("keeps explicit heading anchors aligned with include routes so links correspond to the routes we have in multi-page version", () => {
    const mdx = fs.readFileSync(singlePagePath, "utf-8");
    const headingMatches = collectHeadingMatches(mdx);

    const mismatches = headingMatches.flatMap((heading) => {
      if (heading.title === "Preface") {
        // Preface is intentionally exempt from this strict mapping rule.
        return [];
      }

      const expected = expectedSlugFromInclude(heading.includePath);
      if (heading.anchor === expected) {
        return [];
      }

      return [
        `Line ${heading.lineNumber}: "${heading.title}" expected [#${expected}] from include "${heading.includePath}", got "${heading.anchor ?? "missing"}"`
      ];
    });

    expect(mismatches, mismatches.join("\n")).toEqual([]);
  });
});
