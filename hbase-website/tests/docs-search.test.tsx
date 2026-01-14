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
import { scoreMatch } from "@/components/docs/docs-search";

describe("Search Ranking - scoreMatch", () => {
  describe("Exact matches", () => {
    it("should rank single-word exact page title highest", () => {
      const score = scoreMatch("Configuration", "configuration", true);
      expect(score).toBe(10000);
    });

    it("should rank exact multi-word page title very high", () => {
      const score = scoreMatch("HBase Configuration", "hbase configuration", true);
      expect(score).toBe(9500);
    });

    it("should rank exact heading lower than page title with word match", () => {
      const titleWordMatch = scoreMatch("Important Configuration Settings", "configuration", true);
      const exactHeading = scoreMatch("Configuration", "configuration", false);

      expect(titleWordMatch).toBeGreaterThan(exactHeading);
      expect(exactHeading).toBe(7000);
    });
  });

  describe("Breadcrumb and segment matches", () => {
    it("should rank page with exact segment alone very high", () => {
      const score = scoreMatch("Configuration & Setup > Configuration", "configuration", true);
      expect(score).toBe(9200);
    });

    it("should rank page with exact segment high", () => {
      const score = scoreMatch("Setup > Configuration Files", "configuration", true);
      // This matches "segmentStarts": "Configuration Files" segment starts with "configuration"
      // Word count for "setup > configuration files" = 4 words total
      expect(score).toBe(8200 - 4 * 20); // 8200 - 80 = 8120
    });
  });

  describe("Plural forms", () => {
    it("should rank exact plural form high but below exact match", () => {
      const exactScore = scoreMatch("Configuration", "configuration", true);
      const pluralScore = scoreMatch("Configurations", "configuration", true);

      expect(pluralScore).toBe(8800);
      expect(exactScore).toBeGreaterThan(pluralScore);
    });
  });

  describe("Word boundary matches", () => {
    it("should rank title with word match higher than exact heading", () => {
      const titleScore = scoreMatch("Example Configurations", "configuration", true);
      const headingScore = scoreMatch("Configuration", "configuration", false);

      // Title with word match should rank higher
      expect(titleScore).toBeGreaterThan(headingScore);
    });

    it("should detect word boundaries correctly", () => {
      const wordMatch = scoreMatch("The Important Configuration", "configuration", true);
      const substringMatch = scoreMatch("Misconfiguration Issues", "configuration", true);

      expect(wordMatch).toBeGreaterThan(substringMatch);
    });
  });

  describe("Real-world scenarios for 'configuration' search", () => {
    const testCases = [
      { text: "Configuration", type: "page", expectedRank: 1 },
      { text: "Configuration & Setup > Configuration", type: "page", expectedRank: 2 },
      { text: "Configurations", type: "page", expectedRank: 3 },
      { text: "Configuration Guide", type: "page", expectedRank: 4 },
      { text: "Example Configurations", type: "page", expectedRank: 5 },
      { text: "The Important Configurations", type: "page", expectedRank: 6 },
      { text: "Configuration", type: "heading", expectedRank: 7 }
    ];

    it("should rank results in correct order", () => {
      const scored = testCases.map((tc) => ({
        ...tc,
        score: scoreMatch(tc.text, "configuration", tc.type === "page")
      }));

      // Sort by score descending
      scored.sort((a, b) => b.score - a.score);

      // Check that the order matches expected ranks
      scored.forEach((item, index) => {
        expect(item.expectedRank).toBe(index + 1);
      });
    });

    it("should have scores in descending order", () => {
      const scores = testCases.map((tc) =>
        scoreMatch(tc.text, "configuration", tc.type === "page")
      );

      for (let i = 0; i < scores.length - 1; i++) {
        expect(scores[i]).toBeGreaterThanOrEqual(scores[i + 1]);
      }
    });
  });

  describe("Title vs Heading priority", () => {
    it("should always rank page titles higher than headings for same match quality", () => {
      const matchTypes = [
        "Configuration",
        "Configurations",
        "Configuration Guide",
        "Example Configuration",
        "HBase Configuration Settings"
      ];

      matchTypes.forEach((text) => {
        const titleScore = scoreMatch(text, "configuration", true);
        const headingScore = scoreMatch(text, "configuration", false);

        expect(titleScore).toBeGreaterThan(headingScore);
      });
    });
  });

  describe("Case insensitivity", () => {
    it("should be case insensitive", () => {
      const score1 = scoreMatch("Configuration", "configuration", true);
      const score2 = scoreMatch("CONFIGURATION", "configuration", true);
      const score3 = scoreMatch("CoNfIgUrAtIoN", "configuration", true);

      expect(score1).toBe(score2);
      expect(score2).toBe(score3);
    });
  });

  describe("Length penalty", () => {
    it("should penalize longer titles", () => {
      const short = scoreMatch("Configuration Guide", "configuration", true);
      const long = scoreMatch("Configuration Guide for Distributed Systems", "configuration", true);

      expect(short).toBeGreaterThan(long);
    });
  });

  describe("No match scenarios", () => {
    it("should return 0 for no match", () => {
      const score = scoreMatch("RegionServer", "configuration", true);
      expect(score).toBe(0);
    });

    it("should return 0 for empty text", () => {
      const score = scoreMatch("", "configuration", true);
      expect(score).toBe(0);
    });
  });

  describe("Edge cases", () => {
    it("should handle special characters in breadcrumbs", () => {
      const score1 = scoreMatch("Setup > Configuration", "configuration", true);
      const score2 = scoreMatch("Setup / Configuration", "configuration", true);
      const score3 = scoreMatch("Setup | Configuration", "configuration", true);

      // All should have same high score
      expect(score1).toBe(9200);
      expect(score2).toBe(9200);
      expect(score3).toBe(9200);
    });

    it("should handle URLs in search", () => {
      const score = scoreMatch("Configuration > Advanced", "configuration", true);
      expect(score).toBeGreaterThan(7000);
    });
  });
});

describe("Full ranking integration", () => {
  interface SearchResult {
    content: string;
    type: "page" | "heading";
    url: string;
  }

  function rankResults(results: SearchResult[], searchTerm: string) {
    return results
      .map((item) => ({
        ...item,
        score: scoreMatch(item.content, searchTerm, item.type === "page")
      }))
      .sort((a, b) => b.score - a.score);
  }

  it("should rank realistic search results correctly", () => {
    const mockResults: SearchResult[] = [
      { content: "Example Configurations", type: "page", url: "/docs/examples" },
      { content: "Configuration", type: "heading", url: "/docs/intro#config" },
      { content: "The Important Configurations", type: "page", url: "/docs/important" },
      { content: "Configuration & Setup > Configuration", type: "page", url: "/docs/config" },
      { content: "Configuration Guide", type: "page", url: "/docs/guide" },
      { content: "HBase Configuration Settings", type: "heading", url: "/docs/hbase#settings" }
    ];

    const ranked = rankResults(mockResults, "configuration");

    // Page with exact segment should be first
    expect(ranked[0].content).toBe("Configuration & Setup > Configuration");
    expect(ranked[0].type).toBe("page");

    // Page titles should rank above headings
    const firstHeadingIndex = ranked.findIndex((r) => r.type === "heading");
    const lastPageIndex = ranked.map((r) => r.type).lastIndexOf("page");

    expect(firstHeadingIndex).toBeGreaterThan(lastPageIndex);
  });
});
