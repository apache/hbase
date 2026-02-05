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
import {
  extractField,
  shouldSkipProperty,
  parseProperties,
  formatDescription,
  escapeUnwrappedDollars,
  generateMDX
} from "../scripts/extract-hbase-config.js";

describe("extract-hbase-config script", () => {
  const createXml = (propertiesXml) => `<?xml version="1.0" encoding="UTF-8"?>
<configuration>
${propertiesXml}
</configuration>`;

  describe("extractField", () => {
    it("should extract existing field value", () => {
      const block = "<name>hbase.master.port</name><value>16000</value>";
      expect(extractField(block, "name")).toBe("hbase.master.port");
      expect(extractField(block, "value")).toBe("16000");
    });

    it("should return empty string for missing field", () => {
      const block = "<name>hbase.master.port</name><value>16000</value>";
      expect(extractField(block, "description")).toBe("");
    });

    it("should trim whitespace from field values", () => {
      const block = "<name>  hbase.master.port  </name><value>  16000  </value>";
      expect(extractField(block, "name")).toBe("hbase.master.port");
      expect(extractField(block, "value")).toBe("16000");
    });

    it("should handle multiline field values", () => {
      const block = "<description>This is a\nmultiline\ndescription</description>";
      const result = extractField(block, "description");
      expect(result).toContain("This is a");
      expect(result).toContain("multiline");
      expect(result).toContain("description");
    });

    it("should handle empty field values", () => {
      const block = "<name>test.property</name><value></value>";
      expect(extractField(block, "value")).toBe("");
    });
  });

  describe("shouldSkipProperty", () => {
    it('should return true for skipInDoc="true"', () => {
      const block = '<property skipInDoc="true"><name>test</name></property>';
      expect(shouldSkipProperty(block)).toBe(true);
    });

    it("should return true for skipInDoc='true'", () => {
      const block = "<property skipInDoc='true'><name>test</name></property>";
      expect(shouldSkipProperty(block)).toBe(true);
    });

    it('should return false for skipInDoc="false"', () => {
      const block = '<property skipInDoc="false"><name>test</name></property>';
      expect(shouldSkipProperty(block)).toBe(false);
    });

    it("should return false when skipInDoc is not present", () => {
      const block = "<property><name>test</name></property>";
      expect(shouldSkipProperty(block)).toBe(false);
    });

    it("should handle skipInDoc with extra whitespace", () => {
      const block = '<property skipInDoc = "true" ><name>test</name></property>';
      expect(shouldSkipProperty(block)).toBe(true);
    });
  });

  describe("parseProperties", () => {
    it("should extract properties with all fields present", () => {
      const xml = createXml(`  <property>
    <name>hbase.master.port</name>
    <value>16000</value>
    <description>The port the HBase Master listens on.</description>
  </property>
  <property>
    <name>hbase.regionserver.port</name>
    <value>16020</value>
    <description>The port the HBase RegionServer listens on.</description>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(2);
      expect(properties[0]).toEqual({
        name: "hbase.master.port",
        value: "16000",
        description: "The port the HBase Master listens on."
      });
      expect(properties[1]).toEqual({
        name: "hbase.regionserver.port",
        value: "16020",
        description: "The port the HBase RegionServer listens on."
      });
    });

    it("should handle properties without description", () => {
      const xml = createXml(`  <property>
    <name>hbase.test.property</name>
    <value>testvalue</value>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(1);
      expect(properties[0]).toEqual({
        name: "hbase.test.property",
        value: "testvalue",
        description: ""
      });
    });

    it("should handle properties without value", () => {
      const xml = createXml(`  <property>
    <name>hbase.empty.property</name>
    <description>Property with no value</description>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(1);
      expect(properties[0]).toEqual({
        name: "hbase.empty.property",
        value: "",
        description: "Property with no value"
      });
    });

    it('should skip properties with skipInDoc="true"', () => {
      const xml = createXml(`  <property>
    <name>visible.property</name>
    <value>visible</value>
    <description>This should be included</description>
  </property>
  <property skipInDoc="true">
    <name>hidden.property</name>
    <value>hidden</value>
    <description>This should be skipped</description>
  </property>
  <property>
    <name>another.visible.property</name>
    <value>visible2</value>
    <description>This should also be included</description>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(2);
      expect(properties[0].name).toBe("visible.property");
      expect(properties[1].name).toBe("another.visible.property");
    });

    it("should skip properties without name", () => {
      const xml = createXml(`  <property>
    <value>orphan</value>
    <description>Property without name</description>
  </property>
  <property>
    <name>valid.property</name>
    <value>valid</value>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(1);
      expect(properties[0].name).toBe("valid.property");
    });

    it("should handle empty configuration", () => {
      const xml = createXml("");
      const properties = parseProperties(xml);
      expect(properties).toHaveLength(0);
    });

    it("should trim whitespace from field values", () => {
      const xml = createXml(`  <property>
    <name>  spacey.property  </name>
    <value>  spacey value  </value>
    <description>  Spacey description  </description>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(1);
      expect(properties[0]).toEqual({
        name: "spacey.property",
        value: "spacey value",
        description: "Spacey description"
      });
    });

    it("should handle fields in different order", () => {
      const xml = createXml(`  <property>
    <description>Description first</description>
    <value>123</value>
    <name>reordered.property</name>
  </property>`);

      const properties = parseProperties(xml);

      expect(properties).toHaveLength(1);
      expect(properties[0]).toEqual({
        name: "reordered.property",
        value: "123",
        description: "Description first"
      });
    });
  });

  describe("formatDescription", () => {
    it("should return empty string for empty description", () => {
      expect(formatDescription("")).toBe("");
      expect(formatDescription(null)).toBe("");
      expect(formatDescription(undefined)).toBe("");
    });

    it("should normalize whitespace", () => {
      const description = "This is a   multi-space    description";
      expect(formatDescription(description)).toBe("This is a multi-space description");
    });

    it("should join multiple lines with spaces", () => {
      const description = "Line one\nLine two\nLine three";
      expect(formatDescription(description)).toBe("Line one Line two Line three");
    });

    it("should remove empty lines", () => {
      const description = "Line one\n\n\nLine two\n\nLine three";
      expect(formatDescription(description)).toBe("Line one Line two Line three");
    });

    it("should trim each line", () => {
      const description = "  Line one  \n  Line two  \n  Line three  ";
      expect(formatDescription(description)).toBe("Line one Line two Line three");
    });

    it("should escape dollar signs", () => {
      const description = "Cost is $100 and price is $200";
      expect(formatDescription(description)).toBe("Cost is \\$100 and price is \\$200");
    });

    it("should not escape dollar signs in inline code", () => {
      const description = "Use `$VAR` for variable";
      expect(formatDescription(description)).toBe("Use `$VAR` for variable");
    });

    it("should not escape dollar signs in fenced code", () => {
      const description = "Example: ```$VAR=value```";
      expect(formatDescription(description)).toBe("Example: ```$VAR=value```");
    });
  });

  describe("escapeUnwrappedDollars", () => {
    it("should escape standalone dollar signs", () => {
      expect(escapeUnwrappedDollars("This costs $100")).toBe("This costs \\$100");
      expect(escapeUnwrappedDollars("$VAR")).toBe("\\$VAR");
      expect(escapeUnwrappedDollars("Price: $50")).toBe("Price: \\$50");
    });

    it("should not escape dollar signs in inline code", () => {
      expect(escapeUnwrappedDollars("Use `$VAR` here")).toBe("Use `$VAR` here");
      expect(escapeUnwrappedDollars("`$PATH`")).toBe("`$PATH`");
    });

    it("should not escape dollar signs in fenced code blocks", () => {
      expect(escapeUnwrappedDollars("```$VAR=test```")).toBe("```$VAR=test```");
      expect(escapeUnwrappedDollars("```\n$VAR\n```")).toBe("```\n$VAR\n```");
    });

    it("should handle mixed code and text", () => {
      const text = "Cost $10, use `$VAR`, and pay $20";
      expect(escapeUnwrappedDollars(text)).toBe("Cost \\$10, use `$VAR`, and pay \\$20");
    });

    it("should handle multiple inline code blocks", () => {
      const text = "Use `$VAR1` and `$VAR2` but not $VAR3";
      expect(escapeUnwrappedDollars(text)).toBe("Use `$VAR1` and `$VAR2` but not \\$VAR3");
    });

    it("should handle nested backticks correctly", () => {
      const text = "Before `code $VAR` after $value";
      expect(escapeUnwrappedDollars(text)).toBe("Before `code $VAR` after \\$value");
    });

    it("should not escape already escaped dollar signs", () => {
      expect(escapeUnwrappedDollars("Already \\$escaped")).toBe("Already \\$escaped");
    });

    it("should handle empty string", () => {
      expect(escapeUnwrappedDollars("")).toBe("");
    });

    it("should handle string without dollar signs", () => {
      expect(escapeUnwrappedDollars("No dollars here")).toBe("No dollars here");
    });

    it("should handle fenced code blocks with multiple lines", () => {
      const text = "```\nexport $VAR=value\necho $VAR\n``` outside $money";
      expect(escapeUnwrappedDollars(text)).toBe(
        "```\nexport $VAR=value\necho $VAR\n``` outside \\$money"
      );
    });
  });

  describe("generateMDX", () => {
    it("should generate MDX with header", () => {
      const properties = [
        {
          name: "test.property",
          value: "123",
          description: "Test description"
        }
      ];

      const mdx = generateMDX(properties);

      expect(mdx).toContain('title: "HBase Default Configuration"');
      expect(mdx).toContain('description: "Complete reference');
      expect(mdx).toContain("---");
    });

    it("should format single property correctly", () => {
      const properties = [
        {
          name: "hbase.master.port",
          value: "16000",
          description: "The port the HBase Master listens on."
        }
      ];

      const mdx = generateMDX(properties);

      expect(mdx).toContain("#### `hbase.master.port` [!toc]");
      expect(mdx).toContain("**Description:** The port the HBase Master listens on.");
      expect(mdx).toContain("**Default:** `16000`");
    });

    it("should handle empty value", () => {
      const properties = [
        {
          name: "test.property",
          value: "",
          description: "Test property"
        }
      ];

      const mdx = generateMDX(properties);

      expect(mdx).toContain("**Default:** `(empty)`");
    });

    it("should format multiple properties", () => {
      const properties = [
        {
          name: "prop1",
          value: "value1",
          description: "Description 1"
        },
        {
          name: "prop2",
          value: "value2",
          description: "Description 2"
        }
      ];

      const mdx = generateMDX(properties);

      expect(mdx).toContain("#### `prop1` [!toc]");
      expect(mdx).toContain("**Description:** Description 1");
      expect(mdx).toContain("**Default:** `value1`");
      expect(mdx).toContain("#### `prop2` [!toc]");
      expect(mdx).toContain("**Description:** Description 2");
      expect(mdx).toContain("**Default:** `value2`");
    });

    it("should handle empty description", () => {
      const properties = [
        {
          name: "test.property",
          value: "123",
          description: ""
        }
      ];

      const mdx = generateMDX(properties);

      expect(mdx).toContain("**Description:**  ");
      expect(mdx).toContain("**Default:** `123`");
    });

    it("should handle empty properties array", () => {
      const properties = [];
      const mdx = generateMDX(properties);

      expect(mdx).toContain('title: "HBase Default Configuration"');
      expect(mdx).toContain("---");
    });

    it("should escape special characters in property values", () => {
      const properties = [
        {
          name: "test.property",
          value: "value with `backticks`",
          description: "Test"
        }
      ];

      const mdx = generateMDX(properties);

      expect(mdx).toContain("**Default:** `value with `backticks``");
    });
  });
});
