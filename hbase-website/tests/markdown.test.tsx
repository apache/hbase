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
import { screen } from "@testing-library/react";
import { renderWithProviders } from "./utils";
import { MarkdownLayout } from "@/components/markdown-layout";

describe("MarkdownLayout", () => {
  it("renders markdown content", () => {
    const markdown = "# Hello World\n\nThis is a test.";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("Hello World")).toBeInTheDocument();
    expect(screen.getByText("This is a test.")).toBeInTheDocument();
  });

  it("renders headings correctly", () => {
    const markdown = "# Heading 1\n## Heading 2\n### Heading 3";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByRole("heading", { level: 1, name: "Heading 1" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Heading 2" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 3, name: "Heading 3" })).toBeInTheDocument();
  });

  it("renders paragraphs", () => {
    const markdown = "First paragraph.\n\nSecond paragraph.";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("First paragraph.")).toBeInTheDocument();
    expect(screen.getByText("Second paragraph.")).toBeInTheDocument();
  });

  it("renders links correctly", () => {
    const markdown = "[Internal Link](/test) and [External Link](https://example.com)";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    const internalLink = screen.getByRole("link", { name: /Internal Link/i });
    expect(internalLink).toHaveAttribute("href", "/test");

    const externalLink = screen.getByRole("link", { name: /External Link/i });
    expect(externalLink).toHaveAttribute("href", "https://example.com");
    expect(externalLink).toHaveAttribute("target", "_blank");
  });

  it("renders lists correctly", () => {
    const markdown = "- Item 1\n- Item 2\n- Item 3";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("Item 1")).toBeInTheDocument();
    expect(screen.getByText("Item 2")).toBeInTheDocument();
    expect(screen.getByText("Item 3")).toBeInTheDocument();
  });

  it("renders ordered lists", () => {
    const markdown = "1. First\n2. Second\n3. Third";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("First")).toBeInTheDocument();
    expect(screen.getByText("Second")).toBeInTheDocument();
    expect(screen.getByText("Third")).toBeInTheDocument();
  });

  it("renders inline code", () => {
    const markdown = "Use `const value = 42` for variables.";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    const code = screen.getByText("const value = 42");
    expect(code).toBeInTheDocument();
    expect(code.tagName).toBe("CODE");
  });

  it("renders code blocks", () => {
    const markdown = '```javascript\nconst greeting = "Hello";\nconsole.log(greeting);\n```';

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    // Code blocks with syntax highlighting split text into spans, so check for code element
    expect(screen.getByText("const")).toBeInTheDocument();
    expect(screen.getByText('"Hello"')).toBeInTheDocument();
  });

  it("renders blockquotes", () => {
    const markdown = "> This is a quote";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("This is a quote")).toBeInTheDocument();
  });

  it("renders bold and italic text", () => {
    const markdown = "**Bold text** and *italic text*";

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("Bold text")).toBeInTheDocument();
    expect(screen.getByText("italic text")).toBeInTheDocument();
  });

  it("renders tables with GFM", () => {
    const markdown = `
| Header 1 | Header 2 |
|----------|----------|
| Cell 1   | Cell 2   |
| Cell 3   | Cell 4   |
`;

    renderWithProviders(<MarkdownLayout>{markdown}</MarkdownLayout>);

    expect(screen.getByText("Header 1")).toBeInTheDocument();
    expect(screen.getByText("Header 2")).toBeInTheDocument();
    expect(screen.getByText("Cell 1")).toBeInTheDocument();
    expect(screen.getByText("Cell 2")).toBeInTheDocument();
  });
});
