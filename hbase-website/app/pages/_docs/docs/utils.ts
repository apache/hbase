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

import Asciidoctor from "@asciidoctor/core";

const asciidoctor = Asciidoctor();

export interface Heading {
  id: string;
  text: string;
  level: number;
  element?: HTMLElement;
}

export interface ParsedDoc {
  html: string;
  headings: Heading[];
  title: string;
}

export interface SearchItem {
  id: string;
  title: string;
  content: string;
  type: "heading" | "body";
  headingId?: string;
  headingText?: string;
  level?: number;
}

/**
 * Parse AsciiDoc content and convert to HTML
 */
export function parseAsciiDoc(content: string): ParsedDoc {
  const doc = asciidoctor.load(content, {
    safe: "safe",
    doctype: "book",
    attributes: {
      showtitle: true,
      icons: "font",
      sectanchors: true,
      sectlinks: true,
      linkattrs: true,
      idprefix: "",
      idseparator: "-",
      "toc!": "", // Disable built-in TOC
      experimental: true,
      "source-language": "java"
    }
  });

  const html = doc.convert() as string;
  const title = doc.getDocumentTitle() as string;

  // Extract headings from the parsed document
  const headings = extractHeadingsFromHtml(html);

  return { html, headings, title };
}

/**
 * Extract headings from HTML content
 */
function extractHeadingsFromHtml(html: string): Heading[] {
  // Create a temporary DOM element to parse the HTML
  if (typeof window === "undefined") {
    // Server-side: parse without DOM
    return extractHeadingsFromString(html);
  }

  const tempDiv = document.createElement("div");
  tempDiv.innerHTML = html;

  const headings: Heading[] = [];
  const headingElements = tempDiv.querySelectorAll("h1, h2, h3, h4, h5, h6");

  headingElements.forEach((element) => {
    const level = parseInt(element.tagName.substring(1));
    const text = element.textContent?.trim() || "";
    let id = element.id;

    // If no id exists, create one from the text
    if (!id) {
      id = text
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");
      element.id = id;
    }

    headings.push({
      id,
      text,
      level,
      element: element as HTMLElement
    });
  });

  return headings;
}

/**
 * Extract headings from HTML string (server-side)
 */
function extractHeadingsFromString(html: string): Heading[] {
  const headings: Heading[] = [];
  const headingRegex = /<h([1-6])[^>]*id="([^"]*)"[^>]*>(.*?)<\/h\1>/gi;

  let match;
  while ((match = headingRegex.exec(html)) !== null) {
    const level = parseInt(match[1]);
    const id = match[2];
    // Remove HTML tags from text
    const text = match[3].replace(/<[^>]*>/g, "").trim();

    headings.push({
      id,
      text,
      level
    });
  }

  return headings;
}

/**
 * Build a search index from parsed documents
 */
export function buildSearchIndex(docs: ParsedDoc[]): SearchItem[] {
  const searchItems: SearchItem[] = [];

  docs.forEach((doc) => {
    // Add headings to search index with higher priority
    doc.headings.forEach((heading) => {
      searchItems.push({
        id: `heading-${heading.id}`,
        title: heading.text,
        content: heading.text,
        type: "heading",
        headingId: heading.id,
        headingText: heading.text,
        level: heading.level
      });
    });

    // Extract body content and add to search index
    const bodyContent = extractBodyContent(doc.html, doc.headings);
    bodyContent.forEach((item) => {
      searchItems.push(item);
    });
  });

  return searchItems;
}

/**
 * Extract body content sections for search
 */
function extractBodyContent(html: string, headings: Heading[]): SearchItem[] {
  const items: SearchItem[] = [];

  if (typeof window === "undefined") {
    // Server-side: simplified extraction
    return items;
  }

  const tempDiv = document.createElement("div");
  tempDiv.innerHTML = html;

  // Get all paragraph elements
  const paragraphs = tempDiv.querySelectorAll("p, li, td, blockquote");

  paragraphs.forEach((p, index) => {
    const content = p.textContent?.trim() || "";
    if (content.length < 20) return; // Skip very short content

    // Find the nearest preceding heading
    let currentHeading: Heading | undefined;
    let currentElement = p.previousElementSibling;

    while (currentElement && !currentHeading) {
      if (currentElement.tagName.match(/^H[1-6]$/)) {
        const headingId = currentElement.id;
        currentHeading = headings.find((h) => h.id === headingId);
        break;
      }
      currentElement = currentElement.previousElementSibling;
    }

    items.push({
      id: `body-${index}`,
      title: currentHeading?.text || "Content",
      content,
      type: "body",
      headingId: currentHeading?.id,
      headingText: currentHeading?.text
    });
  });

  return items;
}

/**
 * Generate a table of contents tree from headings
 */
export interface TocItem {
  id: string;
  text: string;
  level: number;
  children: TocItem[];
}

export function generateToc(headings: Heading[]): TocItem[] {
  const toc: TocItem[] = [];
  const stack: TocItem[] = [];

  headings.forEach((heading) => {
    const item: TocItem = {
      id: heading.id,
      text: heading.text,
      level: heading.level,
      children: []
    };

    // Find the parent in the stack
    while (stack.length > 0 && stack[stack.length - 1].level >= item.level) {
      stack.pop();
    }

    if (stack.length === 0) {
      toc.push(item);
    } else {
      stack[stack.length - 1].children.push(item);
    }

    stack.push(item);
  });

  return toc;
}

/**
 * Scroll to element smoothly
 */
export function scrollToElement(elementId: string) {
  const element = document.getElementById(elementId);
  if (element) {
    const offset = 80; // Account for fixed header
    const elementPosition = element.getBoundingClientRect().top;
    const offsetPosition = elementPosition + window.pageYOffset - offset;

    window.scrollTo({
      top: offsetPosition,
      behavior: "smooth"
    });
  }
}

/**
 * Get currently visible heading based on scroll position
 */
export function getCurrentHeadingId(headings: Heading[]): string | null {
  if (typeof window === "undefined" || headings.length === 0) {
    return null;
  }

  const scrollPosition = window.scrollY + 100; // Offset for header

  // Find the last heading that is above the current scroll position
  let currentId: string | null = null;

  for (let i = headings.length - 1; i >= 0; i--) {
    const element = document.getElementById(headings[i].id);
    if (element) {
      const elementTop = element.offsetTop;
      if (elementTop <= scrollPosition) {
        currentId = headings[i].id;
        break;
      }
    }
  }

  return currentId || headings[0]?.id || null;
}

/**
 * Combine multiple AsciiDoc files into a single document
 */
export function combineDocuments(contents: string[]): string {
  return contents.join("\n\n");
}

