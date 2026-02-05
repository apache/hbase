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

import { test, expect } from "@playwright/test";
import fs from "node:fs/promises";
import path from "node:path";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import { fileNameVariants } from "../app/lib/export-pdf-types";

const outDir = "public/books";
const marginInches = 0.4;
const xMargin = marginInches * 72;
const yMargin = marginInches * 72;
const darkMarginColor = rgb(15 / 255, 15 / 255, 15 / 255);

const variants = [
  { name: fileNameVariants.light, theme: "light" },
  { name: fileNameVariants.dark, theme: "dark" }
];

async function postProcess(pdfPath: string, darkMode?: boolean) {
  const pdfBytes = await fs.readFile(pdfPath);
  const pdfDoc = await PDFDocument.load(pdfBytes);
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);

  let pageCount = 1;
  let startPageCount = false;
  const pages = pdfDoc.getPages();
  pages.forEach((page, index) => {
    const { width, height } = page.getSize();

    // Margins are white by default, so we're repainting them to black for the dark mode pdf
    if (darkMode) {
      // Left margin
      page.drawRectangle({
        x: 0,
        y: 0,
        width: xMargin,
        height,
        color: darkMarginColor,
        borderWidth: 0
      });

      // Right margin
      page.drawRectangle({
        x: width - xMargin,
        y: 0,
        width: xMargin,
        height,
        color: darkMarginColor,
        borderWidth: 0
      });

      // Top margin
      page.drawRectangle({
        x: 0,
        y: height - yMargin,
        width,
        height: yMargin,
        color: darkMarginColor,
        borderWidth: 0
      });

      // Bottom margin
      page.drawRectangle({
        x: 0,
        y: 0,
        width,
        height: yMargin,
        color: darkMarginColor,
        borderWidth: 0
      });
    }

    // TODO: Find a better solution, currently we just hardcoding a page we start counting page numbers from.
    if (!startPageCount && index === 16) {
      startPageCount = true;
    }

    if (startPageCount) {
      // Adding page number
      const fontSize = 12;
      const pageNumberColor = darkMode
        ? rgb(203 / 255, 203 / 255, 203 / 255)
        : rgb(34 / 255, 34 / 255, 34 / 255);
      const pageLabel = String(pageCount);
      const textWidth = font.widthOfTextAtSize(pageLabel, fontSize);
      const paddingX = 28;
      const paddingY = 10;
      const x = width - xMargin + (xMargin - textWidth) - paddingX;
      const y = paddingY;
      page.drawText(pageLabel, {
        x,
        y,
        size: fontSize,
        font,
        color: pageNumberColor
      });
      pageCount++;
    }
  });

  await fs.writeFile(pdfPath, await pdfDoc.save());
}

test("export documentation pdfs", async ({ browser, browserName }) => {
  test.skip(browserName !== "chromium", "PDF export only supported in Chromium.");
  test.setTimeout(3 * 60 * 1000);

  await fs.mkdir(outDir, { recursive: true });

  for (const variant of variants) {
    const page = await browser.newPage();
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });
    page.on("console", (message) => {
      if (message.type() === "error") {
        pageErrors.push(message.text());
      }
    });
    await page.addInitScript((theme) => {
      localStorage.setItem("theme", theme);
    }, variant.theme);

    await page.goto("/docs/single-page", { waitUntil: "networkidle" });

    await page.evaluate((theme) => {
      localStorage.setItem("theme", theme);
      document.documentElement.classList.remove("light", "dark");
      document.documentElement.classList.add(theme);
    }, variant.theme);

    await page.waitForFunction(
      `document.documentElement.classList.contains(${JSON.stringify(variant.theme)})`,
      { timeout: 10000 }
    );

    const errorBoundaryVisible = await page
      .locator("main")
      .filter({ hasText: "Oops!" })
      .isVisible()
      .catch(() => false);
    if (errorBoundaryVisible || pageErrors.length > 0) {
      throw new Error(
        `Export page failed for theme "${variant.theme}".\n` +
          `Errors: ${pageErrors.join(" | ") || "Unknown error"}`
      );
    }

    const pdfPath = path.join(outDir, variant.name);
    await page.pdf({
      path: pdfPath,
      format: "A4",
      margin: {
        top: `${marginInches}in`,
        right: `${marginInches}in`,
        bottom: `${marginInches}in`,
        left: `${marginInches}in`
      },
      printBackground: true
    });

    await page.close();

    await postProcess(pdfPath, variant.theme === "dark");

    // Verify PDF was created
    const pdfExists = await fs
      .access(pdfPath)
      .then(() => true)
      .catch(() => false);
    expect(pdfExists).toBe(true);

    // Verify PDF is not empty and has valid structure
    const pdfFileBytes = await fs.readFile(pdfPath);
    expect(pdfFileBytes.length).toBeGreaterThan(1000); // At least 1KB

    // Verify PDF can be loaded and has pages
    const verifyDoc = await PDFDocument.load(pdfFileBytes);
    const pageCount = verifyDoc.getPageCount();
    expect(pageCount).toBeGreaterThan(10); // Should have at least 10 pages

    console.log(
      `✓ ${variant.theme} theme PDF generated successfully (${pageCount} pages, ${(pdfFileBytes.length / 1024 / 1024).toFixed(2)} MB)`
    );
  }

  // Verify both variants were generated
  const lightPdfPath = path.join(outDir, fileNameVariants.light);
  const darkPdfPath = path.join(outDir, fileNameVariants.dark);

  const lightExists = await fs
    .access(lightPdfPath)
    .then(() => true)
    .catch(() => false);
  const darkExists = await fs
    .access(darkPdfPath)
    .then(() => true)
    .catch(() => false);

  expect(lightExists).toBe(true);
  expect(darkExists).toBe(true);

  // Verify both PDFs have similar page counts
  const lightDoc = await PDFDocument.load(await fs.readFile(lightPdfPath));
  const darkDoc = await PDFDocument.load(await fs.readFile(darkPdfPath));
  const lightPages = lightDoc.getPageCount();
  const darkPages = darkDoc.getPageCount();

  expect(lightPages - darkPages).toBe(0);

  console.log(`\n✅ All PDF exports validated successfully!`);
  console.log(`   Light: ${lightPages} pages`);
  console.log(`   Dark:  ${darkPages} pages`);
});
