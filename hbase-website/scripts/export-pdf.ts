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

// This script can export the HTML page in PDF.
// Run the local server first, then execute the script.
// It will output the single-page docs.
// Execute it from the root directory with `npx tsx scripts/export-pdf.ts`
// The output will be stored under the `pdfs` directory in the root directory of the website.

import puppeteer from "puppeteer";
import fs from "node:fs/promises";
import path from "node:path";
import { PDFDocument, rgb } from "pdf-lib";

const browser = await puppeteer.launch();
const outDir = "public/books";
// The path
const urls = ["/docs/single-page"];
const variants = [
  { name: "", theme: "light" },
  { name: "-dark-mode", theme: "dark" }
];

async function applyDarkBackground(pdfPath: string) {
  const pdfBytes = await fs.readFile(pdfPath);
  const pdfDoc = await PDFDocument.load(pdfBytes);

  const xMargin = 0.4 * 72; // points. 72 = 1 inch
  const yMargin = 0.39 * 72; // points. 72 = 1 inch
  const c = rgb(15 / 255, 15 / 255, 15 / 255); // #0F0F0F

  for (const page of pdfDoc.getPages()) {
    const { width, height } = page.getSize();

    // Left margin
    page.drawRectangle({
      x: 0,
      y: 0,
      width: xMargin,
      height,
      color: c,
      borderWidth: 0
    });

    // Right margin
    page.drawRectangle({
      x: width - xMargin,
      y: 0,
      width: xMargin,
      height,
      color: c,
      borderWidth: 0
    });

    // Top margin
    page.drawRectangle({
      x: 0,
      y: height - yMargin,
      width,
      height: yMargin,
      color: c,
      borderWidth: 0
    });

    // Bottom margin
    page.drawRectangle({
      x: 0,
      y: 0,
      width,
      height: yMargin,
      color: c,
      borderWidth: 0
    });
  }

  await fs.writeFile(pdfPath, await pdfDoc.save());
}

async function exportPdf(pathname: string, variant: (typeof variants)[number]) {
  const page = await browser.newPage();
  await page.evaluateOnNewDocument((theme) => {
    localStorage.setItem("theme", theme);
    document.documentElement.classList.remove("light", "dark");
    document.documentElement.classList.add(theme);
  }, variant.theme);
  await page.goto("http://localhost:5173" + pathname, {
    waitUntil: "networkidle2"
  });
  // Ensure the theme is applied after hydration/theme script runs.
  await page.evaluate((theme) => {
    localStorage.setItem("theme", theme);
    document.documentElement.classList.remove("light", "dark");
    document.documentElement.classList.add(theme);
  }, variant.theme);
  await page.waitForFunction(
    (theme) => document.documentElement.classList.contains(theme),
    { timeout: 10000 },
    variant.theme
  );

  await page.pdf({
    path: path.join(outDir, `documentation${variant.name}.pdf`),
    format: "A4",
    margin: {
      top: "0.4in",
      right: "0.4in",
      bottom: "0.4in",
      left: "0.4in"
    },
    printBackground: true
  });

  if (variant.theme === "dark") {
    await applyDarkBackground(path.join(outDir, `documentation${variant.name}.pdf`));
  }

  console.log(`${variant.theme} theme PDF generated successfully for ${pathname}`);
  await page.close();
}

await fs.mkdir(outDir, { recursive: true });
for (const url of urls) {
  for (const variant of variants) {
    await exportPdf(url, variant);
  }
}
await browser.close();
