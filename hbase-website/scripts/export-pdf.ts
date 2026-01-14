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

const browser = await puppeteer.launch();
const outDir = "pdfs";
// The path
const urls = ["/docs/single-page"];

async function exportPdf(pathname: string) {
  const page = await browser.newPage();
  await page.goto("http://localhost:5173" + pathname, {
    waitUntil: "networkidle2"
  });

  await page.pdf({
    path: path.join(outDir, pathname.slice(1).replaceAll("/", "-") + ".pdf"),
    width: 950,
    printBackground: true
  });

  console.log(`PDF generated successfully for ${pathname}`);
  await page.close();
}

await fs.mkdir(outDir, { recursive: true });
await Promise.all(urls.map(exportPdf));
await browser.close();
