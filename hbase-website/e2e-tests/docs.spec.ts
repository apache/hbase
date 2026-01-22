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

test.describe("Docs", () => {
  test("docs page loads with content", async ({ page }) => {
    await page.goto("/docs", { waitUntil: "domcontentloaded" });

    // Wait for the page to be fully loaded
    await page.waitForLoadState("load");

    // Wait for any content to appear (give it time to hydrate)
    await page.waitForTimeout(1000);

    // The page should have rendered content
    const body = page.locator("body");
    await expect(body).toBeVisible();

    // Check that we're on the docs page
    await expect(page).toHaveURL(/.*docs/);
  });

  test("docs page has content and text", async ({ page }) => {
    await page.goto("/docs", { waitUntil: "domcontentloaded" });

    // Wait for page to be fully loaded
    await page.waitForLoadState("load");
    await page.waitForTimeout(1000);

    // Check that the page has text content
    const bodyText = await page.locator("body").textContent();
    expect(bodyText).toBeTruthy();
    expect(bodyText!.length).toBeGreaterThan(0);
  });
});
