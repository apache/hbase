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

test.describe("Landing Page", () => {
  test("page loads without errors", async ({ page }) => {
    const response = await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("load");
    expect(response?.status()).toBe(200);
  });

  test("page has content", async ({ page }) => {
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("load");

    // Check that the page has text content
    const bodyText = await page.locator("body").textContent();
    expect(bodyText).toBeTruthy();
    expect(bodyText!.length).toBeGreaterThan(100);
  });

  test("has links to main sections", async ({ page }) => {
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("load");

    // Check for a link to docs
    const docsLink = page.locator('a[href*="docs"]');
    const count = await docsLink.count();

    // Should have at least one link to docs
    expect(count).toBeGreaterThan(0);
  });

  test("footer is present", async ({ page }) => {
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("load");

    // Look for footer
    const footer = page.locator("footer").first();
    await expect(footer).toBeVisible();
  });

  test("can navigate to downloads page", async ({ page }) => {
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("load");

    // Look for downloads link
    const downloadsLink = page.getByRole("link", { name: /download/i });
    const count = await downloadsLink.count();

    if (count > 0) {
      await downloadsLink.first().click();
      await page.waitForLoadState("load");
      await expect(page).toHaveURL(/.*download/);
    }
  });

  test("can navigate to team page", async ({ page }) => {
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("load");

    // Look for team link
    const teamLink = page.getByRole("link", { name: /team/i });
    const count = await teamLink.count();

    if (count > 0) {
      await teamLink.first().click();
      await page.waitForLoadState("load");
      await expect(page).toHaveURL(/.*team/);
    }
  });
});
