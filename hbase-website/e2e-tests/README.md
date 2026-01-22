# E2E Tests

This directory contains end-to-end tests for the HBase website using Playwright.

## Running Tests

```bash
# Run all e2e tests
npm run test:e2e

# Run tests with UI mode (interactive)
npm run test:e2e:ui

# Run tests in headed mode (see the browser)
npm run test:e2e:headed

# Debug tests
npm run test:e2e:debug

# Run all tests (unit + e2e)
npm run test
```

## Configuration

The Playwright configuration is in `playwright.config.ts` at the root of the project.

- Tests run against `http://localhost:5173`
- The dev server is automatically started before tests run
- Tests run in parallel by default
- Three browsers are configured: Chromium, Firefox, and WebKit

## Writing New Tests

1. Create a new `.spec.ts` file in this directory
2. Import test utilities: `import { test, expect } from "@playwright/test";`
3. Group related tests using `test.describe()`
4. Use `test()` for individual test cases
5. Follow the existing patterns for consistency

## Tips

- Use `page.goto("/")` instead of full URLs (baseURL is configured)
- Group related tests with `test.describe()`
- Use meaningful test descriptions
- Check for visibility before interacting with elements
- Add the Apache license header to all new test files

