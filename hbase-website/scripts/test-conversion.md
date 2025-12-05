# Conversion Test Guide

This guide helps you test the AsciiDoc to MDX conversion.

## Quick Test

To test the conversion with a single file:

1. **Ensure Pandoc is installed**:
   ```bash
   pandoc --version
   ```
   
   If not installed:
   - macOS: `brew install pandoc`
   - Ubuntu: `sudo apt-get install pandoc`
   - Windows: `choco install pandoc`

2. **Run the conversion**:
   ```bash
   cd hbase-website
   npm run convert:adoc
   ```

3. **Check the output**:
   ```bash
   ls -la app/pages/_docs/docs/_mdx/
   cat app/pages/_docs/docs/_mdx/preface.mdx
   ```

## Expected Output

### Sample Input (preface.adoc)

```asciidoc
= Preface
:doctype: article
:numbered:

This is the official reference guide for HBase.

NOTE: This is an important note about the documentation.

.About This Guide
This reference guide is a work in progress.

[source,bash]
----
mvn site
----
```

### Expected Output (preface.mdx)

```mdx
---
title: "Preface"
description: "This is the official reference guide for HBase."
---

# Preface

This is the official reference guide for HBase.

<Callout type="info">
This is an important note about the documentation.
</Callout>

## About This Guide

This reference guide is a work in progress.

```bash
mvn site
```
```

## Testing Checklist

After running the conversion, verify:

- [ ] All `.adoc` files are converted to `.mdx`
- [ ] Frontmatter includes `title` and `description`
- [ ] Headings are properly formatted (##, ###, etc.)
- [ ] Code blocks have language tags
- [ ] Admonitions are converted to `<Callout>` components
- [ ] Links and anchor references work
- [ ] Tables are properly formatted
- [ ] No excessive whitespace

## Manual Test with Sample File

To test conversion of a single file manually:

```bash
# Convert one file directly with pandoc
pandoc \
  --from=asciidoc \
  --to=gfm \
  --wrap=preserve \
  --atx-headers \
  --standalone \
  --lua-filter=scripts/fumadocs-filter.lua \
  app/pages/_docs/docs/ascii-docs/_chapters/preface.adoc \
  -o /tmp/test-output.md

# View the output
cat /tmp/test-output.md
```

## Verification in Browser

1. Start the dev server:
   ```bash
   npm run dev
   ```

2. Navigate to a documentation page

3. Verify:
   - Content renders correctly
   - Callouts are styled properly
   - Code syntax highlighting works
   - Headings appear in table of contents
   - Links and anchors work

## Common Issues

### Issue: Pandoc not found
**Solution**: Install pandoc using the instructions above.

### Issue: Callouts not rendering
**Check**: 
- Lua filter is being applied
- MDX components are properly imported
- Fumadocs configuration is correct

### Issue: Code blocks without highlighting
**Check**:
- Language tags are present in output
- Syntax highlighting is enabled in Fumadocs
- Language is supported by highlight.js

## Performance Test

To measure conversion performance:

```bash
time npm run convert:adoc
```

Expected: 5-10 seconds for all files.

## Regression Test

When making changes to the conversion scripts:

1. Convert all files: `npm run convert:adoc`
2. Start dev server: `npm run dev`
3. Manually check several documentation pages
4. Verify no broken formatting or missing content
5. Check browser console for errors

## Automated Testing

Consider adding automated tests:

```typescript
// test/conversion.test.ts
import { describe, it, expect } from 'vitest';
import { execSync } from 'child_process';
import { readFileSync } from 'fs';

describe('AsciiDoc to MDX Conversion', () => {
  it('should convert all files successfully', () => {
    const output = execSync('npm run convert:adoc', { encoding: 'utf-8' });
    expect(output).toContain('✓ Conversion complete!');
  });

  it('should generate valid frontmatter', () => {
    const mdx = readFileSync('app/pages/_docs/docs/_mdx/preface.mdx', 'utf-8');
    expect(mdx).toMatch(/^---\ntitle: /);
  });

  it('should convert admonitions to callouts', () => {
    const mdx = readFileSync('app/pages/_docs/docs/_mdx/preface.mdx', 'utf-8');
    expect(mdx).toContain('<Callout type=');
  });
});
```


