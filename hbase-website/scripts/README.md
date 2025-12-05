# HBase Website Documentation Conversion Scripts

This directory contains scripts for converting HBase's AsciiDoc documentation to MDX format compatible with Fumadocs.

## Overview

The HBase documentation is originally written in AsciiDoc format (`.adoc` files) located in `app/pages/_docs/docs/ascii-docs/_chapters/`. These files are automatically converted to MDX (`.mdx` files) at build time using [Pandoc](https://pandoc.org/), a universal document converter.

## Why Pandoc?

Previously, the conversion process used:
- `@asciidoctor/core` to convert AsciiDoc → HTML
- `turndown` to convert HTML → Markdown

This approach had several limitations:
- Loss of semantic information during HTML intermediary step
- Manual handling of special elements (admonitions, code blocks, etc.)
- Inconsistent conversion quality
- Difficult to maintain custom conversion rules

**With Pandoc**, we get:
- Direct AsciiDoc → Markdown conversion (better quality)
- Industry-standard converter with excellent AsciiDoc support
- Extensible via Lua filters for custom transformations
- Better handling of tables, code blocks, and complex formatting
- Maintained by a large community

## Architecture

### 1. Main Conversion Script
**File**: `scripts/convert-adoc-to-mdx.ts`

This TypeScript script:
- Scans all `.adoc` files in the `ascii-docs/_chapters/` directory
- Uses Pandoc to convert each file to GitHub Flavored Markdown (GFM)
- Applies the Fumadocs Lua filter for component compatibility
- Adds MDX frontmatter (title, description)
- Post-processes the output for Fumadocs compatibility
- Outputs `.mdx` files to `app/pages/_docs/docs/_mdx/`

### 2. Pandoc Lua Filter
**File**: `scripts/fumadocs-filter.lua`

This Lua script runs during Pandoc conversion and:
- Converts AsciiDoc admonitions (NOTE, TIP, WARNING, etc.) to Fumadocs `<Callout>` components
- Ensures proper language tags for code blocks
- Generates proper anchor IDs for headings
- Handles special formatting for tables and links

### 3. Post-Processing
After Pandoc conversion, the TypeScript script:
- Converts remaining admonition patterns to `<Callout>` components
- Fixes code blocks without language tags
- Cleans up excessive whitespace
- Converts anchor links to proper format

## Prerequisites

### Required Software

1. **Node.js** (>= 22.12.0)
   ```bash
   node --version
   ```

2. **Pandoc** (>= 3.0 recommended)
   ```bash
   pandoc --version
   ```

### Installing Pandoc

#### macOS
```bash
brew install pandoc
```

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install pandoc
```

#### Windows
```bash
choco install pandoc
```

Or download from: https://pandoc.org/installing.html

## Usage

### Automatic Conversion (Recommended)

The conversion happens automatically before building or running the dev server:

```bash
# Development - converts docs then starts dev server
npm run dev

# Production build - converts docs then builds
npm run build
```

**Note**: Conversion runs in **quiet mode** during builds - it won't spam your console with per-file messages. If some files fail to convert, the build/dev server will still run successfully with the converted files.

### Manual Conversion

To convert documentation manually:

```bash
# Standard conversion (verbose output)
npm run convert:adoc

# Verbose mode (same as above)
npm run convert:adoc:verbose

# Debug mode (shows detailed error messages)
npm run convert:adoc:debug
```

This will:
1. Check if Pandoc and Asciidoctor are installed
2. Find all `.adoc` files in `ascii-docs/_chapters/`
3. Convert each file to MDX
4. Output results to `_mdx/` directory
5. Show a summary of successful and failed conversions

### Output

After conversion, you'll see a summary:

```bash
HBase AsciiDoc to MDX Converter (Asciidoctor + Pandoc)
==================================================
Using: Asciidoctor 2.0.26 [https://asciidoctor.org]
Using: pandoc 3.8.2.1

Found 52 AsciiDoc files to convert...

# ... conversion progress ...

==================================================

📊 Conversion Summary:
   ✓ Successfully converted: 50/52 files
   ⚠️  Failed to convert: 2 files

   Failed files:
     - developer.adoc
     - protobuf.adoc

   💡 Tip: Run with DEBUG=1 for detailed error messages
   💡 The dev server will still work with successfully converted files
```

**Key Points**:
- ✅ Failed conversions don't block the build/dev server
- ✅ The script exits with code 0 even if some files fail
- ✅ Successfully converted files are still usable
- ✅ Use `DEBUG=1` environment variable for detailed errors

## MDX Output Format

Each converted MDX file has the following structure:

```mdx
---
title: "Document Title"
description: "Optional description extracted from first paragraph"
---

## Heading

Content here...

<Callout type="info">
This is a note from the original AsciiDoc.
</Callout>

### Subheading

More content...
```

## Fumadocs Callout Types

AsciiDoc admonitions are converted to Fumadocs callouts:

| AsciiDoc | Fumadocs Callout | Usage |
|----------|------------------|-------|
| `NOTE:` | `type="info"` | General information |
| `TIP:` | `type="tip"` | Helpful suggestions |
| `IMPORTANT:` | `type="warn"` | Important information |
| `WARNING:` | `type="warning"` | Warnings and cautions |
| `CAUTION:` | `type="error"` | Critical warnings |

## Troubleshooting

### Pandoc Not Found

**Error**: `❌ Error: Pandoc is not installed!`

**Solution**: Install Pandoc using the instructions above.

### Conversion Errors

If specific files fail to convert:

1. **Don't panic!** The build/dev server will still work with successfully converted files
2. Check the summary at the end to see which files failed
3. Run with debug mode for details:
   ```bash
   npm run convert:adoc:debug
   ```
4. Common causes:
   - Malformed AsciiDoc syntax (mismatched formatting)
   - Missing include files
   - Invalid XML generated by Asciidoctor
5. Try converting the problematic file manually:
   ```bash
   asciidoctor -b docbook5 your-file.adoc -o output.xml
   pandoc --from=docbook --to=gfm output.xml
   ```

### Missing Output Files

If `.mdx` files aren't generated:

1. Verify input directory exists: `app/pages/_docs/docs/ascii-docs/_chapters/`
2. Verify output directory permissions
3. Check for TypeScript compilation errors
4. Run with verbose logging

### Code Blocks Without Syntax Highlighting

If code blocks don't have proper syntax highlighting:

1. Check if the original AsciiDoc specifies the language
2. Verify the Lua filter is being applied
3. Check post-processing rules in `convert-adoc-to-mdx.ts`

## Customization

### Adding Custom Conversions

To add custom conversion rules:

1. **For AST-level transformations**: Edit `fumadocs-filter.lua`
   - Modify existing filters
   - Add new Pandoc AST handlers

2. **For text-level transformations**: Edit `postProcessMarkdown()` in `convert-adoc-to-mdx.ts`
   - Add regex replacements
   - Handle special formatting

### Example: Adding a New Callout Type

1. In `fumadocs-filter.lua`, add to the type map:
   ```lua
   local function admonition_to_callout_type(admon_type)
     local type_map = {
       note = "info",
       tip = "tip",
       example = "success",  -- New type
       -- ...
     }
     return type_map[admon_type:lower()] or "info"
   end
   ```

2. In `convert-adoc-to-mdx.ts`, add post-processing:
   ```typescript
   processed = processed.replace(
     />\s*\*\*Example\*\*\s*\n>\s*\n>([\s\S]*?)(?=\n\n|$)/gi,
     (match, content) => {
       const cleanContent = content.replace(/^>\s*/gm, "").trim();
       return `<Callout type="success">\n${cleanContent}\n</Callout>\n`;
     }
   );
   ```

## Development Workflow

When modifying the conversion scripts:

1. Make changes to `convert-adoc-to-mdx.ts` or `fumadocs-filter.lua`
2. Test with a single file:
   ```bash
   npm run convert:adoc
   ```
3. Verify the output in `_mdx/` directory
4. Check the website renders correctly:
   ```bash
   npm run dev
   ```
5. Commit your changes

## CI/CD Integration

The conversion script is integrated into the build pipeline:

- **`prebuild`**: Runs before production builds
- **`predev`**: Runs before starting dev server

This ensures documentation is always up-to-date.

## Performance

Conversion performance on typical hardware:

- **52 files**: ~5-10 seconds
- **Single file**: ~0.1-0.2 seconds

Pandoc is highly optimized and handles large documents efficiently.

## Contributing

When contributing to the conversion scripts:

1. Test with all documentation files
2. Verify Fumadocs compatibility
3. Check for regressions in existing conversions
4. Update this README with any new features
5. Add comments for complex transformations

## Resources

- [Pandoc Manual](https://pandoc.org/MANUAL.html)
- [Pandoc Lua Filters](https://pandoc.org/lua-filters.html)
- [Fumadocs Documentation](https://fumadocs.vercel.app/)
- [AsciiDoc Syntax Quick Reference](https://docs.asciidoctor.org/asciidoc/latest/syntax-quick-reference/)
- [GitHub Flavored Markdown Spec](https://github.github.com/gfm/)

## License

Licensed under the Apache License, Version 2.0. See the license headers in each file.

