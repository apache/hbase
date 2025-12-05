# Quick Start Guide - Documentation Conversion

Get started with the new Pandoc-based documentation conversion system in under 5 minutes!

## 🚀 Quick Setup

### 1. Install Pandoc

**macOS:**
```bash
brew install pandoc
```

**Ubuntu/Debian:**
```bash
sudo apt-get update && sudo apt-get install pandoc
```

**Windows:**
```bash
choco install pandoc
```

### 2. Verify Installation

```bash
pandoc --version
# Should show: pandoc 3.x.x or similar
```

Or use our helper script:
```bash
./scripts/check-pandoc.sh
```

### 3. Convert Documentation

```bash
cd hbase-website
npm run convert:adoc
```

You should see:
```
HBase AsciiDoc to MDX Converter (using Pandoc)
==================================================
Using: pandoc 3.1.11

Found 52 AsciiDoc files to convert...

Converting preface.adoc...
✓ Created preface.mdx
...

✓ Conversion complete! Successfully converted 52/52 files.
```

### 4. Start Development

```bash
# Converts docs automatically, then starts dev server
npm run dev
```

### 5. Build for Production

```bash
# Converts docs automatically, then builds
npm run build
```

## ✅ That's It!

Your documentation is now converted and ready to use!

## 📚 What Happens During Conversion?

```
AsciiDoc Files → Pandoc → Post-Process → MDX Files
    (.adoc)                               (.mdx)
    
    ✓ Preserves structure
    ✓ Converts admonitions to Callouts
    ✓ Adds proper code highlighting
    ✓ Generates frontmatter
    ✓ Fumadocs-compatible output
```

## 🎯 Key Commands

| Command | Description |
|---------|-------------|
| `npm run convert:adoc` | Manually convert all docs |
| `npm run dev` | Convert + start dev server |
| `npm run build` | Convert + build for production |
| `./scripts/check-pandoc.sh` | Check if Pandoc is installed |

## 📂 File Locations

- **Input**: `app/pages/_docs/docs/ascii-docs/_chapters/*.adoc`
- **Output**: `app/pages/_docs/docs/_mdx/*.mdx`
- **Scripts**: `scripts/convert-adoc-to-mdx.ts`
- **Filter**: `scripts/fumadocs-filter.lua`

## 🔍 Verify Conversion Quality

1. **Check a converted file**:
   ```bash
   cat app/pages/_docs/docs/_mdx/preface.mdx
   ```

2. **Look for**:
   - Frontmatter with title and description
   - `<Callout>` components for admonitions
   - Code blocks with language tags
   - Proper heading structure

3. **View in browser**:
   ```bash
   npm run dev
   # Visit http://localhost:5173/docs/preface
   ```

## 🆘 Troubleshooting

### Pandoc Not Found?
```bash
# Install it first (see step 1 above)
brew install pandoc  # macOS
```

### Conversion Fails?
```bash
# Check for errors in specific file
pandoc --from=asciidoc --to=gfm \
  app/pages/_docs/docs/ascii-docs/_chapters/your-file.adoc
```

### Need Help?
- Read: `scripts/README.md` - Full documentation
- Examples: `scripts/EXAMPLE-CONVERSION.md` - Before/after examples
- Testing: `scripts/test-conversion.md` - Test procedures
- Overview: `DOCUMENTATION-CONVERSION.md` - Complete system overview

## 🎉 What's New?

✨ **Pandoc-powered conversion** - Industry-standard quality  
✨ **Fumadocs-compatible** - Works seamlessly with your docs  
✨ **Automatic conversion** - Runs before build/dev  
✨ **Better quality** - Preserves semantic information  
✨ **Easy to customize** - Via Lua filters  
✨ **Fast** - ~0.15s per file  

## 📖 Learn More

For comprehensive documentation, see:
- `scripts/README.md` - Full usage guide
- `DOCUMENTATION-CONVERSION.md` - Complete system overview
- `scripts/EXAMPLE-CONVERSION.md` - Conversion examples

---

**Happy documenting!** 🚀


