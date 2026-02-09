<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache HBase Website

The official website for Apache HBase, built with modern web technologies to provide a fast, accessible, and maintainable web presence.

---

## Table of Contents

- [Content Editing](#content-editing)
- [Development](#development)
  - [Prerequisites](#prerequisites)
  - [Technology Stack](#technology-stack)
  - [Project Architecture](#project-architecture)
  - [Getting Started](#getting-started)
  - [Development Workflow](#development-workflow)
  - [Building for Production](#building-for-production)
  - [Maven Integration](#maven-integration)
  - [Deployment](#deployment)
  - [Troubleshooting](#troubleshooting)

---

## Content Editing

Most landing pages store content in **Markdown (`.md`)** or **JSON (`.json`)** files located in `app/pages/_landing/[page-name]/`. Docs content lives under `app/pages/_docs/` and is authored in MDX.

Legacy documentation is preserved for those users who have old bookmarked links and notes: the old book lives at `/public/book.html`, and its static assets are in `public/old-book-static-files/`.

**Examples:**

- `app/pages/_landing/team/content.md` - Markdown content for team page
- `app/pages/_landing/powered-by-hbase/companies.json` - JSON data for companies
- `app/pages/_landing/news/events.json` - JSON data for news/events
- `app/pages/_docs/docs/_mdx/(multi-page)/...` - MDX content for documentation

---

## Development

### Prerequisites

Before you begin, ensure you have the following installed:

- **Node.js version 22** - JavaScript runtime (like the JVM for Java)
  - Download from [nodejs.org](https://nodejs.org/)
  - Verify installation: `node --version` (should show v20.19+ or v22.12+)

- **NPM** - Node Package Manager (like Maven for Java)
  - Comes bundled with Node.js
  - Verify installation: `npm --version`

### Technology Stack

This website uses modern web technologies. Here's what each one does (with Java analogies):

#### Core Framework

- **React Router** - Full-stack web framework with Server-Side Generation (SSG)
  - Handles routing (like Spring MVC controllers)
  - Provides server-side rendering for better performance and SEO
  - Enables progressive enhancement (see below)
  - [Documentation](https://reactrouter.com/)

#### Documentation Framework

- **Fumadocs** - Documentation framework used for the docs section
  - Provides MDX-based docs structure and navigation
  - Lives alongside the landing pages in the same React Router app
  - Supports multi-page and single-page docs from the same MDX sources
  - [Documentation](https://fumadocs.com/)

#### Progressive Enhancement

The website uses **progressive enhancement** ([learn more](https://reactrouter.com/explanation/progressive-enhancement)), which means:

- **With JavaScript enabled**: Users get a Single Page Application (SPA) experience
  - Fast page transitions without full page reloads
  - Smooth animations and interactive features
  - Enhanced user experience

- **Without JavaScript**: Users still get a fully functional website
  - All links and forms work via traditional HTML
  - Content is accessible to everyone
  - Better for search engines and accessibility tools

This approach ensures the website works for all users, regardless of their browser capabilities or connection speed.

#### UI Components

- **shadcn/ui** - Pre-built, accessible UI components
  - Similar to a component library like PrimeFaces or Vaadin in Java
  - Provides buttons, cards, navigation menus, etc.
  - [Documentation](https://ui.shadcn.com/)

#### Styling

- **TailwindCSS** - Utility-first CSS framework, aka Bootstrap on steroids
  - Instead of writing CSS files, you apply classes directly in components
  - Example: `className="text-blue-500 font-bold"` makes blue, bold text

#### Code Quality Tools

- **TypeScript** - Typed superset of JavaScript
  - Similar to Java's type system
  - Catches errors at compile-time instead of runtime
  - Provides autocomplete and better IDE support

- **ESLint + Prettier** - Code linting and formatting (like Checkstyle)
  - ESLint analyzes code for potential errors and enforces coding standards
  - Prettier handles automatic code formatting (spacing, indentation, etc.)
  - Integrated together: `npm run lint:fix` handles both linting and formatting
  - Configuration: `eslint.config.js` and `prettier.config.js`

### Project Architecture

The project follows a clear directory structure with separation of concerns:

```
my-react-router-app/
├── app/                               # Application source code
│   ├── ui/                            # Reusable UI components (no business logic)
│   │   ├── button.tsx                 # Generic button component
│   │   ├── card.tsx                   # Card container component
│   │   └── ...                        # Other UI primitives
│   │
│   ├── components/                    # Reusable components WITH business logic
│   │   ├── site-navbar.tsx            # Website navigation bar
│   │   ├── site-footer.tsx            # Website footer
│   │   ├── theme-toggle.tsx           # Dark/light mode toggle
│   │   └── markdown-layout.tsx        # Layout for markdown content pages
│   │
│   ├── pages/                         # Complete pages (composed of ui + components)
│   │   ├── _landing/                  # Landing pages + layout
│   │   │   ├── home/                  # Home page
│   │   │   │   ├── index.tsx          # Main page component (exported)
│   │   │   │   ├── hero.tsx           # Hero section (not exported)
│   │   │   │   ├── features.tsx       # Features section (not exported)
│   │   │   │   └── ...
│   │   │   ├── team/                  # Landing page content
│   │   │   └── ...
│   │   ├── _docs/                     # Documentation (Fumadocs)
│   │   │   ├── docs/                  # MDX content and structure
│   │   │   ├── docs-layout.tsx        # Fumadocs layout wrapper
│   │   │   └── ...
│   │
│   ├── routes/                        # Route definitions and metadata
│   │   ├── home.tsx                   # Home route configuration
│   │   ├── team.tsx                   # Team route configuration
│   │   └── ...
│   │
│   ├── lib/                           # Utility functions and integrations
│   │   ├── utils.ts                   # Helper functions
│   │   └── theme-provider.tsx         # Theme management
│   │
│   ├── routes.ts                      # Main routing configuration
│   ├── root.tsx                       # Root layout component
│   └── app.css                        # Global styles
│
├── build/                        # Generated files (DO NOT EDIT)
│   ├── client/                   # Browser-side assets
│   │   ├── index.html            # HTML files for each page
│   │   ├── assets/               # JavaScript, CSS bundles
│   │   └── images/               # Optimized images
│
├── public/                       # Static files (copied as-is to build/)
│   ├── favicon.ico               # Website icon
│   ├── images/                   # Image assets
|   └── ...
│
├── node_modules/                 # Dependencies (like Maven's .m2 directory)
├── package.json                  # Project metadata and dependencies (like pom.xml)
├── tsconfig.json                 # TypeScript configuration
└── react-router.config.ts        # React Router framework configuration
```

#### Key Principles

1. **UI Components (`/ui`)**: Pure, reusable components with no business logic
   - Can be used anywhere in the application
   - Only concerned with appearance and basic interaction

2. **Business Components (`/components`)**: Reusable across pages
   - May contain business logic specific to HBase website
   - Examples: navigation, footer, theme toggle

3. **Pages (`/pages`)**: Complete pages combining ui and components
   - Each page has its own directory
   - Only `index.tsx` is exported
   - Internal components stay within the page directory
   - If a component needs to be reused, move it to `/components`

4. **Routes (`/routes`)**: Define routing and metadata
   - Maps URLs to pages
   - Sets page titles, meta tags, etc.

5. **Two Layout Systems in One App**:
   - **Landing pages** live under `app/pages/_landing/` and use the landing layout.
   - **Docs pages** live under `app/pages/_docs/` and use Fumadocs layouts.
   - Both are part of the same React Router application, but render with different layouts and visual styles.

6. **Documentation Versions**:
   - **Multi-page docs** live under `app/pages/_docs/docs/_mdx/(multi-page)/` and are the source of truth.
   - **Single-page docs** live under `app/pages/_docs/docs/_mdx/single-page/` and import content from the multi-page docs.

#### Important Conventions

##### Custom Link Component

**Always use the custom Link component from `@/components/link` instead of importing Link directly from `react-router`.**

The HBase website includes pages that are not part of this React Router application (e.g., documentation pages, API docs). The custom Link component automatically determines whether a link should trigger a hard reload or use React Router's client-side navigation:

**Usage:**

```typescript
// ✅ CORRECT - Use custom Link component
import { Link } from "@/components/link";

export const MyComponent = () => (
  <Link to="/team">Team</Link>
);
```

```typescript
// ❌ WRONG - Do not import Link from react-router
import { Link } from "react-router";

export const MyComponent = () => (
  <Link to="/team">Team</Link>
);
```

The ESLint configuration includes a custom rule (`custom/no-react-router-link`) that will throw an error if you attempt to import `Link` from `react-router`, helping enforce this convention automatically.

### Getting Started

#### 1. Install Dependencies

Think of this as `mvn install`:

```bash
npm install
```

This downloads all required packages from npm (similar to Maven Central).

#### 2. Generate Developers and Config Data

**Important:** Before starting the development server, generate the `developers.json` file from the root `pom.xml`:

```bash
npm run extract-developers
```

This extracts the developer information from the parent `pom.xml` file and creates `app/pages/team/developers.json`, which is required for the Team page to work properly. Re-run this command whenever the developers section in `pom.xml` changes. The output json is ignored by git, and this command also runs at a build time, so there is no need to `git commit` the generated file.

**Important:** Generate the HBase configuration markdown before starting the development server:

```bash
npm run extract-hbase-config
```

This extracts data from `hbase-default.xml` and creates `app/pages/_docs/docs/_mdx/(multi-page)/configuration/hbase-default.md`, which is required for the documentation page to work properly. Re-run this command whenever `hbase-default.xml` changes. The generated markdown is ignored by git, and this command also runs at build time, so there is no need to `git commit` the generated file.

**Important:** Generate the HBase version metadata before starting the development server:

```bash
npm run extract-hbase-version
```

This extracts the `<revision>` value from the root `pom.xml` and creates `app/lib/export-pdf/hbase-version.json`, which is used on the docs PDF cover. Re-run this command whenever the root `pom.xml` version changes. The generated json is ignored by git, and this command also runs at build time, so there is no need to `git commit` the generated file.

#### 3. Start Development Server

```bash
npm run dev
```

This starts a local development server with:

- **Hot Module Replacement (HMR)**: Code changes appear instantly without full page reload
- **Live at**: `http://localhost:5173`

### Development Workflow

#### Making Changes

1. **Edit code** in the `app/` directory
2. **Save the file** - changes appear automatically in the browser
3. **Check for errors** in the terminal where `npm run dev` is running and in browser console

#### Common Tasks

**Add a new page:**

1. Create directory in `app/pages/my-new-page/`
2. Create `index.tsx` in that directory
3. Create route file in `app/routes/my-new-page.tsx`
4. Register route in `app/routes.ts`

**Add a new documentation page:**

1. Create a new `.mdx` file in `app/pages/_docs/docs/_mdx/(multi-page)/` (for example `my-topic.mdx`).
2. Add the new file to the relevant `meta.json` in the same section folder so it appears in navigation.
3. Import the page into `app/pages/_docs/docs/_mdx/single-page/index.mdx` and add an `#` header so it renders in the single-page docs.

**Update content:**

- Edit the appropriate `.md` or `.json` file
- Changes appear automatically

**Add a UI component:**

- Check if shadcn/ui has what you need first
- Only create custom components if necessary

**Check code quality:**

```bash
npm run lint
```

**Fix linting and formatting issues:**

```bash
npm run lint:fix
```

### Testing

The project uses [Vitest](https://vitest.dev/) and [Playwright](http://playwright.dev/) for testing. Vitest is for unit testing, while Playwright is for e2e testing.

#### Export Documentation PDF

The docs PDF export is implemented as a Playwright e2e test in `e2e-tests/export-pdf.spec.ts`. It runs during `npm run ci` and generates static PDF assets for the documentation by rendering the single-page docs in both light and dark themes (HTML → PDF).

The export quality depends heavily on the `@media print` styles defined in `app/app.css`, which control layout, pagination, and print-only behavior.

There is also a dedicated command you can run manually when needed:

```bash
npm run export-pdf
```

This command is not part of the CI pipeline and does not run automatically unless invoked directly.

**Run tests:**

```bash
# Run all tests
npm test

# Run unit tests once (for CI/CD)
npm run test:run

# Run unit tests with UI
npm run test:ui

# Run e2e tests
npm run test:e2e

# Run e2e tests with UI
npm run test:e2e:ui
```

**Writing new tests:**

Use the `renderWithProviders` utility in `test/utils.tsx` to ensure components have access to routing and theme context:

```typescript
import { renderWithProviders, screen } from './utils'
import { MyComponent } from '@/components/my-component'

describe('MyComponent', () => {
  it('renders correctly', () => {
    renderWithProviders(<MyComponent />)
    expect(screen.getByText('Hello World')).toBeInTheDocument()
  })
})
```

### Building for Production

**CI/CD Workflow:**

Before merging or deploying, run the full CI pipeline:

```bash
npm run ci
```

This command runs all quality checks and builds the project. All checks must pass before code is considered ready.

Generated files are located under the `build/` directory.

### Maven Integration

The website is integrated with the Apache HBase Maven build system using the `frontend-maven-plugin`. This allows the website to be built as part of the main HBase build or separately using Maven commands.

#### What Gets Executed

When you run the Maven build, it automatically:

1. **Cleans previous build artifacts** (when using `mvn clean`)
   - Removes `build/` directory
   - Removes `node_modules/` directory
   - Ensures a fresh build environment

2. **Installs Node.js v22.20.0 and npm 11.6.2** (if not already available)
   - Installed to `target/` directory
   - Does not affect your system Node/npm installation

3. **Runs `npm install`** to install all dependencies
   - Reads from `package.json`
   - Installs to `node_modules/`

4. **Runs `npm run ci`** which executes:
   - `npm run lint` - ESLint code quality checks
   - `npm run typecheck` - TypeScript type checking
   - `npm run extract-developers` - Extract developers from parent pom.xml
   - `npm run extract-hbase-config` - Extract data from `hbase-default.xml` to `app/pages/_docs/docs/_mdx/(multi-page)/configuration/hbase-default.md`
   - `npm run extract-hbase-version` - Extract version from root `pom.xml` to `app/lib/export-pdf/hbase-version.json`
   - `npm run test:unit:run` - Vitest unit tests
   - `npm run test:e2e` - Playwright e2e tests
   - `npm run build` - Production build

5. **Build Output**: Generated files are in `build/` directory

#### Maven Commands

**Build Website with Full HBase Build:**

```bash
# From HBase root directory
mvn clean install
```

The website will be built automatically as part of the full build.

**Build Website Only:**

```bash
# Option 1: From HBase root directory
mvn clean install -pl hbase-website

# Option 2: From hbase-website directory
cd hbase-website
mvn clean install
```

**Skip Website Build:**

If you want to build HBase but skip the website:

```bash
# From HBase root directory
mvn clean install -DskipSite
```

### Deployment

#### Static Hosting

Since this site uses Static Site Generation (SSG), you can deploy the `build/client/` directory to any static file host:

- **Apache HTTP Server**: Copy `build/client/` contents to your web root
- **Nginx**: Copy `build/client/` contents to your web root
- **GitHub Pages**: Push `build/client/` to `gh-pages` branch
- **Netlify/Vercel**: Connect your repository for automatic deployments
- **AWS S3 + CloudFront**: Upload `build/client/` to S3 bucket

### Troubleshooting

#### TypeScript Types Are Broken

If you see type errors related to React Router's `+types`, regenerate them:

```bash
npx react-router typegen
```

#### Port Already in Use

If `npm run dev` fails because port 5173 is in use:

```bash
# Kill the process using the port
lsof -ti:5173 | xargs kill -9

# Or change the port in vite.config.ts
```

#### Build Fails

1. **Clear generated files:**

   ```bash
   rm -rf build/ node_modules/ .vite/ .react-router/ .source/
   ```

2. **Reinstall dependencies:**

   ```bash
   npm i
   ```

3. **Try building again:**
   ```bash
   npm run build
   ```

---

## Additional Resources

- **React Router Documentation**: https://reactrouter.com/
- **Progressive Enhancement Explained**: https://reactrouter.com/explanation/progressive-enhancement
- **shadcn/ui Components**: https://ui.shadcn.com/
- **TailwindCSS Docs**: https://tailwindcss.com/
- **TypeScript Handbook**: https://www.typescriptlang.org/docs/

---

Built with ❤️ for the Apache HBase community.
