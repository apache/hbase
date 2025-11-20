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

Most pages (except the home page) store content in **Markdown (`.md`)** or **JSON (`.json`)** files located in `app/pages/[page-name]/`. This makes it easy to update content without touching code.

**Examples:**
- `app/pages/team/content.md` - Markdown content for team page
- `app/pages/powered-by-hbase/companies.json` - JSON data for companies
- `app/pages/news/events.json` - JSON data for news/events

Edit these files with any text editor, then run `npm run build` to regenerate the site.

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
  - Built on top of Radix UI primitives
  - Similar to a component library like PrimeFaces or Vaadin in Java
  - Provides buttons, cards, navigation menus, etc.
  - [Documentation](https://ui.shadcn.com/)

- **Radix UI** - Low-level, accessible UI primitives
  - The foundation that shadcn/ui builds upon
  - Handles complex accessibility (ARIA) requirements automatically
  - Think of it as the "Spring Framework" for UI components

#### Styling
- **TailwindCSS** - Utility-first CSS framework
  - Instead of writing CSS files, you apply classes directly in components
  - Example: `className="text-blue-500 font-bold"` makes blue, bold text

#### Code Quality Tools
- **TypeScript** - Typed superset of JavaScript
  - Similar to Java's type system
  - Catches errors at compile-time instead of runtime
  - Provides autocomplete and better IDE support
  
- **ESLint + Prettier** - Code linting and formatting (like Checkstyle + google-java-format)
  - ESLint analyzes code for potential errors and enforces coding standards
  - Prettier handles automatic code formatting (spacing, indentation, etc.)
  - Integrated together: `npm run lint:fix` handles both linting and formatting
  - Configuration: `eslint.config.js` and `prettier.config.js`

### Project Architecture

The project follows a clear directory structure with separation of concerns:

```
my-react-router-app/
├── app/                          # Application source code
│   ├── ui/                       # Reusable UI components (no business logic)
│   │   ├── button.tsx            # Generic button component
│   │   ├── card.tsx              # Card container component
│   │   └── ...                   # Other UI primitives
│   │
│   ├── components/               # Reusable components WITH business logic
│   │   ├── site-navbar.tsx       # Website navigation bar
│   │   ├── site-footer.tsx       # Website footer
│   │   ├── theme-toggle.tsx      # Dark/light mode toggle
│   │   └── markdown-layout.tsx   # Layout for markdown content pages
│   │
│   ├── pages/                    # Complete pages (composed of ui + components)
│   │   ├── home/                 # Home page
│   │   │   ├── index.tsx         # Main page component (exported)
│   │   │   ├── hero.tsx          # Hero section (not exported)
│   │   │   ├── features.tsx      # Features section (not exported)
│   │   │   └── ...
│   │   ├── team/                 # Team page
│   │   │   ├── index.tsx         # Main page component (exported)
│   │   │   └── content.md        # Markdown content
│   │   └── ...
│   │
│   ├── routes/                   # Route definitions and metadata
│   │   ├── home.tsx              # Home route configuration
│   │   ├── team.tsx              # Team route configuration
│   │   └── ...
│   │
│   ├── lib/                      # Utility functions and integrations
│   │   ├── utils.ts              # Helper functions
│   │   └── theme-provider.tsx    # Theme management
│   │
│   ├── routes.ts                 # Main routing configuration
│   ├── root.tsx                  # Root layout component
│   └── app.css                   # Global styles
│
├── build/                        # Generated files (DO NOT EDIT)
│   ├── client/                   # Browser-side assets
│   │   ├── index.html            # HTML files for each page
│   │   ├── assets/               # JavaScript, CSS bundles
│   │   └── images/               # Optimized images
│   └── server/                   # Server-side code (if using SSR)
│
├── public/                       # Static files (copied as-is to build/)
│   ├── favicon.ico               # Website icon
│   └── images/                   # Images and other static assets
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

### Getting Started

#### 1. Install Dependencies

Think of this as `mvn install`:

```bash
npm install
```

This downloads all required packages from npm (similar to Maven Central).

#### 2. Generate Developers Data

**Important:** Before starting the development server, generate the `developers.json` file from the root `pom.xml`:

```bash
npm run extract-developers
```

This extracts the developer information from the parent `pom.xml` file and creates `app/pages/team/developers.json`, which is required for the Team page to work properly. Re-run this command whenever the developers section in `pom.xml` changes.

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
3. **Check for errors** in the terminal where `npm run dev` is running

#### Common Tasks

**Add a new page:**
1. Create directory in `app/pages/my-new-page/`
2. Create `index.tsx` in that directory
3. Create route file in `app/routes/my-new-page.tsx`
4. Register route in `app/routes.ts`

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

The project uses [Vitest](https://vitest.dev/) for testing React components.

**Run tests:**
```bash
# Run tests in watch mode (for development)
npm test

# Run tests once (for CI/CD)
npm run test:run

# Run tests with UI
npm run test:ui
```

**Test coverage includes:**
- Home Page - Hero section, buttons, features, use cases, community sections
- Theme Toggle - Light/dark mode switching
- Navigation - Navbar, dropdown menus, links
- Markdown Rendering - Headings, lists, code blocks, tables, links

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

**CI/CD Workflow:**

Before merging or deploying, run the full CI pipeline:

```bash
npm run ci
```

This command runs all quality checks and builds the project:
1. `npm run lint` - Check linting
2. `npm run typecheck` - Check types
3. `npm run test:run` - Run tests
4. `npm run build` - Build for production

All checks must pass before code is considered ready for deployment.

**CI/CD Pipeline Example:**
```yaml
# Example for GitHub Actions, GitLab CI, etc.
- npm run ci  # Runs all checks and build
```

### Building for Production

Create an optimized production build:

```bash
npm run build
```

This command:
1. Compiles TypeScript to JavaScript
2. Bundles and minifies all code
3. Optimizes images and assets
4. Generates static HTML files
5. Outputs everything to `build/` directory

**Generated files location:**
```
build/
├── client/           # Everything needed for the website
│   ├── *.html        # Pre-rendered HTML pages
│   ├── assets/       # Optimized JavaScript and CSS
│   │   ├── *.js      # JavaScript bundles (minified)
│   │   ├── *.css     # Stylesheets (minified)
│   │   └── manifest-*.js  # Asset manifest
│   └── images/       # Optimized images
└── server/           # Server-side code (if applicable)
```

The `build/client/` directory contains everything needed to deploy the website to any static file host.

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
   - `npm run test:run` - Vitest unit tests
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

**Validate Configuration Only:**

To verify the Maven configuration without building:

```bash
cd hbase-website
mvn validate
```

#### Maven Lifecycle Phases

The frontend-maven-plugin binds to these Maven phases:

- **generate-resources**: Installs Node.js/npm and runs `npm install`
- **compile**: Runs `npm run ci` (lint, typecheck, test, build)

#### Build Artifacts

After a successful Maven build, you'll find:

```
hbase-website/
├── build/                    # Production build output
│   ├── client/               # Static website files
│   └── server/               # Server-side code (if applicable)
├── node_modules/             # npm dependencies (gitignored)
├── target/                   # Maven build directory (gitignored)
│   └── node/                 # Installed Node.js/npm (gitignored)
└── ...
```

#### Integration with CI/CD

The Maven configuration ensures consistent builds across different environments:

- **Local Development**: Developers can build with `mvn clean install`
- **CI/CD Pipelines**: Automated builds work out-of-the-box with Maven
- **No Manual Steps**: No need to manually run `npm install` or `npm run ci`

#### Maven Troubleshooting

**Build Fails During npm install:**

```bash
# Clean and rebuild
cd hbase-website
mvn clean install
```

**Build Fails During npm run ci:**

This usually indicates:
- ESLint errors (code quality issues)
- TypeScript type errors
- Failing unit tests
- Build configuration issues

To diagnose, run the commands directly:
```bash
cd hbase-website
npm install
npm run lint      # Check linting
npm run typecheck # Check types
npm run test:run  # Check tests
npm run build     # Check build
```

Fix any errors and try the Maven build again.

**Clean Everything:**

To completely reset the build environment, use Maven's clean phase which automatically removes `build/` and `node_modules/`:

```bash
cd hbase-website
mvn clean install
```

This will:
- Remove `build/` directory
- Remove `node_modules/` directory  
- Remove `target/` directory
- Reinstall Node.js and npm
- Install all dependencies fresh
- Run the full build pipeline

For a manual clean (if needed):
```bash
cd hbase-website
rm -rf node_modules/ build/ target/ .react-router/
mvn clean install
```

#### Configuration Files

- **pom.xml**: Maven configuration using frontend-maven-plugin
- **package.json**: npm scripts and dependencies
- **.gitignore**: Excludes `target/`, `node/`, `node_modules/`, `build/`

#### Benefits

✅ **Consistent Builds**: Same build process everywhere (local, CI, production)  
✅ **No Manual Steps**: Maven handles everything automatically  
✅ **Isolated Node.js**: Doesn't interfere with system Node installation  
✅ **Skip Option**: Can skip website build with `-DskipSite`  
✅ **Standalone**: Can build website separately with `-pl hbase-website`  
✅ **Quality Checks**: Runs linting, type checking, and tests before building  

#### For HBase Developers

If you're working on HBase but not the website:

```bash
# Skip website build to save time
mvn clean install -DskipSite
```

If you're working on the website:

```bash
# Use npm for faster development iteration
cd hbase-website
npm install
npm run dev      # Start dev server with hot reload

# Or use Maven for full CI pipeline
mvn clean install
```

### Deployment

#### Docker Deployment (most likely won't use in production)

Build and run using Docker:

```bash
# Build the Docker image
docker build -t hbase-website .

# Run the container
docker run -p 3000:3000 hbase-website
```

The website will be available at `http://localhost:3000`.

**Deploy to any platform supporting Docker:**
- AWS ECS
- Google Cloud Run
- Azure Container Apps
- Digital Ocean App Platform
- Fly.io
- Railway

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
   rm -rf build/ node_modules/.vite
   ```

2. **Reinstall dependencies:**
   ```bash
   rm -rf node_modules/
   npm install
   ```

3. **Try building again:**
   ```bash
   npm run build
   ```

#### Need to Clean Everything

Nuclear option - removes all generated files:

```bash
rm -rf node_modules/ build/ .react-router/
npm install
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