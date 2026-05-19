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

import {
  isRouteErrorResponse,
  Links,
  Meta,
  Outlet,
  Scripts,
  ScrollRestoration
} from "react-router";

import type { Route } from "./+types/root";
import appStyles from "./app.css?url";
import "katex/dist/katex.css";
import { ThemeProvider } from "./lib/theme-provider";
import { Button } from "./ui/button";

export const links: Route.LinksFunction = () => [
  {
    rel: "preload",
    as: "font",
    href: "/fonts/inter-latin-wght-normal.woff2",
    type: "font/woff2",
    crossOrigin: "anonymous"
  },
  {
    rel: "prefetch",
    as: "font",
    href: "/fonts/inter-latin-wght-italic.woff2",
    type: "font/woff2",
    crossOrigin: "anonymous"
  },
  { rel: "stylesheet", href: appStyles }
];

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
        <script
          dangerouslySetInnerHTML={{
            __html: `
              (function() {
                const theme = localStorage.getItem('theme');
                const root = document.documentElement;
                root.classList.remove('light', 'dark');
                
                if (theme && ['light', 'dark'].includes(theme)) {
                  root.classList.add(theme);
                } else {
                  const systemTheme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
                  root.classList.add(systemTheme);
                  localStorage.setItem('theme', systemTheme);
                }
              })();
            `
          }}
        />
        <noscript>
          <style>{`.theme-toggle-wrapper { display: none !important; }`}</style>
        </noscript>
      </head>
      <body className="font-base">
        <ThemeProvider defaultTheme="light">
          {children}

          <ScrollRestoration />
          <Scripts />
        </ThemeProvider>
      </body>
    </html>
  );
}

export default function App() {
  return <Outlet />;
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  let eyebrow = "Error";
  let message = "Something went wrong";
  let details = "An unexpected error occurred.";
  let stack: string | undefined;

  if (isRouteErrorResponse(error)) {
    eyebrow = String(error.status);
    message = error.status === 404 ? "Page not found" : "Request failed";
    details =
      error.status === 404 ? "The requested page could not be found." : error.statusText || details;
  } else if (import.meta.env.DEV && error && error instanceof Error) {
    details = error.message;
    stack = error.stack;
  }

  return (
    <main className="grid min-h-screen place-items-center bg-[radial-gradient(circle_at_top,rgba(186,22,12,0.08),transparent_32rem)] px-4 py-16">
      <section
        className="mx-auto flex w-full max-w-2xl flex-col items-center text-center"
        aria-labelledby="error-title"
      >
        <img className="mb-8 h-auto w-36" src="/images/logo.svg" alt="Apache HBase" />
        <p className="text-muted-foreground text-sm font-semibold tracking-[0.3em] uppercase">
          {eyebrow}
        </p>
        <h1
          id="error-title"
          className="mt-4 text-4xl font-semibold tracking-tight text-balance md:text-6xl"
        >
          {message}
        </h1>
        <p className="text-muted-foreground mt-5 max-w-xl text-lg leading-8 text-pretty md:text-xl">
          {details}
        </p>
        <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
          <Button asChild size="lg">
            <a href="/">Go back home</a>
          </Button>
          <Button asChild variant="outline" size="lg">
            <a href="/docs/">Read documentation</a>
          </Button>
        </div>
        {stack && (
          <pre className="bg-muted/50 text-muted-foreground border-border mt-8 max-h-80 w-full overflow-x-auto rounded-lg border p-4 text-left text-sm">
            <code>{stack}</code>
          </pre>
        )}
      </section>
    </main>
  );
}
