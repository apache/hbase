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

import { docs } from "@/.source";
import { toClientRenderer } from "fumadocs-mdx/runtime/vite";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import {
  DocsBody as FumaDocsBody,
  DocsDescription as FumaDocsDescription,
  DocsPage as FumaDocsPage,
  DocsTitle as FumaDocsTitle
} from "fumadocs-ui/page";
import defaultMdxComponents from "fumadocs-ui/mdx";
import type * as PageTree from "fumadocs-core/page-tree";
import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { PageLastUpdate } from "fumadocs-ui/layouts/docs/page";
import { useParams } from "react-router";
import { getGithubLastEdit } from "fumadocs-core/content/github";
import { useEffect, useState } from "react";
import { getPageTreePeers } from "fumadocs-core/page-tree";
import { Card, Cards } from "fumadocs-ui/components/card";
import { Step, Steps } from "fumadocs-ui/components/steps";
import { Link } from "@/components/link";
import type { MDXComponents } from "mdx/types";

// Extend default MDX components to include Steps globally
// Note: We'll override the 'a' component in the renderer to handle route-specific logic
const baseMdxComponents: MDXComponents = {
  ...defaultMdxComponents,
  p: (props) => <p className="wrap-anywhere" {...props} />,
  Step,
  Steps
};

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <div className="flex items-center gap-2">
          <img src="/favicon.ico" alt="HBase favicon" width={16} height={16} />
          <p>Apache HBase</p>
        </div>
      )
    },
    githubUrl: "https://github.com/apache/hbase/"
  };
}

type DocsLoaderData = { path: string; url: string; tree: unknown };

const renderer = toClientRenderer(
  docs.doc,
  ({ toc, default: Mdx, frontmatter }, { tree }: { tree: PageTree.Root }) => {
    const route = useParams()["*"];

    // Filter TOC: only H1 (depth: 1) for single-page, all headings for other pages
    const isSinglePage = route?.startsWith("single-page");
    const filteredToc = isSinglePage ? toc.filter((item: any) => item.depth === 1) : toc;

    // Handle hash navigation for single-page after content loads
    useEffect(() => {
      if (!isSinglePage) return;

      const hash = window.location.hash;
      if (!hash) return;

      const targetId = hash.substring(1);

      // Use MutationObserver to wait for the element to exist
      const observer = new MutationObserver(() => {
        const element = document.getElementById(targetId);
        if (element) {
          observer.disconnect();
          element.scrollIntoView({ behavior: "smooth", block: "start" });
        }
      });

      // Check if element already exists
      const element = document.getElementById(targetId);
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
      } else {
        // Watch for DOM changes until element appears
        observer.observe(document.body, {
          childList: true,
          subtree: true
        });
      }

      return () => observer.disconnect();
    }, [isSinglePage]);

    const grouppedRoutes = [
      "configuration",
      "upgrading",
      "security",
      "architecture",
      "backup-restore",
      "operational-management",
      "building-and-developing"
    ];
    const trimmedRoute = route?.endsWith("/") ? route?.slice(0, -1) : route;
    const isGrouppedRoute = !!trimmedRoute && grouppedRoutes.includes(trimmedRoute);

    const [lastModifiedTime, setLastModifiedTime] = useState<Date | undefined>(undefined);

    useEffect(() => {
      getGithubLastEdit({
        owner: "apache",
        repo: "hbase",
        // file path in Git
        path: `hbase-website/app/pages/_docs/docs/_mdx/${route}.mdx`
      }).then((value) => {
        if (value) {
          setLastModifiedTime(value);
        }
      });
    }, [route]);

    // Custom link component that transforms /docs/ links to anchors on single-page
    const CustomLink = ({ href, children, ...rest }: any) => {
      let transformedHref = href;

      // Transform internal /docs/ links to single-page anchors when on single-page route
      if (isSinglePage && href?.startsWith("/docs/") && !href.startsWith("/docs/single-page")) {
        // Convert /docs/configuration/basic-prerequisites to /docs/single-page#basic-prerequisites
        // Extract the last segment as the anchor
        const segments = href.replace("/docs/", "").split("/");
        const anchor = segments[segments.length - 1];
        transformedHref = `/docs/single-page#${anchor}`;
      }

      // Use default Link component for all links (external links are handled by Link component)
      return (
        <Link to={transformedHref ?? "#"} {...rest}>
          {children}
        </Link>
      );
    };

    // Merge custom link component with base components
    const mdxComponents = {
      ...baseMdxComponents,
      a: CustomLink
    };

    return (
      <FumaDocsPage toc={filteredToc} tableOfContent={{ style: "clerk" }}>
        <title>{frontmatter.title}</title>
        <meta name="description" content={frontmatter.description} />
        <FumaDocsTitle>{frontmatter.title}</FumaDocsTitle>
        <FumaDocsDescription>{frontmatter.description}</FumaDocsDescription>
        <FumaDocsBody>
          <Mdx components={mdxComponents} />
        </FumaDocsBody>

        {route && (
          <div className="mt-10 flex flex-col gap-10">
            {/* table of content for groupped routes */}
            {isGrouppedRoute && (
              <div className="flex flex-col gap-4">
                <p>In this section:</p>
                <Cards>
                  {getPageTreePeers(tree, `/docs/${trimmedRoute}`).map((peer) => (
                    <Card key={peer.url} title={peer.name} href={peer.url}>
                      {peer.description}
                    </Card>
                  ))}
                </Cards>
              </div>
            )}

            {/* TODO: check this link */}
            <div className="flex items-end gap-3">
              <a
                href={`https://github.com/apache/hbase/hbase-website/app/pages/_docs/docs/_mdx/${route}.mdx`}
                rel="noreferrer noopener"
                target="_blank"
                className="text-fd-secondary-foreground bg-fd-secondary hover:text-fd-accent-foreground hover:bg-fd-accent w-fit rounded-xl border p-2 text-sm font-medium transition-colors"
              >
                Edit on GitHub
              </a>

              {lastModifiedTime && <PageLastUpdate date={lastModifiedTime} />}
            </div>
          </div>
        )}
      </FumaDocsPage>
    );
  }
);

export function DocsPage({ loaderData }: { loaderData: DocsLoaderData }) {
  const { tree, path } = loaderData;
  const Content = renderer[path];

  // Check if we're on single-page route
  const isSinglePage = path.includes("single-page");

  const layoutOptions = isSinglePage
    ? {
        ...baseOptions(),
        sidebar: {
          banner: (
            <div className="px-4 py-2">
              <p className="text-fd-muted-foreground text-center text-xs leading-relaxed">
                You're viewing a single-page documentation. All content is merged into one
                continuous page. You can switch to a multi-page version by using the dropdown above.
              </p>
            </div>
          )
        }
      }
    : baseOptions();

  useEffect(() => {
    if (!isSinglePage) return;

    const links = document.querySelectorAll<HTMLAnchorElement>("#nd-sidebar a");

    for (const a of links) {
      if (
        a.textContent?.trim().includes("Single-Page Documentation") ||
        a.getAttribute("href") === "/docs/single-page"
      ) {
        // Hide instead of remove - React can still clean it up properly
        a.style.display = "none";
      }
    }

    // Cleanup: restore visibility when unmounting
    return () => {
      const links = document.querySelectorAll<HTMLAnchorElement>("#nd-sidebar a");
      for (const a of links) {
        if (
          a.textContent?.trim().includes("Single-Page Documentation") ||
          a.getAttribute("href") === "/docs/single-page"
        ) {
          a.style.display = ""; // Restore original display
        }
      }
    };
  }, [isSinglePage]);

  return (
    <DocsLayout
      {...layoutOptions}
      sidebar={{ className: "w-auto" }}
      tree={tree as PageTree.Root}
      searchToggle={{ enabled: !isSinglePage }}
    >
      <Content tree={tree as PageTree.Root} />
    </DocsLayout>
  );
}
