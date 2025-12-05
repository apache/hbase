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

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: "Apache HBase"
    },
    githubUrl: "https://github.com/apache/hbase/"
  };
}

type DocsLoaderData = { path: string; url: string; tree: unknown };

const renderer = toClientRenderer(docs.doc, ({ toc, default: Mdx, frontmatter }) => {
  const route = useParams()["*"];
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

  return (
    <FumaDocsPage toc={toc} tableOfContent={{ style: "clerk" }}>
      <title>{frontmatter.title}</title>
      <meta name="description" content={frontmatter.description} />
      <FumaDocsTitle>{frontmatter.title}</FumaDocsTitle>
      <FumaDocsDescription>{frontmatter.description}</FumaDocsDescription>
      <FumaDocsBody>
        <Mdx components={{ ...defaultMdxComponents }} />
      </FumaDocsBody>

      {route && (
        <div>
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
      )}
    </FumaDocsPage>
  );
});

export function DocsPage({ loaderData }: { loaderData: DocsLoaderData }) {
  const { tree, path } = loaderData;
  const Content = renderer[path];
  return (
    <DocsLayout {...baseOptions()} tree={tree as PageTree.Root}>
      <Content />
    </DocsLayout>
  );
}
