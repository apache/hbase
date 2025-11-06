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

interface LinkType {
  label: string;
  to: string;
  external?: boolean;
}

interface NestedLinkType {
  label: string;
  links: LinkType[];
}

export const projectLinks: LinkType[] = [
  {
    label: "Overview",
    to: "/"
  },
  {
    label: "Downloads",
    to: "/downloads"
  },
  {
    label: "Mailing Lists",
    to: "/mailing-lists"
  },
  {
    label: "News",
    to: "/news"
  },
  {
    label: "Team",
    to: "/team"
  },
  {
    label: "Powered by HBase",
    to: "/powered-by-hbase"
  },
  {
    label: "HBase Sponsors",
    to: "/sponsors"
  },
  {
    label: "Code of Conduct",
    to: "/code-of-conduct"
  },
  {
    label: "Other Resources",
    to: "/other-resources"
  }
];

export const documentationLinks: (LinkType | NestedLinkType)[] = [
  {
    label: "Reference Guide",
    to: "https://hbase.apache.org/book.html"
  },
  {
    label: "Reference Guide (PDF)",
    to: "https://hbase.apache.org/apache_hbase_reference_guide.pdf"
  },
  {
    label: "中文参考指南(单页)",
    to: "https://abloz.com/hbase/book.html",
    external: true
  },
  {
    label: "Release Notes",
    to: "https://issues.apache.org/jira/browse/HBASE?report=com.atlassian.jira.plugin.system.project:changelog-panel#selectedTab=com.atlassian.jira.plugin.system.project%3Achangelog-panel",
    external: true
  },
  {
    label: "Issue Tracking",
    to: "https://issues.apache.org/jira/browse/HBASE",
    external: true
  },
  {
    label: "Source Repository",
    to: "/source-repository"
  },
  {
    label: "Resources",
    links: [
      {
        label: "Wiki",
        to: "https://cwiki.apache.org/confluence/display/HADOOP2/Hbase",
        external: true
      },
      {
        label: "Video/Presentations",
        to: "https://hbase.apache.org/book.html#other.info"
      },
      {
        label: "ACID Semantics",
        to: "/acid-semantics"
      },
      {
        label: "Bulk Loads",
        to: "https://hbase.apache.org/book.html#arch.bulk.load"
      },
      {
        label: "Metrics",
        to: "https://hbase.apache.org/book.html#hbase_metrics"
      }
    ]
  }
];

export const asfLinks: LinkType[] = [
  {
    label: "Apache Software Foundation",
    to: "http://www.apache.org/foundation/",
    external: true
  },
  {
    label: "License",
    to: "https://www.apache.org/licenses/",
    external: true
  },
  {
    label: "How Apache Works",
    to: "http://www.apache.org/foundation/how-it-works.html",
    external: true
  },
  {
    label: "Foundation Program",
    to: "http://www.apache.org/foundation/sponsorship.html",
    external: true
  },
  {
    label: "Sponsors",
    to: "https://www.apache.org/foundation/sponsors",
    external: true
  },
  {
    label: "Privacy Policy",
    to: "https://privacy.apache.org/policies/privacy-policy-public.html",
    external: true
  }
];

type DocumentationOptions = "ref" | "refPdf" | "userApi" | "userApiTest" | "devApi" | "devApiTest";

const documentationOptionLabels: Record<DocumentationOptions, string> = {
  ref: "Reference Guide",
  refPdf: "Reference Guide (PDF)",
  userApi: "User API",
  userApiTest: "User API (Test)",
  devApi: "Developer API",
  devApiTest: "Developer API (Test)"
};

function getDocsURL(version: string, option: DocumentationOptions): string {
  const baseUrl = "https://hbase.apache.org/";
  switch (option) {
    case "ref":
      return `${baseUrl}${version}/book.html`;
    case "refPdf":
      return `${baseUrl}${version}/book.pdf`;
    case "userApi":
      return `${baseUrl}${version}/apidocs/index.html`;
    case "userApiTest":
      return `${baseUrl}${version}/testapidocs/index.html`;
    case "devApi":
      return `${baseUrl}${version}/devapidocs/index.html`;
    case "devApiTest":
      return `${baseUrl}${version}/testdevapidocs/index.html`;
  }
}

const docsItems: Record<string, DocumentationOptions[]> = {
  "1.4": ["ref", "refPdf", "userApi", "userApiTest"],
  "2.3": ["ref", "refPdf", "userApi", "userApiTest", "devApi", "devApiTest"],
  "2.4": ["ref", "refPdf", "userApi", "userApiTest", "devApi", "devApiTest"],
  "2.5": ["userApi", "userApiTest", "devApi", "devApiTest"],
  "2.6": ["userApi", "userApiTest", "devApi", "devApiTest"]
};

export const docsLinks: NestedLinkType[] = Object.keys(docsItems).map((version) => ({
  label: `${version} Documentation`,
  links: docsItems[version].map((option) => ({
    label: documentationOptionLabels[option],
    to: getDocsURL(version, option)
  }))
}));
