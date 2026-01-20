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

import { Link } from "@/components/link";
import { projectLinks, documentationLinks, asfLinks } from "./links";

function ExternalIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="ml-1 inline-block h-3 w-3"
    >
      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}

export function SiteFooter() {
  return (
    <footer className="container mx-auto">
      <div className="border-border/60 text-muted-foreground px-4 pt-12 pb-8 text-sm md:pt-16 md:pb-10">
        <div className="grid gap-6 md:grid-cols-4">
          <div>
            <p className="text-foreground font-medium">Apache HBase</p>
            <p className="mt-2">
              Open-source, distributed, and scalable big data store modeled after Google Bigtable.
            </p>
          </div>
          <div>
            <p className="text-foreground font-medium">Project</p>
            <ul className="mt-2 space-y-1">
              {projectLinks.map((link) => (
                <li key={link.label}>
                  <Link
                    to={link.to}
                    target={link.external ? "_blank" : "_self"}
                    className="hover:text-foreground inline-flex items-center"
                  >
                    {link.label}
                    {link.external && <ExternalIcon />}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
          <div>
            <p className="text-foreground font-medium">Documentation</p>
            <ul className="mt-2 space-y-1">
              {documentationLinks.map((link) =>
                "to" in link ? (
                  <li key={link.label}>
                    <Link
                      to={link.to}
                      target={link.external ? "_blank" : "_self"}
                      className="hover:text-foreground inline-flex items-center"
                    >
                      {link.label}
                      {link.external && <ExternalIcon />}
                    </Link>
                  </li>
                ) : (
                  link.links.map((link) => (
                    <li key={link.label}>
                      <Link
                        to={link.to}
                        target={link.external ? "_blank" : "_self"}
                        className="hover:text-foreground inline-flex items-center"
                      >
                        {link.label}
                        {link.external && <ExternalIcon />}
                      </Link>
                    </li>
                  ))
                )
              )}
            </ul>
          </div>
          <div>
            <p className="text-foreground font-medium">ASF</p>
            <ul className="mt-2 space-y-1">
              {asfLinks.map((link) => (
                <li key={link.label}>
                  <Link
                    to={link.to}
                    target={link.external ? "_blank" : "_self"}
                    className="hover:text-foreground inline-flex items-center"
                  >
                    {link.label}
                    {link.external && <ExternalIcon />}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        </div>

        <hr className="border-border/60 my-8" />

        <p className="text-xs leading-6">
          Copyright ©2007–2026 The Apache Software Foundation. All rights reserved. Apache HBase,
          HBase, Apache, the Apache HBase logo and the ASF logo are either registered trademarks or
          trademarks of the Apache Software Foundation. All other marks mentioned may be trademarks
          or registered trademarks of their respective owners.
        </p>
      </div>
    </footer>
  );
}
