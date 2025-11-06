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

import { Button } from "@/ui/button";
import { ArrowUpRight } from "lucide-react";
import { Link } from "react-router";

export function CommunitySection() {
  return (
    <section id="community">
      <div className="container mx-auto px-4 py-12 md:py-16">
        <div className="grid grid-cols-1 gap-12 md:grid-cols-2">
          <div>
            <h2 className="text-3xl font-semibold tracking-tight">A Vibrant Community</h2>
            <p className="text-muted-foreground mt-3">
              HBase is a top-level Apache project with an active community of users and
              contributors. Join discussions, read the reference guide, and help shape the roadmap.
            </p>
            <div className="mt-6 flex flex-wrap gap-3">
              <Button asChild>
                <Link to="/mailing-lists">Mailing Lists</Link>
              </Button>
              <Button asChild variant="outline">
                <Link to="https://github.com/apache/hbase" target="_blank">
                  Contribute
                </Link>
              </Button>
              <Button asChild variant="ghost">
                <Link to="/code-of-conduct">Code of Conduct</Link>
              </Button>
            </div>
          </div>
          <ul className="grid gap-3 text-sm leading-6">
            <li id="news" className="relative p-0">
              <Link
                to="/news"
                className="border-border/60 bg-background hover:border-primary focus-visible:ring-primary block rounded-lg border p-4 transition-colors focus-visible:ring-2 focus-visible:outline-none"
              >
                <span className="text-foreground font-medium">News &amp; Events</span>
                <p className="text-muted-foreground">HBaseCon and community meetups worldwide.</p>
                <ArrowUpRight className="text-muted-foreground absolute top-2.5 right-2.5 size-4" />
              </Link>
            </li>
            <li id="sponsors" className="relative p-0">
              <Link
                to="/sponsors"
                className="border-border/60 bg-background hover:border-primary focus-visible:ring-primary block rounded-lg border p-4 transition-colors focus-visible:ring-2 focus-visible:outline-none"
              >
                <span className="text-foreground font-medium">Sponsors</span>
                <p className="text-muted-foreground">
                  Thanks to organizations supporting the project.
                </p>
                <ArrowUpRight className="text-muted-foreground absolute top-2.5 right-2.5 size-4" />
              </Link>
            </li>
            <li id="security" className="relative p-0">
              <Link
                to="https://hbase.apache.org/book.html#security"
                className="border-border/60 bg-background hover:border-primary focus-visible:ring-primary block rounded-lg border p-4 transition-colors focus-visible:ring-2 focus-visible:outline-none"
              >
                <span className="text-foreground font-medium">Security</span>
                <p className="text-muted-foreground">See the Reference Guide security chapter.</p>
                <ArrowUpRight className="text-muted-foreground absolute top-2.5 right-2.5 size-4" />
              </Link>
            </li>
          </ul>
        </div>
      </div>
    </section>
  );
}
