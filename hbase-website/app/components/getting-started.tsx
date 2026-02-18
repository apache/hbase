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
import { Link } from "@/components/link";

export function GettingStartedSection() {
  const steps = [
    {
      title: "1. Download",
      desc: "Grab the latest stable release and verify checksums.",
      to: "/downloads"
    },
    {
      title: "2. Read the Guide",
      desc: "Walk through cluster setup, schema design, and operations.",
      to: "https://hbase.apache.org/book.html#_get_started_with_hbase"
    },
    {
      title: "3. Connect a Client",
      desc: "Use the Java API, REST, or Thrift to start building.",
      to: "https://hbase.apache.org/book.html#hbase_apis"
    }
  ];
  return (
    <section id="getting-started" className="border-border/60 bg-muted/30 border-y">
      <div className="container mx-auto px-4 py-12 md:py-16">
        <div className="mb-8 text-center">
          <h2 className="text-3xl font-semibold tracking-tight md:text-4xl">Getting Started</h2>
          <p className="text-muted-foreground mt-2">
            From download to production in a few simple steps.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          {steps.map((s) => (
            <div
              key={s.title}
              className="border-border/60 bg-card flex flex-col rounded-xl border p-5 shadow-sm"
            >
              <h3 className="text-foreground text-lg font-semibold">{s.title}</h3>
              <p className="text-muted-foreground mt-1 text-sm leading-6">{s.desc}</p>
              <div className="mt-auto">
                <Button asChild size="sm" variant="link" className="p-0">
                  <Link to={s.to}>Learn more →</Link>
                </Button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
