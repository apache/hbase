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

import { Calendar, ExternalLink } from "lucide-react";
import events from "./events.json";

interface Event {
  date: string;
  title: string;
  location: string;
  url: string;
  registrationOpen: boolean;
}

export function NewsPage() {
  return (
    <section className="container mx-auto px-4 py-12">
      <h1 className="my-12 text-center text-4xl font-semibold tracking-tight text-balance md:text-6xl">
        News
      </h1>

      {/* Apache Event Banner */}
      <div className="mb-12 text-center">
        <p className="text-muted-foreground mb-2 font-semibold">The next Apache Event 👇</p>
        <a
          href="https://www.apache.org/events/current-event.html"
          target="_blank"
          rel="noopener noreferrer"
          className="border-primary/30 shadow-primary/20 hover:border-primary/60 hover:shadow-primary/30 inline-block rounded-lg border-2 shadow-lg transition-all duration-700 hover:shadow-2xl"
        >
          <img
            src="https://www.apache.org/events/current-event-234x60.png"
            alt="Apache Event"
            className="rounded-md"
          />
        </a>
      </div>

      <div className="space-y-4">
        {(events as Event[]).map((event, index) => (
          <div
            key={index}
            className="border-border bg-card hover:border-primary/50 group relative rounded-lg border p-4 transition-all hover:shadow-md"
          >
            {event.registrationOpen && (
              <div className="absolute top-3 right-3">
                <span className="inline-flex items-center rounded-full bg-green-500/10 px-2.5 py-1 text-xs font-semibold text-green-600 ring-1 ring-green-500/20 ring-inset dark:text-green-400">
                  Registration Open
                </span>
              </div>
            )}
            <div className="flex items-start gap-3">
              <div className="mt-1 flex-shrink-0">
                <Calendar className="text-muted-foreground size-4" />
              </div>
              <div className="flex-1">
                <div className="text-muted-foreground mt-0.5 mb-1 text-sm font-semibold">
                  {event.date}
                </div>
                <div className="text-foreground mb-1 text-base leading-7">
                  {event.url ? (
                    <a
                      href={event.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-primary inline-flex items-center gap-1.5 font-medium underline-offset-4 hover:underline"
                    >
                      {event.title}
                      <ExternalLink className="size-3.5" />
                    </a>
                  ) : (
                    <span className="font-medium">{event.title}</span>
                  )}
                </div>
                {event.location && (
                  <div className="text-muted-foreground text-sm">{event.location}</div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
