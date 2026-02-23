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

import developers from "./developers.json";

interface Developer {
  id: string;
  name: string;
  email: string;
  timezone: string;
}

export function TeamPage() {
  return (
    <div className="container mx-auto px-4 py-12">
      <article className="prose prose-slate dark:prose-invert max-w-none">
        <h1 className="my-12 text-center text-4xl font-semibold tracking-tight text-balance md:text-6xl">
          The Team
        </h1>

        <p className="mb-4 text-base leading-7">
          A successful project requires many people to play many roles. Some members write code or
          documentation, while others are valuable as testers, submitting patches and suggestions.
        </p>

        <p className="mb-4 text-base leading-7">
          The team is comprised of Members and Contributors. Members have direct access to the
          source of a project and actively evolve the code-base. Contributors improve the project
          through submission of patches and suggestions to the Members. The number of Contributors
          to the project is unbounded. Get involved today. All contributions to the project are
          greatly appreciated.
        </p>

        <h2 className="mt-12 mb-4 scroll-mt-28 text-3xl font-semibold tracking-tight md:text-4xl">
          Members
        </h2>

        <p className="mb-4 text-base leading-7">
          These are the developers with commit privileges that have directly contributed to the
          project in one way or another.
        </p>

        <div className="border-border my-8 w-full overflow-x-auto rounded-lg border">
          <table className="w-full table-fixed border-collapse text-sm">
            <thead className="bg-muted">
              <tr className="border-border border-b">
                <th className="px-4 py-3 text-left font-semibold">Id</th>
                <th className="px-4 py-3 text-left font-semibold">Name</th>
                <th className="px-4 py-3 text-left font-semibold">Email</th>
                <th className="px-4 py-3 text-left font-semibold">Time Zone</th>
              </tr>
            </thead>
            <tbody>
              {(developers as Developer[]).map((developer) => (
                <tr
                  key={developer.id}
                  className="border-border hover:bg-muted/50 border-b transition-colors"
                >
                  <td className="px-4 py-3 align-top">{developer.id}</td>
                  <td className="px-4 py-3 align-top">{developer.name || "-"}</td>
                  <td className="px-4 py-3 align-top">
                    <a href={`mailto:${developer.email}`} className="text-primary hover:underline">
                      {developer.email}
                    </a>
                  </td>
                  <td className="px-4 py-3 align-top">{developer.timezone}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <h2 className="mt-12 mb-4 scroll-mt-28 text-3xl font-semibold tracking-tight md:text-4xl">
          Contributors
        </h2>
        <p className="mb-4 text-base leading-7">
          Apache HBase™ does not maintain a list of contributors.
        </p>
      </article>
    </div>
  );
}
