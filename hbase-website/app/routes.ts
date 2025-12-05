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

import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
  // Landing
  layout("./pages/_landing/landing-layout.tsx", [
    index("routes/_landing/home.tsx"),
    route("code-of-conduct", "routes/_landing/code-of-conduct.tsx"),
    route("downloads", "routes/_landing/downloads.tsx"),
    route("mailing-lists", "routes/_landing/mailing-lists.tsx"),
    route("team", "routes/_landing/team.tsx"),
    route("sponsors", "routes/_landing/sponsors.tsx"),
    route("powered-by-hbase", "routes/_landing/powered-by-hbase.tsx"),
    route("other-resources", "routes/_landing/other-resources.tsx"),
    route("source-repository", "routes/_landing/source-repository.tsx"),
    route("acid-semantics", "routes/_landing/acid-semantics.tsx"),
    route("news", "routes/_landing/news.tsx")
  ]),
  // Docs
  layout("./pages/_docs/docs-layout.tsx", [route("docs/*", "routes/_docs/docs.tsx")]),
  route("api/search", "routes/_api/search.ts")
] satisfies RouteConfig;
