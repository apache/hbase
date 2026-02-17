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

import { describe, it, expect } from "vitest";
import { extractVersion } from "../scripts/extract-hbase-version.js";

describe("extract-hbase-version script", () => {
  const createPomXml = (revision) => `<?xml version="1.0" encoding="UTF-8"?>
<project>
  <properties>
    <revision>${revision}</revision>
  </properties>
</project>`;

  it("should extract the revision value", () => {
    const pomXml = createPomXml("4.0.0-alpha-1-SNAPSHOT");
    expect(extractVersion(pomXml)).toBe("4.0.0-alpha-1-SNAPSHOT");
  });

  it("should trim whitespace around the revision value", () => {
    const pomXml = createPomXml("  3.0.0  ");
    expect(extractVersion(pomXml)).toBe("3.0.0");
  });

  it("should handle multiline revision content", () => {
    const pomXml = createPomXml("\n 4.0.0 \n");
    expect(extractVersion(pomXml)).toBe("4.0.0");
  });

  it("should throw when revision is missing", () => {
    const pomXml = `<?xml version="1.0" encoding="UTF-8"?>
<project>
  <properties></properties>
</project>`;
    expect(() => extractVersion(pomXml)).toThrow("No <revision> tag found in pom.xml");
  });
});

