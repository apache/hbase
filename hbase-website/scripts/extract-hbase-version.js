#!/usr/bin/env node
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

/**
 * Extracts the HBase version from pom.xml
 * @param {string} pomContent - The content of the pom.xml file
 * @returns {string} The extracted version
 */
export function extractVersion(pomContent) {
  const revisionMatch = pomContent.match(/<revision>(.*?)<\/revision>/s);
  if (!revisionMatch) {
    throw new Error("No <revision> tag found in pom.xml");
  }
  return revisionMatch[1].trim();
}

/**
 * Main function to extract version and write to JSON file
 */
export function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);

  const pomPath = path.join(__dirname, "..", "..", "pom.xml");
  const pomContent = fs.readFileSync(pomPath, "utf-8");

  let version;
  try {
    version = extractVersion(pomContent);
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }

  const outputPath = path.join(__dirname, "..", "app", "lib", "export-pdf", "hbase-version.json");
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  fs.writeFileSync(outputPath, JSON.stringify({ version }, null, 2));

  console.log(`HBase version written to ${outputPath}`);
}

// Run main if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
