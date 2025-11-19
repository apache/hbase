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

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Read the parent pom.xml
const pomPath = path.join(__dirname, '..', '..', 'pom.xml');
const pomContent = fs.readFileSync(pomPath, 'utf-8');

// Extract developers using regex
const developersMatch = pomContent.match(/<developers>([\s\S]*?)<\/developers>/);
if (!developersMatch) {
  console.error('No developers section found in pom.xml');
  process.exit(1);
}

const developersXml = developersMatch[1];
const developerRegex = /<developer>\s*<id>(.*?)<\/id>\s*<name>(.*?)<\/name>\s*<email>(.*?)<\/email>\s*<timezone>(.*?)<\/timezone>\s*<\/developer>/gs;

const developers = [];
let match;

while ((match = developerRegex.exec(developersXml)) !== null) {
  const id = match[1].trim();
  const name = match[2].trim();
  const email = match[3].trim();
  const timezone = match[4].trim();
  
  developers.push({
    id,
    name,
    email,
    timezone
  });
}

console.log(`Extracted ${developers.length} developers from pom.xml`);

// Write to JSON file
const outputPath = path.join(__dirname, '..', 'app', 'pages', 'team', 'developers.json');
fs.writeFileSync(outputPath, JSON.stringify(developers, null, 2));

console.log(`Developers data written to ${outputPath}`);

