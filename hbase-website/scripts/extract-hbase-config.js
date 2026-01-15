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

/**
 * Extracts a field value from a property XML block
 * @param {string} block - The XML block containing property information
 * @param {string} fieldName - The name of the field to extract
 * @returns {string} The field value or empty string if not present
 */
export function extractField(block, fieldName) {
  const fieldRegex = new RegExp(`<${fieldName}>(.*?)<\/${fieldName}>`, 's');
  const fieldMatch = block.match(fieldRegex);
  return fieldMatch ? fieldMatch[1].trim() : '';
}

/**
 * Checks if a property block has skipInDoc attribute set to true
 * @param {string} block - The full property XML block including the opening tag
 * @returns {boolean} True if the property should be skipped
 */
export function shouldSkipProperty(block) {
  return /skipInDoc\s*=\s*["']true["']/.test(block);
}

/**
 * Parses properties from hbase-default.xml content
 * @param {string} xmlContent - The content of the hbase-default.xml file
 * @returns {Array<Object>} Array of property objects
 */
export function parseProperties(xmlContent) {
  // Match each property block including its opening tag
  const propertyBlockRegex = /<property[^>]*>([\s\S]*?)<\/property>/gs;

  const properties = [];
  let match;

  while ((match = propertyBlockRegex.exec(xmlContent)) !== null) {
    const fullBlock = match[0];
    const innerBlock = match[1];
    
    // Skip properties marked with skipInDoc="true"
    if (shouldSkipProperty(fullBlock)) {
      continue;
    }
    
    const name = extractField(innerBlock, 'name');
    const value = extractField(innerBlock, 'value');
    const description = extractField(innerBlock, 'description');
    
    // Only add properties that have a name
    if (name) {
      properties.push({
        name,
        value,
        description
      });
    }
  }

  return properties;
}

/**
 * Formats the description text for MDX output
 * @param {string} description - The raw description text
 * @returns {string} Formatted description
 */
export function formatDescription(description) {
  if (!description) {
    return '';
  }
  
  // Remove excessive whitespace and newlines, but preserve paragraph breaks
  return description
    .split('\n')
    .map(line => line.trim())
    .filter(line => line.length > 0)
    .join(' ');
}

/**
 * Generates MDX content from properties
 * @param {Array<Object>} properties - Array of property objects
 * @returns {string} The MDX content
 */
export function generateMDX(properties) {
  const header = `---
title: "HBase Default Configuration"
description: "Complete reference of all HBase configuration properties with descriptions and default values."
---

`;

  const propertiesContent = properties.map(prop => {
    const description = formatDescription(prop.description);
    const value = prop.value || '(empty)';
    
    return `#### \`${prop.name}\` [!toc]

**Description:** ${description}  
**Default:** \`${value}\`
`;
  }).join('\n');

  return header + propertiesContent;
}

/**
 * Main function to extract properties and generate MDX file
 */
export function main() {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);

  // Read the hbase-default.xml file
  const xmlPath = path.join(__dirname, '..', '..', 'hbase-common', 'src', 'main', 'resources', 'hbase-default.xml');
  
  if (!fs.existsSync(xmlPath)) {
    console.error(`Error: Could not find hbase-default.xml at ${xmlPath}`);
    process.exit(1);
  }
  
  const xmlContent = fs.readFileSync(xmlPath, 'utf-8');

  let properties;
  try {
    properties = parseProperties(xmlContent);
  } catch (error) {
    console.error('Error parsing hbase-default.xml:', error.message);
    process.exit(1);
  }

  console.log(`Extracted ${properties.length} properties from hbase-default.xml`);

  // Generate MDX content
  const mdxContent = generateMDX(properties);

  // Write to MD file
  const outputPath = path.join(__dirname, '..', 'app', 'pages', '_docs', 'docs', '_mdx', '(multi-page)', 'configuration', 'hbase-default.md');
  
  // Ensure the directory exists
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  fs.writeFileSync(outputPath, mdxContent, 'utf-8');

  console.log(`Configuration documentation written to ${outputPath}`);
}

// Run main if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

