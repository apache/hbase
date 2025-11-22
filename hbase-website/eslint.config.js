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

import js from "@eslint/js";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import tseslint from "typescript-eslint";
import { defineConfig, globalIgnores } from "eslint/config";
import importPlugin from "eslint-plugin-import";
import prettier from "eslint-plugin-prettier";

// Custom rule to enforce Apache License header
const apacheLicenseRule = {
  meta: {
    type: "problem",
    docs: {
      description: "Enforce Apache License header in source files"
    },
    fixable: "code",
    messages: {
      missingHeader: "Missing Apache License header at the top of the file"
    }
  },
  create(context) {
    const REQUIRED_HEADER = `//
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
//`;

    return {
      Program(node) {
        const sourceCode = context.sourceCode || context.getSourceCode();
        const text = sourceCode.getText();
        
        if (!text.startsWith(REQUIRED_HEADER)) {
          context.report({
            node,
            messageId: "missingHeader",
            fix(fixer) {
              return fixer.insertTextBefore(node, REQUIRED_HEADER + "\n\n");
            }
          });
        }
      }
    };
  }
};

export default defineConfig([
  globalIgnores([
    "node_modules",
    "build",
    "dist",
    "tsconfig.json",
    "prettier.config.js",
    "react-router.config.ts",
    ".react-router"
  ]),
  {
    files: ["**/*.{ts,tsx}"],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs["recommended-latest"],
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    plugins: {
      prettier,
      import: importPlugin,
      custom: {
        rules: {
          "apache-license": apacheLicenseRule
        }
      }
    },
    settings: {
      // so import/no-unresolved understands TS paths and "@/*"
      "import/resolver": {
        typescript: {
          project: ['./tsconfig.app.json', './tsconfig.node.json'],
        },
      },
    },
    rules: {
      "import/no-unresolved": "error",
      "import/no-duplicates": "warn",

      "no-implicit-globals": "off",
      "no-empty-pattern": "off",

      "@typescript-eslint/no-unused-vars": "warn",
      "@typescript-eslint/ban-ts-comment": "off",
      "@typescript-eslint/no-explicit-any": "off",

      "react/react-in-jsx-scope": "off",
      "react/prop-types": "off",
      "react/no-unescaped-entities": "off",

      "prettier/prettier": "error",
      
      // Enforce Apache License header
      "custom/apache-license": "error",
    },
  },
]);
