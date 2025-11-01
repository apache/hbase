import js from "@eslint/js";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import tseslint from "typescript-eslint";
import { defineConfig, globalIgnores } from "eslint/config";
import importPlugin from "eslint-plugin-import";
import prettier from "eslint-plugin-prettier";

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
      import: importPlugin
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
    },
  },
]);
