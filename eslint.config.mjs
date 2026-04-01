import globals from "globals";
import pluginJs from "@eslint/js";

/** @type {import('eslint').Linter.Config[]} */
export default [
  {
    files: ["**/*.js"],
    languageOptions: {
      sourceType: "commonjs",
      globals: {
        ...globals.node,
      },
    },
    rules: {
      // keeping lint runnable for legacy codebase
      "no-undef": "off",
      "no-unused-vars": "off",
      "no-redeclare": "off",
      "no-empty": "off",
      "no-cond-assign": "off",
      "no-useless-escape": "off",
      "no-useless-assignment": "off",
      "no-prototype-builtins": "off",
      "no-async-promise-executor": "off",
    },
  },
  pluginJs.configs.recommended,
];