const { rules } = require('@eslint/js').configs.recommended;

module.exports = [
    // Configuration for JavaScript files
    {
        files: ['lib/**', 'test/**', 'bench/**', 'tools/**'],
        languageOptions: {
            ecmaVersion: 2024,
            sourceType: 'commonjs',
            globals: {
                // Node.js globals
                global: 'readonly',
                process: 'readonly',
                Buffer: 'readonly',
                __dirname: 'readonly',
                __filename: 'readonly',
                console: 'readonly',
                URL: 'readonly',
                queueMicrotask: 'readonly',
                setImmediate: 'readonly',
                clearImmediate: 'readonly',
                setInterval: 'readonly',
                clearInterval: 'readonly',
                setTimeout: 'readonly',
                clearTimeout: 'readonly',
            },
        },
        rules: {
            ...rules, // use recommended defaults
            // Error prevention rules
            'block-scoped-var': 'error',
            'no-cond-assign': 'error',
            // Off, not per-file. lib/headers.js matches a field value against the octets that
            // may not appear in one (the CVE-2020-8287 class: a CR or LF that an HTTP/1.1
            // translator on the other side reads as a line break). A guard that looks for
            // illegal control characters cannot be written without naming them, so the rule
            // has nothing to catch here and would only buy an inline disable comment.
            'no-control-regex': 'off',
            'no-debugger': 'error',
            'no-dupe-args': 'error',
            'no-dupe-keys': 'error',
            'no-duplicate-case': 'error',
            'no-ex-assign': 'error',
            'no-extra-semi': 'error',
            'no-func-assign': 'error',
            'no-invalid-regexp': 'error',
            'no-irregular-whitespace': 'error',
            'no-obj-calls': 'error',
            'no-redeclare': 'error',
            'no-regex-spaces': 'error',
            // A parameter a caller has to fill to reach a later one is not dead weight, it is
            // the signature: `sess.on('goaway', (code, _last) => …)` keeps node's argument
            // shape visible while saying the value is unused on purpose. Named without the
            // underscore, both would have to be dropped and the shape lost.
            'no-unused-vars': ['error', { args: 'after-used', argsIgnorePattern: '^_' }],
            'no-sparse-arrays': 'error',
            'no-unexpected-multiline': 'error',
            'no-unreachable': 'error',
            'no-delete-var': 'error',
            'no-shadow': [
                'error',
                { builtinGlobals: false, hoist: 'all', allow: [] },
            ],
            'no-undef': 'error',
            'use-isnan': 'error',
            'valid-typeof': 'error',

            // Code quality and best practices
            'no-eval': 'error',
            'no-implied-eval': 'error',
            'no-with': 'error',
            'no-multi-spaces': 'error',
            'no-multiple-empty-lines': [
                'error',
                { max: 2, maxBOF: 0, maxEOF: 0 },
            ],
            'no-trailing-spaces': 'error',
            'eol-last': ['error', 'always'],
            semi: ['error', 'always'],
            indent: ['error', 2, { SwitchCase: 1 }],
            quotes: ['error', 'single', { avoidEscape: true }],
            'comma-dangle': ['error', 'never'],
            'no-console': [
                'warn',
                { allow: ['warn', 'error', 'table', 'info'] },
            ],

            // Security-related rules
            'no-new-func': 'error',
            'no-script-url': 'error',

            // Performance considerations
            'no-constant-condition': 'warn',
            'no-unused-expressions': 'error',
            'no-useless-escape': 'warn',
            'no-useless-catch': 'error',
            'no-useless-return': 'error',

            // Readability improvements
            curly: ['error', 'all'],
            'brace-style': ['error', '1tbs', { allowSingleLine: false }],
            'keyword-spacing': 'error',
            'space-before-blocks': 'error',
            'space-before-function-paren': [
                'error',
                { anonymous: 'always', named: 'never', asyncArrow: 'always' },
            ],
            'space-infix-ops': 'error',
            'space-unary-ops': 'error',
            'prefer-const': 'warn',
            'no-var': 'error',
        },
    },
    // Configuration for test files. A differential test pairs a server handler's (req, res)
    // with the client's response of the same name, and both are names the API fixes, so
    // `no-shadow` is off here (it stays on in lib/, bench/ and tools/). The file list must
    // stay narrow: a '**/*.js' entry turns the block into a global override, and rules that
    // look enabled in the block above are silently not running.
    {
        files: ['test/**/*.js', '**/*.test.js'],
        rules: {
            'no-console': 'off',
            'no-unused-expressions': 'off',
            'no-shadow': 'off',
        },
    },
    // The benchmark arms and the probes are reporters: their output IS the result.
    {
        files: ['bench/**/*.js', 'tools/**/*.js'],
        rules: {
            'no-console': 'off',
        },
    },
    // Two microbenches exist to cost a code-generated header-object builder, so the
    // Function constructor is the arm under measurement, not a way to escape it
    // (bench/headers-cost.js, bench/toobject-cost.js; the result is in AGENTS.md 7).
    // lib/ keeps the rule: nothing there may build code out of a string.
    {
        files: ['bench/**/*.js'],
        rules: {
            'no-new-func': 'off',
        },
    },
    // Ignore certain patterns. Everything under out/, profs/ and profs2/ is a generated
    // artifact (the same list .gitignore keeps) -- including scratch files that are not
    // parseable JavaScript at all.
    {
        ignores: [
            'node_modules/**', 'coverage/**', '.git/**', '*.log',
            'out/**', 'profs/**', 'profs2/**', 'bench/client/dir/**',
            // Vendored byte-for-byte (AGENTS.md 1), so its upstream formatting is part
            // of the vendoring: fast-timer.js is undici's.
            // It may not be reflowed by --fix, and it is not ours to lint.
            'lib/fast-timer.js',
            // V8 natives-syntax (%HasFastProperties, %DebugPrint) is not JavaScript a
            // parser can accept; these archive probes run under --allow-natives-syntax.
            'bench/micro/nullproto-alloc.js', 'bench/micro/nullproto-root.js',
            'bench/micro/nullproto-tree2.js', 'bench/micro/nullproto-tree3.js',
        ],
    },
];
