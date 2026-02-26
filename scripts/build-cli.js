import * as esbuild from 'esbuild';
import { chmod } from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

async function build() {
  // Main CLI
  await esbuild.build({
    entryPoints: [join(__dirname, 'start-server.ts')],
    bundle: true,
    minify: true,
    platform: 'node',
    target: 'node18',
    format: 'esm',
    outfile: 'bin/ncli.mjs',
    banner: {
      js: "#!/usr/bin/env node\nimport { createRequire } from 'module';const require = createRequire(import.meta.url);" // see https://github.com/evanw/esbuild/pull/2067
    },
    external: ['util', './notion-to-local.js', './auth.js'],
  });

  // Auth module
  await esbuild.build({
    entryPoints: [join(__dirname, 'auth.ts')],
    bundle: true,
    minify: true,
    platform: 'node',
    target: 'node18',
    format: 'esm',
    outfile: 'bin/auth.js',
    banner: {
      js: "import { createRequire } from 'module';const require = createRequire(import.meta.url);"
    },
    external: ['util'],
  });

  // Export script (separate bundle)
  await esbuild.build({
    entryPoints: [join(__dirname, 'notion-to-local.ts')],
    bundle: true,
    minify: true,
    platform: 'node',
    target: 'node18',
    format: 'esm',
    outfile: 'bin/notion-to-local.js',
    banner: {
      js: "import { createRequire } from 'module';const require = createRequire(import.meta.url);"
    },
    external: ['util'],
  });

  // Make the output file executable
  await chmod('./bin/ncli.mjs', 0o755);
}

build().catch((err) => {
  console.error(err);
  process.exit(1);
});
