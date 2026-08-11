// Build the downloadable browser SDK layout. The outer Make/PowerShell target
// turns this verified stage into the release ZIP for the host platform.

import { createHash } from "node:crypto";
import { cp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { basename, dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { brotliCompress, constants as zlibConstants, gzip } from "node:zlib";
import { promisify } from "node:util";
import { build } from "esbuild";

const gzipAsync = promisify(gzip);
const brotliCompressAsync = promisify(brotliCompress);
const scriptDir = dirname(fileURLToPath(import.meta.url));
const sdkDir = resolve(scriptDir, "..");
const repoDir = resolve(sdkDir, "..", "..");
const distDir = resolve(repoDir, "dist");

function cargoVersion(cargoToml) {
  const match = cargoToml.match(/^version\s*=\s*"([^"]+)"\s*$/m);
  if (!match) throw new Error("could not read the root Cargo.toml version");
  return match[1];
}

function sha256(data) {
  return createHash("sha256").update(data).digest("hex");
}

function assertInside(parent, child) {
  const parentPrefix = parent.endsWith(sep) ? parent : `${parent}${sep}`;
  if (!child.startsWith(parentPrefix)) throw new Error(`refusing to write outside ${parent}`);
}

async function filesUnder(root) {
  const files = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const fullPath = resolve(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...await filesUnder(fullPath));
    } else if (entry.isFile()) {
      files.push(fullPath);
    }
  }
  return files.sort();
}

async function writeChecksums(stage) {
  const checksumPath = resolve(stage, "SHA256SUMS.txt");
  const files = (await filesUnder(stage)).filter((file) => file !== checksumPath);
  const lines = [];
  for (const file of files) {
    const rel = relative(stage, file).split(sep).join("/");
    lines.push(`${sha256(await readFile(file))}  ${rel}`);
  }
  await writeFile(checksumPath, `${lines.join("\n")}\n`);
}

async function verifyStage(stage) {
  const required = [
    "dist/citadel-client.min.mjs",
    "dist/citadel-client.min.mjs.map",
    "dist/citadel-client.min.mjs.gz",
    "dist/citadel-client.min.mjs.br",
    "index.d.ts",
    "chat.d.ts",
    "README.md",
    "examples/threejs-starter/index.html",
    "examples/threejs-starter/app.js",
    "examples/threejs-starter/README.md",
    "SHA256SUMS.txt",
  ];
  for (const file of required) {
    const fullPath = resolve(stage, file);
    if (!(await stat(fullPath)).isFile()) throw new Error(`missing staged release file: ${file}`);
  }

  const bundlePath = resolve(stage, "dist", "citadel-client.min.mjs");
  const bundleText = await readFile(bundlePath, "utf8");
  if (bundleText.includes("node:")) throw new Error("browser bundle unexpectedly imports a Node built-in");
  const exports = await import(`${pathToFileURL(bundlePath).href}?verify=${Date.now()}`);
  for (const name of ["CitadelClient", "Envelope", "KIND_POSITION"]) {
    if (!(name in exports)) throw new Error(`browser bundle is missing export ${name}`);
  }

  const sourceMap = JSON.parse(await readFile(`${bundlePath}.map`, "utf8"));
  if ("sourcesContent" in sourceMap) {
    throw new Error("browser source map must not embed source contents");
  }

  const checksums = new Map();
  for (const line of (await readFile(resolve(stage, "SHA256SUMS.txt"), "utf8")).trim().split("\n")) {
    const match = line.match(/^([a-f0-9]{64})  (.+)$/);
    if (!match) throw new Error(`invalid checksum line: ${line}`);
    checksums.set(match[2], match[1]);
  }
  for (const [rel, expected] of checksums) {
    const actual = sha256(await readFile(resolve(stage, rel)));
    if (actual !== expected) throw new Error(`checksum mismatch for ${rel}`);
  }
}

const version = cargoVersion(await readFile(resolve(repoDir, "Cargo.toml"), "utf8"));
const packageName = `citadel-client-js-v${version}`;
const stage = resolve(distDir, packageName);
assertInside(distDir, stage);

await rm(stage, { recursive: true, force: true });
await mkdir(resolve(stage, "dist"), { recursive: true });

const bundlePath = resolve(stage, "dist", "citadel-client.min.mjs");
await build({
  absWorkingDir: sdkDir,
  entryPoints: ["src/index.js"],
  outfile: bundlePath,
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  minify: true,
  legalComments: "none",
  sourcemap: "external",
  sourcesContent: false,
});

const bundle = await readFile(bundlePath);
await writeFile(`${bundlePath}.gz`, await gzipAsync(bundle, {
  level: zlibConstants.Z_BEST_COMPRESSION,
  mtime: 0,
}));
await writeFile(`${bundlePath}.br`, await brotliCompressAsync(bundle, {
  params: {
    [zlibConstants.BROTLI_PARAM_MODE]: zlibConstants.BROTLI_MODE_TEXT,
    [zlibConstants.BROTLI_PARAM_QUALITY]: zlibConstants.BROTLI_MAX_QUALITY,
  },
}));

await cp(resolve(sdkDir, "index.d.ts"), resolve(stage, "index.d.ts"));
await cp(resolve(sdkDir, "chat.d.ts"), resolve(stage, "chat.d.ts"));
await cp(resolve(sdkDir, "README.md"), resolve(stage, "README.md"));
await cp(resolve(sdkDir, "examples", "threejs-starter"), resolve(stage, "examples", "threejs-starter"), {
  recursive: true,
});
await writeChecksums(stage);
await verifyStage(stage);

console.log(`Staged verified browser SDK: ${relative(repoDir, stage)} (${basename(bundlePath)})`);
