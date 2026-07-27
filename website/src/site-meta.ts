import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const cargoTomlPath = resolve(process.cwd(), '..', 'Cargo.toml');
const cargoToml = readFileSync(cargoTomlPath, 'utf8');
const versionMatch = cargoToml.match(/^version\s*=\s*"([^"]+)"$/m);

if (!versionMatch) {
	throw new Error(`Unable to read the Citadel version from ${cargoTomlPath}.`);
}

export const CURRENT_VERSION = versionMatch[1];
