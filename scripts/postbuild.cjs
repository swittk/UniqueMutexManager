const fs = require('fs');
const path = require('path');

const esmDir = path.join(__dirname, '..', 'dist', 'esm');
const esmPackageJsonPath = path.join(esmDir, 'package.json');

if (!fs.existsSync(esmDir)) {
  throw new Error(`Expected ESM output directory at ${esmDir} to exist. Did the TypeScript build succeed?`);
}

const existingPackageJson = fs.existsSync(esmPackageJsonPath)
  ? JSON.parse(fs.readFileSync(esmPackageJsonPath, 'utf8'))
  : {};

if (existingPackageJson.type !== 'module') {
  fs.writeFileSync(esmPackageJsonPath, JSON.stringify({ type: 'module' }, null, 2) + '\n');
}
