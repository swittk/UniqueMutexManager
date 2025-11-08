const fs = require('fs');
const path = require('path');

const esmDir = path.join(__dirname, '..', 'dist', 'esm');
const esmPackageJsonPath = path.join(esmDir, 'package.json');

if (!fs.existsSync(esmDir)) {
  throw new Error(`Expected ESM output directory at ${esmDir} to exist. Did the TypeScript build succeed?`);
}

const esmPackageJson = {
  type: 'module'
};

fs.writeFileSync(esmPackageJsonPath, JSON.stringify(esmPackageJson, null, 2) + '\n');
