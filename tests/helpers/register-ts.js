const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const compilerOptions = {
  module: ts.ModuleKind.CommonJS,
  target: ts.ScriptTarget.ES2020,
  moduleResolution: ts.ModuleResolutionKind.Node10,
  esModuleInterop: true,
  strict: true,
  skipLibCheck: true,
};

require.extensions['.ts'] = function registerTsModule(module, filename) {
  const source = fs.readFileSync(filename, 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions,
    fileName: filename,
    reportDiagnostics: false,
  });
  module._compile(outputText, filename);
};

if (require.main === module && process.argv[2]) {
  require(path.resolve(process.argv[2]));
}
