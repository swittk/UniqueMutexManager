const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const configPath = path.join(__dirname, '..', '..', 'tsconfig.json');
let compilerOptions = {
  module: ts.ModuleKind.CommonJS,
  target: ts.ScriptTarget.ES2020,
  moduleResolution: ts.ModuleResolutionKind.Node10,
  esModuleInterop: true,
  strict: true,
};

try {
  const configFile = ts.readConfigFile(configPath, ts.sys.readFile);
  if (!configFile.error) {
    const parsed = ts.parseJsonConfigFileContent(configFile.config, ts.sys, path.dirname(configPath));
    compilerOptions = { ...compilerOptions, ...parsed.options };
  }
} catch (error) {
  // Ignore config loading issues and fall back to defaults
}

require.extensions['.ts'] = function registerTsModule(module, filename) {
  const source = fs.readFileSync(filename, 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions,
    fileName: filename,
    reportDiagnostics: false,
  });
  module._compile(outputText, filename);
};
