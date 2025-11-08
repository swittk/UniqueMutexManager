const fs = require('fs');
const path = require('path');
const ts = require('typescript');

const projectRoot = path.join(__dirname, '..');
const esmDir = path.join(projectRoot, 'dist', 'esm');
const typesDir = path.join(projectRoot, 'dist', 'types');
const esmPackageJsonPath = path.join(esmDir, 'package.json');

if (!fs.existsSync(esmDir)) {
  throw new Error(
    `Expected ESM output directory at ${esmDir} to exist. Did the TypeScript build succeed?`,
  );
}

const existingPackageJson = fs.existsSync(esmPackageJsonPath)
  ? JSON.parse(fs.readFileSync(esmPackageJsonPath, 'utf8'))
  : {};

if (existingPackageJson.type !== 'module') {
  fs.writeFileSync(esmPackageJsonPath, JSON.stringify({ type: 'module' }, null, 2) + '\n');
}

const RELATIVE_SPECIFIER_PATTERN = /^\.{1,2}\//;
const JS_EXTENSIONS = new Set(['.js', '.cjs', '.mjs']);

function withJsExtension(specifier) {
  if (!RELATIVE_SPECIFIER_PATTERN.test(specifier)) {
    return specifier;
  }

  const [pathPart, queryPart] = specifier.split('?');
  const extension = path.extname(pathPart);

  if (JS_EXTENSIONS.has(extension)) {
    return specifier;
  }

  const updatedPath = `${pathPart}.js`;
  return queryPart ? `${updatedPath}?${queryPart}` : updatedPath;
}

function updateStringLiteral(node) {
  const text = node.text;
  const updated = withJsExtension(text);

  if (updated === text) {
    return null;
  }

  return ts.factory.createStringLiteral(updated);
}

function createSpecifierTransformer() {
  let mutated = false;

  const transformer = (context) => {
    const visit = (node) => {
      if (ts.isImportDeclaration(node) && ts.isStringLiteral(node.moduleSpecifier)) {
        const updatedLiteral = updateStringLiteral(node.moduleSpecifier);
        if (updatedLiteral) {
          mutated = true;
          return ts.factory.updateImportDeclaration(
            node,
            node.modifiers,
            node.importClause,
            updatedLiteral,
            node.assertClause ?? node.attributes,
          );
        }
      }

      if (ts.isExportDeclaration(node) && node.moduleSpecifier && ts.isStringLiteral(node.moduleSpecifier)) {
        const updatedLiteral = updateStringLiteral(node.moduleSpecifier);
        if (updatedLiteral) {
          mutated = true;
          return ts.factory.updateExportDeclaration(
            node,
            node.modifiers,
            node.isTypeOnly,
            node.exportClause,
            updatedLiteral,
            node.assertClause ?? node.attributes,
          );
        }
      }

      if (
        ts.isCallExpression(node) &&
        node.expression.kind === ts.SyntaxKind.ImportKeyword &&
        node.arguments.length === 1 &&
        ts.isStringLiteral(node.arguments[0])
      ) {
        const updatedLiteral = updateStringLiteral(node.arguments[0]);
        if (updatedLiteral) {
          mutated = true;
          return ts.factory.updateCallExpression(
            node,
            node.expression,
            node.typeArguments,
            [updatedLiteral],
          );
        }
      }

      if (
        ts.isImportTypeNode(node) &&
        node.argument &&
        ts.isLiteralTypeNode(node.argument) &&
        ts.isStringLiteral(node.argument.literal)
      ) {
        const updatedLiteral = updateStringLiteral(node.argument.literal);
        if (updatedLiteral) {
          mutated = true;
          return ts.factory.updateImportTypeNode(
            node,
            ts.factory.updateLiteralTypeNode(node.argument, updatedLiteral),
            node.qualifier,
            node.typeArguments,
            node.isTypeOf,
          );
        }
      }

      return ts.visitEachChild(node, visit, context);
    };

    return (node) => ts.visitNode(node, visit);
  };

  return { transformer, didMutate: () => mutated };
}

function rewriteFileSpecifiers(filePath, scriptKind) {
  const sourceText = fs.readFileSync(filePath, 'utf8');
  const sourceFile = ts.createSourceFile(
    filePath,
    sourceText,
    ts.ScriptTarget.ES2020,
    true,
    scriptKind,
  );

  const { transformer, didMutate } = createSpecifierTransformer();
  const result = ts.transform(sourceFile, [transformer]);

  try {
    if (!didMutate()) {
      return;
    }

    const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
    const updatedText = printer.printFile(result.transformed[0]);

    if (updatedText !== sourceText) {
      fs.writeFileSync(filePath, updatedText);
    }
  } finally {
    result.dispose();
  }
}

function walkAndRewrite(rootDir, matcher, scriptKind) {
  if (!fs.existsSync(rootDir)) {
    return;
  }

  const entries = fs.readdirSync(rootDir, { withFileTypes: true });

  for (const entry of entries) {
    const entryPath = path.join(rootDir, entry.name);

    if (entry.isDirectory()) {
      walkAndRewrite(entryPath, matcher, scriptKind);
      continue;
    }

    if (matcher(entry.name)) {
      rewriteFileSpecifiers(entryPath, scriptKind);
    }
  }
}

walkAndRewrite(esmDir, (name) => name.endsWith('.js'), ts.ScriptKind.JS);
walkAndRewrite(typesDir, (name) => name.endsWith('.d.ts'), ts.ScriptKind.TS);
