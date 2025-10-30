declare const require: undefined | ((moduleName: string) => unknown);

type ModuleNotFoundError = Error & { code?: string; moduleName?: string };

function isModuleNotFoundError(error: ModuleNotFoundError, moduleName: string): boolean {
  if (!error) {
    return false;
  }

  if (error.code === 'MODULE_NOT_FOUND' || error.code === 'ERR_MODULE_NOT_FOUND') {
    if (!error.message) {
      return true;
    }
    return error.message.includes(`'${moduleName}'`) || error.message.includes(`"${moduleName}"`);
  }

  return false;
}

export function loadOptionalModule<T>(moduleName: string): T | undefined {
  if (typeof require !== 'function') {
    return undefined;
  }

  try {
    return require(moduleName) as T;
  } catch (error) {
    const err = error as ModuleNotFoundError;
    if (isModuleNotFoundError(err, moduleName)) {
      return undefined;
    }
    throw error;
  }
}

export async function importOptionalModule<T>(moduleName: string): Promise<T | undefined> {
  const syncModule = loadOptionalModule<T>(moduleName);
  if (syncModule) {
    return syncModule;
  }

  try {
    const importer = new Function('moduleName', 'return import(moduleName);') as (
      name: string
    ) => Promise<T>;
    return await importer(moduleName);
  } catch (error) {
    const err = error as ModuleNotFoundError;
    if (isModuleNotFoundError(err, moduleName)) {
      return undefined;
    }
    if (err instanceof SyntaxError) {
      throw new Error(
        `Dynamic import is not supported in this environment. Install "${moduleName}" manually and provide a configured instance.`
      );
    }
    throw error;
  }
}
