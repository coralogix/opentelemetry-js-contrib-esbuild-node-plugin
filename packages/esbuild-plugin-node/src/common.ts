/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { ModuleParams } from './types';

export function wrapModule(
  originalSource: string,
  {
    path,
    moduleVersion,
    oTelInstrumentationPackage,
    oTelInstrumentationClass,
    instrumentationName,
    oTelInstrumentationConstructorArgs = '',
  }: ModuleParams
) {
  return `
(function() {
  ${originalSource}
})(...arguments);
{
  let mod = module.exports;

  const { satisfies } = require('semver');
  const { ${oTelInstrumentationClass} } = require('${oTelInstrumentationPackage}');
  const { diag } = require('@opentelemetry/api');
  const instrumentations = new ${oTelInstrumentationClass}(${oTelInstrumentationConstructorArgs}).getModuleDefinitions();

  if (instrumentations.length > 1 && !'${instrumentationName}') {
    diag.error('instrumentationName must be specified because ${oTelInstrumentationClass} has multiple instrumentations');
    return;
  }
  const instrumentation = ${
    instrumentationName
      ? `instrumentations.find(i => i.name === '${instrumentationName}')`
      : 'instrumentations[0]'
  };

  if (instrumentation.patch) {
    mod = instrumentation.patch(mod)
  }

  if (instrumentation.files?.length) {
    for (const file of instrumentation.files.filter(f => f.name === '${path}')) {
      if (!file.supportedVersions.some(v => satisfies('${moduleVersion}', v))) {
        diag.debug('Skipping instrumentation for ${path}@${moduleVersion} because it does not match supported versions ' + file.supportedVersions.join(','));
        continue;
      }
      mod = file.patch(mod, '${moduleVersion}');
    }
  }


  module.exports = mod;
}
`;
}
