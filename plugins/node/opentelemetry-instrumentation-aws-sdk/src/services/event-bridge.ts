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
import {
  context,
  defaultTextMapSetter,
  diag,
  propagation,
  Span,
  SpanKind,
  Tracer,
} from '@opentelemetry/api';
import { RequestMetadata, ServiceExtension } from './ServiceExtension';
import {
  AwsSdkInstrumentationConfig,
  NormalizedRequest,
  NormalizedResponse,
} from '../types';
import {
  PutEventsRequest,
  PutEventsRequestEntry,
} from 'aws-sdk/clients/eventbridge';

const CONTEXT_KEY = '_context';
type EventBridgeDetailWithContext = { [CONTEXT_KEY]?: object } & object;

export class EventBridgeServiceExtension implements ServiceExtension {
  requestPreSpanHook(_request: NormalizedRequest): RequestMetadata {
    return { isIncoming: false, spanKind: SpanKind.PRODUCER };
  }

  requestPostSpanHook = (request: NormalizedRequest) => {
    if (request.commandName === 'PutEvents') {
      const putEventsRequest = request.commandInput as PutEventsRequest;
      putEventsRequest.Entries.forEach(entry => {
        const details = this.getDetailsFromEvent(entry);
        if (details === undefined) {
          // failure in parsing details
          return;
        }

        if (CONTEXT_KEY in details) {
          diag.warn(
            `EventBridge instrumentation: cannot set context propagation on EventBridge events due to context key (${CONTEXT_KEY}) already exists`
          );
          return;
        }

        // propagate context inside key
        details[CONTEXT_KEY] = {};
        propagation.inject(
          context.active(),
          details[CONTEXT_KEY],
          defaultTextMapSetter
        );
        entry.Detail = JSON.stringify(details);
      });
    }
  };

  responseHook = (
    _request: NormalizedResponse,
    _span: Span,
    _tracer: Tracer,
    _config: AwsSdkInstrumentationConfig
  ) => {};

  private getDetailsFromEvent(
    entry: PutEventsRequestEntry
  ): EventBridgeDetailWithContext | undefined {
    if (typeof entry.Detail === 'object') {
      return entry.Detail as EventBridgeDetailWithContext;
    }

    if (entry.Detail === undefined) {
      diag.warn(
        'EventBridge instrumentation: cannot set context propagation on EventBridge event due to event details are undefined'
      );
      return undefined;
    }

    try {
      return JSON.parse(entry.Detail) as EventBridgeDetailWithContext;
    } catch (error) {
      diag.warn(
        `EventBridge instrumentation: cannot set context propagation on EventBridge event due to event Detail is not a valid json: ${entry.Detail}`
      );
      return undefined;
    }
  }
}
