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
import { EventBridgeEvent } from 'aws-lambda';
import {
  Attributes,
  Link,
  SpanKind,
  diag,
  propagation,
  trace,
} from '@opentelemetry/api';
import { LambdaTrigger, TriggerSpanInitializerResult } from './common';
import { TriggerOrigin } from './index';
import { SpanOptions } from '@opentelemetry/api/build/src/trace/SpanOptions';
import { context as otelContext } from '@opentelemetry/api/build/src/context-api';
import { defaultTextMapGetter } from '@opentelemetry/api/build/src/propagation/TextMapPropagator';

export const CONTEXT_KEY = '_context';
type EventBridgeDetailWithContext = { [CONTEXT_KEY]?: object } & object;

const isEventBridgeEvent = (
  event: any
): event is EventBridgeEvent<string, any> => {
  return (
    event &&
    typeof event === 'object' &&
    'source' in event &&
    typeof event.source === 'string'
  );
};

const getDetailsFromEvent = (
  entry: EventBridgeEvent<string, any>
): EventBridgeDetailWithContext | undefined => {
  if (typeof entry.detail === 'object') {
    return entry.detail as EventBridgeDetailWithContext;
  }

  if (entry.detail === undefined) {
    diag.debug(
      'EventBridge instrumentation: cannot extract propagated context from EventBridge event due to event details are undefined'
    );
    return undefined;
  }

  try {
    return JSON.parse(entry.detail) as EventBridgeDetailWithContext;
  } catch (error) {
    diag.debug(
      `EventBridge instrumentation: cannot extract propagated context from EventBridge event due to event Detail is not a valid json: ${entry.detail}`
    );
    return undefined;
  }
};

const extractEventBridgeLink = (
  event: EventBridgeEvent<string, any>
): Link | undefined => {
  const parsedDetails = getDetailsFromEvent(event);
  if (parsedDetails === undefined) {
    // failed to parse event details
    return undefined;
  }

  if (!parsedDetails._context) {
    diag.debug(
      `EventBridge instrumentation: cannot extract propagated context from EventBridge event due to context key (${CONTEXT_KEY}) does not exist in event details`
    );
    return undefined;
  }

  const extractedContext = propagation.extract(
    otelContext.active(),
    parsedDetails[CONTEXT_KEY],
    defaultTextMapGetter
  );
  const context = trace.getSpan(extractedContext)?.spanContext();
  if (!context) return undefined;
  return { context };
};

function initializeEventBridgeSpan(
  event: EventBridgeEvent<string, any>
): TriggerSpanInitializerResult {
  const attributes: Attributes = {
    'aws.event.bridge.trigger.source': event.source,
  };
  const name = event['detail-type'] ?? 'event bridge event';
  const link = extractEventBridgeLink(event);
  const links = link ? [link] : undefined;
  const options: SpanOptions = {
    kind: SpanKind.CONSUMER,
    attributes,
    links,
  };

  return { name, options, origin: TriggerOrigin.EVENT_BRIDGE };
}

export const EventBridgeTrigger: LambdaTrigger<EventBridgeEvent<string, any>> =
  {
    validator: isEventBridgeEvent,
    initializer: initializeEventBridgeSpan,
  };
