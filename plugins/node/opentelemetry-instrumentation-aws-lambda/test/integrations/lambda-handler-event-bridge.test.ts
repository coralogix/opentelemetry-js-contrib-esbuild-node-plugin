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

// We access through node_modules to allow it to be patched.
/* eslint-disable node/no-extraneous-require */

import * as path from 'path';

import {
  AwsLambdaInstrumentation,
  AwsLambdaInstrumentationConfig,
} from '../../src';
import {
  BatchSpanProcessor,
  InMemorySpanExporter,
  ReadableSpan,
} from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { Context, EventBridgeEvent } from 'aws-lambda';
import * as assert from 'assert';
import {
  defaultTextMapSetter,
  propagation,
  ROOT_CONTEXT,
  SpanContext,
  SpanKind,
  trace,
  TraceFlags,
} from '@opentelemetry/api';
import { assertSpanSuccess } from './lambda-handler.test';

import { CONTEXT_KEY as EVENT_BRIDGE_CONTEXT_KEY } from '../../src/triggers/event-bridge';

const memoryExporter = new InMemorySpanExporter();
const provider = new NodeTracerProvider();
provider.addSpanProcessor(new BatchSpanProcessor(memoryExporter));
provider.register();

const exampleEvent: EventBridgeEvent<string, any> = {
  version: '0',
  id: 'dfb5be99-12f4-c641-3e2d-da5a6d1981b8',
  'detail-type': 'appRequestSubmitted',
  source: 'source-example',
  account: '3841334213',
  time: '2023-09-18T11:05:44Z',
  region: 'eu-west-1',
  resources: ['RESOURCE_ARN'],
  detail: { key1: 'value1', key2: 'value2' },
};

const detailsWithPropagation = { ...exampleEvent.detail };
detailsWithPropagation[EVENT_BRIDGE_CONTEXT_KEY] = {};

// xray context propagation
const TRACE_ID = '8a3c60f7d188f8fa79d48a391a778fa6';
const SPAN_ID = '53995c3f42cd8ad8';
const SAMPLED_TRACE_FLAG = TraceFlags.SAMPLED;

const linkContext: SpanContext = {
  traceId: TRACE_ID,
  spanId: SPAN_ID,
  traceFlags: SAMPLED_TRACE_FLAG,
  isRemote: true,
};

propagation.inject(
  trace.setSpan(ROOT_CONTEXT, trace.wrapSpanContext(linkContext)),
  detailsWithPropagation[EVENT_BRIDGE_CONTEXT_KEY],
  defaultTextMapSetter
);

const exampleEventWithLink: EventBridgeEvent<string, any> = {
  ...exampleEvent,
  detail: detailsWithPropagation,
};
const assertEventBridgeSpan = (
  span: ReadableSpan,
  ebEvent: EventBridgeEvent<string, any>
) => {
  assert.strictEqual(span.kind, SpanKind.SERVER);

  assert.strictEqual(
    span.attributes['aws.event.bridge.trigger.source'],
    ebEvent.source
  );

  assert.strictEqual(span.name, ebEvent['detail-type'] ?? 'event bridge event');
};

describe('Event Bridge handler', () => {
  let instrumentation: AwsLambdaInstrumentation;

  let oldEnv: NodeJS.ProcessEnv;

  const ctx = {
    functionName: 'my_function',
    invokedFunctionArn: 'my_arn',
    awsRequestId: 'aws_request_id',
  } as Context;

  const initializeHandler = (
    handler: string,
    config: AwsLambdaInstrumentationConfig = {
      detectTrigger: true,
    }
  ) => {
    process.env._HANDLER = handler;

    instrumentation = new AwsLambdaInstrumentation(config);
    instrumentation.setTracerProvider(provider);
  };

  const lambdaRequire = (module: string) =>
    require(path.resolve(__dirname, '..', module));

  beforeEach(() => {
    oldEnv = { ...process.env };
    process.env.LAMBDA_TASK_ROOT = path.resolve(__dirname, '..');
  });

  afterEach(() => {
    process.env = oldEnv;
    instrumentation.disable();

    memoryExporter.reset();
  });

  describe('event bridge span tests', () => {
    it('should export two valid span', async () => {
      initializeHandler('lambda-test/event-bridge.handler');

      await lambdaRequire('lambda-test/event-bridge').handler(
        exampleEvent,
        ctx
      );

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [spanLambda, ebSpan] = spans;
      assertSpanSuccess(spanLambda);
      assertEventBridgeSpan(ebSpan, exampleEventWithLink);
      assert.strictEqual(ebSpan.parentSpanId, undefined);
      assert.strictEqual(spanLambda.parentSpanId, ebSpan.spanContext().spanId);
    });

    it(`EventBridge span links should be extracted from event details under the '${EVENT_BRIDGE_CONTEXT_KEY}' key`, async () => {
      initializeHandler('lambda-test/event-bridge.handler');

      await lambdaRequire('lambda-test/event-bridge').handler(
        exampleEventWithLink,
        ctx
      );

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [_, ebSpan] = spans;
      assertEventBridgeSpan(ebSpan, exampleEventWithLink);
      assert.strictEqual(ebSpan.links.length, 1);
      const {
        links: [link],
      } = ebSpan;

      assert.deepStrictEqual(link.context, linkContext);
    });
  });
});
