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
  InstrumentationBase,
  safeExecuteInTheMiddle,
} from '@opentelemetry/instrumentation';
import {
  Context as OtelContext,
  context as otelContext,
  diag,
  propagation,
  ROOT_CONTEXT,
  MeterProvider,
  Span,
  SpanKind,
  SpanStatusCode,
  TextMapGetter,
  trace,
  TraceFlags,
  TracerProvider,
} from '@opentelemetry/api';
import {
  AWSXRAY_TRACE_ID_HEADER,
  AWSXRayPropagator,
} from '@opentelemetry/propagator-aws-xray';
import {SEMATTRS_FAAS_EXECUTION} from '@opentelemetry/semantic-conventions';

import {
  APIGatewayProxyEventHeaders,
  Callback,
  Context,
  Handler,
} from 'aws-lambda';

import { AwsLambdaInstrumentationConfig } from './types';
import { VERSION } from './version';
import { env } from 'process';
import {
  finalizeSpan,
  getEventTrigger,
  LambdaAttributes,
  TriggerOrigin,
} from './triggers';
import { ReadableSpan } from '@opentelemetry/sdk-trace-base';

diag.debug("Loading AwsLambdaInstrumentation")

const awsPropagator = new AWSXRayPropagator();
const headerGetter: TextMapGetter<APIGatewayProxyEventHeaders> = {
  keys(carrier): string[] {
    return Object.keys(carrier);
  },
  get(carrier, key: string) {
    return carrier[key];
  },
};

export const traceContextEnvironmentKey = '_X_AMZN_TRACE_ID';
export const xForwardProto = 'X-Forwarded-Proto';
const SPAN_STATE_ATTRIBUTE = 'cx.internal.span.state';
const TRACE_ID_ATTRIBUTE = 'cx.internal.trace.id';
const SPAN_ID_ATTRIBUTE = 'cx.internal.span.id';
const SPAN_ROLE_ATTRIBUTE = 'cx.internal.span.role';

type InstrumentationContext = { 
  triggerOrigin: TriggerOrigin | undefined; 
  triggerSpan: Span | undefined; 
  invocationSpan: Span; 
  invocationParentContext: OtelContext; 
}

export class AwsLambdaInstrumentation extends InstrumentationBase {
  private _traceForceFlusher?: () => Promise<void>;
  private _metricForceFlusher?: () => Promise<void>;

  constructor(protected override _config: AwsLambdaInstrumentationConfig = {}) {
    super('@opentelemetry/instrumentation-aws-lambda', VERSION, _config);
    if (this._config.disableAwsContextPropagation == null) {
      if (
        typeof env['OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION'] ===
          'string' &&
        env[
          'OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION'
        ].toLocaleLowerCase() === 'true'
      ) {
        this._config.disableAwsContextPropagation = true;
      }
    }
  }

  override setConfig(config: AwsLambdaInstrumentationConfig = {}) {
    this._config = config;
  }

  init() {
    return [];
  }

  public getPatchHandler(original: Handler): Handler {
    diag.debug('patching handler function');
    const self = this;

    return function patchedHandler(
      this: never,
      // The event can be a user type, it truly is any.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      event: any,
      context: Context,
      callback: Callback
    ) {

      self._before_execution(event, context).then(
        (instrCtx) => {
          otelContext.with(
            trace.setSpan(instrCtx.invocationParentContext, instrCtx.invocationSpan),
            () => {
              // Lambda seems to pass a callback even if handler is of Promise form, so we wrap all the time before calling
              // the handler and see if the result is a Promise or not. In such a case, the callback is usually ignored. If
              // the handler happened to both call the callback and complete a returned Promise, whichever happens first will
              // win and the latter will be ignored.
              const wrappedCallback = self._wrapCallback(callback, instrCtx);
    
              let maybePromise: any;
              try {
                maybePromise = original.apply(this, [event, context, wrappedCallback])
              } catch (err: any) {
                // Catching synchronous failures
                diag.debug('handler threw synchronously');
                void self._after_execution(instrCtx, err, undefined);
                context.callbackWaitsForEmptyEventLoop = false;
                callback(err, undefined);
                return;
              }

              if (typeof maybePromise?.then === 'function') {
                diag.debug('handler returned a promise');
                // Promise based async handler
                maybePromise.then(
                  (value: any) => {
                    diag.debug('handler promise completed');
                    self._after_execution(instrCtx, undefined, value);
                    context.callbackWaitsForEmptyEventLoop = false;
                    callback(undefined, value);
                  },
                  (err: Error | string) => {
                    diag.debug('handler promise failed');
                    self._after_execution(instrCtx, err, undefined);
                    context.callbackWaitsForEmptyEventLoop = false;
                    callback(err, undefined);
                  }
                );
              } else {
                diag.debug('handler returned synchronously (callback based)');
              }
            }
          );
        },
        (err) => {
          diag.error('_before_execution failed', err);
          self._after_execution(undefined, err, undefined);
          context.callbackWaitsForEmptyEventLoop = false;
          callback(err, undefined);
        }
      )
    }
  }

  private async _before_execution(
    event: any,
    context: Context,
  ): Promise<InstrumentationContext> {

    const upstreamContext = this._determineUpstreamContext(event, context);

    const { triggerSpan, triggerOrigin } = this._startTriggerSpan(event, upstreamContext) ?? {};

    let invocationParentContext;
    if (triggerSpan) {
      invocationParentContext = trace.setSpan(upstreamContext, triggerSpan);
    } else {
      invocationParentContext = upstreamContext
    }

    const invocationSpan = this._startInvocationSpan(event, context, invocationParentContext);

    diag.info(`upstream: ${trace.getSpan(upstreamContext)?.spanContext().spanId} trigger: ${triggerSpan?.spanContext().spanId} invocationSpan: ${invocationSpan.spanContext().spanId}`)

    await this._sendEarlySpans(upstreamContext, triggerSpan, invocationParentContext, invocationSpan);

    return {triggerOrigin, triggerSpan, invocationSpan, invocationParentContext}
  }

  // never fails
  private async _after_execution(
    context: InstrumentationContext | undefined,
    err:  string | Error | null | undefined, 
    res: any,
  ): Promise<void> {
    try {
      const plugin = this;
      if (context?.invocationSpan) {
        plugin._applyResponseHook(context.invocationSpan, err, res);
        plugin._endInvocationSpan(context.invocationSpan, err);
      }
      if (context?.triggerSpan) {
        plugin._endTriggerSpan(context.triggerSpan, context.triggerOrigin, res, err);
      }
    } catch (e) {
      diag.error('Error in _after_execution', e);
    }

    await this._flush()
  }

  // never fails
  private async _sendEarlySpans(
    triggerParentContext: OtelContext,
    triggerSpan: Span | undefined,
    invocationParentContext: OtelContext,
    invocationSpan: Span,
  ) {
    try {
      if (triggerSpan && ((triggerSpan as unknown as ReadableSpan).kind !== undefined)) {
        const earlyTrigger = this._createEarlySpan(triggerParentContext, triggerSpan as unknown as ReadableSpan);
        earlyTrigger.end();
      }

      if (invocationSpan && ((invocationSpan as unknown as ReadableSpan).kind !== undefined)) {
        const earlyTrigger = this._createEarlySpan(invocationParentContext, invocationSpan as unknown as ReadableSpan);
        earlyTrigger.end();
      }
    } catch (e) {
      diag.warn('Failed to prepare early spans', e);
    }
    await this._flush_trace();
  }

  private _createEarlySpan(
    parentContext: OtelContext,
    span: ReadableSpan,
  ): Span {
    const earlySpan = this.tracer.startSpan(
      span.name, 
      {
        startTime: span.startTime,
        kind: span.kind,
        attributes: span.attributes,
        links: span.links,
      },
      parentContext
    );

    const attributes = span.attributes;
    for (const [key, value] of Object.entries(attributes)) {
      if (value) {
        earlySpan.setAttribute(key, value);
      }
    }

    const events = span.events;
    for (const event of events) {
      earlySpan.addEvent(event.name, event.attributes, event.time);
    }

    earlySpan.setAttribute(SPAN_STATE_ATTRIBUTE, 'early');
    earlySpan.setAttribute(TRACE_ID_ATTRIBUTE, span.spanContext().traceId);
    earlySpan.setAttribute(SPAN_ID_ATTRIBUTE, span.spanContext().spanId);

    return earlySpan;
  }

  private _startTriggerSpan(
    event: unknown,
    parentContext: OtelContext
  ): { triggerOrigin: TriggerOrigin; triggerSpan: Span } | undefined {
    if (this._config.detectTrigger === false) {
      return undefined;
    }
    const trigger = getEventTrigger(event);
    if (!trigger) {
      return undefined;
    }
    const { name, options, origin } = trigger;
    if (!options.attributes) {
      options.attributes = {};
    }
    options.attributes[LambdaAttributes.TRIGGER_SERVICE] = origin;
    options.attributes[SPAN_ROLE_ATTRIBUTE] = 'trigger';
    const triggerSpan = this.tracer.startSpan(name, options, parentContext);
    return { triggerOrigin: origin, triggerSpan };
  }

  private _endTriggerSpan(
    span: Span,
    triggerOrigin: TriggerOrigin | undefined,
    lambdaResponse: any,
    errorFromLambda: string | Error | null | undefined,
  ): void {
    if (span.isRecording()) {
      if (errorFromLambda) {
        span.recordException(errorFromLambda);

        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: this._errorToString(errorFromLambda),
        });

        span.end();
        return;
      }

      if (triggerOrigin) {
        finalizeSpan(this._config, triggerOrigin, span, lambdaResponse);
      }
      span.end();
    } else {
      diag.debug('Ending wrapper span for the second time');
    }
  }

  private _startInvocationSpan(event: any, context: Context, invocationParentContext: OtelContext): Span {
    const invocationSpan = this.tracer.startSpan(
      context.functionName,
      {
        kind: SpanKind.SERVER,
        attributes: {
          [SEMATTRS_FAAS_EXECUTION]: context.awsRequestId,
          [SPAN_ROLE_ATTRIBUTE]: 'invocation',
        },
      },
      invocationParentContext
    );

    if (this._config.requestHook) {
      try {
        this._config.requestHook!(invocationSpan, { event, context })
      } catch (e) {
        diag.error('aws-lambda instrumentation: requestHook error', e)
      }
    }
    return invocationSpan;
  }

  private _endInvocationSpan(span: Span, err: string | Error | null | undefined): void {
    if (span.isRecording()) {
      if (err) {
        span.recordException(err);
      }

      const errMessage = this._errorToString(err);
      if (errMessage) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: errMessage,
        });
      }
      span.end();
    } else {
      diag.debug('Ending span for the second time');
    }
  }

  private _wrapCallback(
    originalAWSLambdaCallback: Callback,
    instrumentationContext: InstrumentationContext
  ): Callback {
    return (err, res) => {
      diag.debug('executing wrapped callback function');
      this._after_execution(instrumentationContext, err, res).then(() => {
          diag.debug('executing original callback function');
          originalAWSLambdaCallback.apply(this, [err, res]); // End of the function
      });
    };
  }

  // never fails
  private async _flush(): Promise<void> {
      await Promise.all([
        this._flush_trace(),
        this._flush_metric()
      ]);
  }

  // never fails
  private async _flush_trace(): Promise<void> {
    if (this._traceForceFlusher) {
      try {
        await this._traceForceFlusher();
      } catch (e) {
        diag.error('Error while flushing traces', e)
      }
    } else {
      diag.error(
        'Spans may not be exported for the lambda function because we are not force flushing before callback.'
      );
    }
  }

    // never fails
  private async _flush_metric(): Promise<void> {
    if (this._metricForceFlusher) {
      try {
        await this._metricForceFlusher();
      } catch (e) {
        diag.error('Error while flushing metrics', e)
      }
    } else {
      diag.error(
        'Metrics may not be exported for the lambda function because we are not force flushing before callback.'
      );
    }
  }

  private _errorToString(err: string | Error | null | undefined): string | undefined {
    let errMessage;
    if (typeof err === 'string') {
      errMessage = err;
    } else if (err) {
      errMessage = err.message;
    }
    return errMessage;
  }

  override setTracerProvider(tracerProvider: TracerProvider): void {
    super.setTracerProvider(tracerProvider);
    this._traceForceFlusher = this._traceForceFlush(tracerProvider);
  }

  private _traceForceFlush(tracerProvider: TracerProvider): (() => Promise<void>) | undefined {
    if (!tracerProvider) return undefined;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let currentProvider: any = tracerProvider;

    if (typeof currentProvider.getDelegate === 'function') {
      currentProvider = currentProvider.getDelegate();
    }

    if (typeof currentProvider.forceFlush === 'function') {
      return currentProvider.forceFlush.bind(currentProvider);
    }

    return undefined;
  }

  override setMeterProvider(meterProvider: MeterProvider): void {
    super.setMeterProvider(meterProvider);
    this._metricForceFlusher = this._metricForceFlush(meterProvider);
  }

  private _metricForceFlush(meterProvider: MeterProvider): (() => Promise<void>) | undefined {
    if (!meterProvider) return undefined;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const currentProvider: any = meterProvider;

    if (typeof currentProvider.forceFlush === 'function') {
      return currentProvider.forceFlush.bind(currentProvider);
    }

    return undefined;
  }

  private _applyResponseHook(
    span: Span,
    err?: Error | string | null,
    res?: any
  ): void {
    if (this._config?.responseHook) {
      safeExecuteInTheMiddle(
        () => this._config.responseHook!(span, { err, res }),
        e => {
          if (e)
            diag.error('aws-lambda instrumentation: responseHook error', e);
        },
        true
      );
    }
  }

  private static _defaultEventContextExtractor(event: any): OtelContext {
    // The default extractor tries to get sampled trace header from HTTP headers.
    const httpHeaders = event.headers || {};
    return propagation.extract(ROOT_CONTEXT, httpHeaders, headerGetter);
  }

  private _determineUpstreamContext(
    event: any,
    context: Context,
  ): OtelContext {
    let parent: OtelContext | undefined = undefined;
    if (!this._config.disableAwsContextPropagation) {
      const lambdaTraceHeader = process.env[traceContextEnvironmentKey];
      if (lambdaTraceHeader) {
        parent = awsPropagator.extract(
          ROOT_CONTEXT,
          { [AWSXRAY_TRACE_ID_HEADER]: lambdaTraceHeader },
          headerGetter
        );
      }
      if (parent) {
        const spanContext = trace.getSpan(parent)?.spanContext();
        if (
          spanContext &&
          (spanContext.traceFlags & TraceFlags.SAMPLED) === TraceFlags.SAMPLED
        ) {
          // Trace header provided by Lambda only sampled if a sampled context was propagated from
          // an upstream cloud service such as S3, or the user is using X-Ray. In these cases, we
          // need to use it as the parent.
          return parent;
        }
      }
    }
    const eventContextExtractor = this._config.eventContextExtractor || AwsLambdaInstrumentation._defaultEventContextExtractor
    const extractedContext = safeExecuteInTheMiddle(
      () => eventContextExtractor(event, context),
      e => {
        if (e)
          diag.error(
            'aws-lambda instrumentation: eventContextExtractor error',
            e
          );
      },
      true
    );
    if (extractedContext && trace.getSpan(extractedContext)?.spanContext()) {
      return extractedContext;
    }
    if (!parent) {
      // No context in Lambda environment or HTTP headers.
      return ROOT_CONTEXT;
    }
    return parent;
  }
}
