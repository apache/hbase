/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.trace;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.Version;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class TraceUtil {

  private TraceUtil() {
  }

  public static Tracer getGlobalTracer() {
    return GlobalOpenTelemetry.getTracer("org.apache.hbase", Version.version);
  }

  /**
   * Create a {@link SpanKind#INTERNAL} span.
   */
  public static Span createSpan(String name) {
    return createSpan(name, SpanKind.INTERNAL);
  }

  /**
   * Create a span with the given {@code kind}. Notice that, OpenTelemetry only expects one
   * {@link SpanKind#CLIENT} span and one {@link SpanKind#SERVER} span for a traced request, so use
   * this with caution when you want to create spans with kind other than {@link SpanKind#INTERNAL}.
   */
  private static Span createSpan(String name, SpanKind kind) {
    return getGlobalTracer().spanBuilder(name).setSpanKind(kind).startSpan();
  }

  /**
   * Create a span which parent is from remote, i.e, passed through rpc.
   * </p>
   * We will set the kind of the returned span to {@link SpanKind#SERVER}, as this should be the top
   * most span at server side.
   */
  public static Span createRemoteSpan(String name, Context ctx) {
    return getGlobalTracer().spanBuilder(name).setParent(ctx).setSpanKind(SpanKind.SERVER)
      .startSpan();
  }

  /**
   * Create a span with {@link SpanKind#CLIENT}.
   */
  public static Span createClientSpan(String name) {
    return createSpan(name, SpanKind.CLIENT);
  }

  /**
   * Trace an asynchronous operation for a table.
   */
  public static <T> CompletableFuture<T> tracedFuture(
    Supplier<CompletableFuture<T>> action,
    Supplier<Span> spanSupplier
  ) {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      CompletableFuture<T> future = action.get();
      endSpan(future, span);
      return future;
    }
  }

  /**
   * Trace an asynchronous operation.
   */
  public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
    String spanName) {
    Span span = createSpan(spanName);
    try (Scope ignored = span.makeCurrent()) {
      CompletableFuture<T> future = action.get();
      endSpan(future, span);
      return future;
    }
  }

  /**
   * Trace an asynchronous operation, and finish the create {@link Span} when all the given
   * {@code futures} are completed.
   */
  public static <T> List<CompletableFuture<T>> tracedFutures(
    Supplier<List<CompletableFuture<T>>> action,
    Supplier<Span> spanSupplier
  ) {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      List<CompletableFuture<T>> futures = action.get();
      endSpan(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])), span);
      return futures;
    }
  }

  public static void setError(Span span, Throwable error) {
    span.recordException(error);
    span.setStatus(StatusCode.ERROR);
  }

  /**
   * Finish the {@code span} when the given {@code future} is completed.
   */
  private static void endSpan(CompletableFuture<?> future, Span span) {
    FutureUtils.addListener(future, (resp, error) -> {
      if (error != null) {
        setError(span, error);
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end();
    });
  }

  /**
   * A {@link Runnable} that may also throw.
   * @param <T> the type of {@link Throwable} that can be produced.
   */
  @FunctionalInterface
  public interface ThrowingRunnable<T extends Throwable> {
    void run() throws T;
  }

  public static <T extends Throwable> void trace(
    final ThrowingRunnable<T> runnable,
    final String spanName) throws T {
    trace(runnable, () -> createSpan(spanName));
  }

  public static <T extends Throwable> void trace(
    final ThrowingRunnable<T> runnable,
    final Supplier<Span> spanSupplier
  ) throws T {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      runnable.run();
      span.setStatus(StatusCode.OK);
    } catch (Throwable e) {
      setError(span, e);
      throw e;
    } finally {
      span.end();
    }
  }

  /**
   * A {@link Callable} that may also throw.
   * @param <R> the result type of method call.
   * @param <T> the type of {@link Throwable} that can be produced.
   */
  @FunctionalInterface
  public interface ThrowingCallable<R, T extends Throwable> {
    R call() throws T;
  }

  public static <R, T extends Throwable> R trace(
    final ThrowingCallable<R, T> callable,
    final String spanName
  ) throws T {
    return trace(callable, () -> createSpan(spanName));
  }

  public static <R, T extends Throwable> R trace(
    final ThrowingCallable<R, T> callable,
    final Supplier<Span> spanSupplier
  ) throws T {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      final R ret = callable.call();
      span.setStatus(StatusCode.OK);
      return ret;
    } catch (Throwable e) {
      setError(span, e);
      throw e;
    } finally {
      span.end();
    }
  }
}
