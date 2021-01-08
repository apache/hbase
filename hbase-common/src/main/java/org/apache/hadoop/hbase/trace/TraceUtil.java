/**
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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Span.Kind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.attributes.SemanticAttributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class TraceUtil {

  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.contrib.hbase";

  public static final AttributeKey<String> NAMESPACE_KEY = SemanticAttributes.DB_HBASE_NAMESPACE;

  public static final AttributeKey<String> TABLE_KEY = AttributeKey.stringKey("db.hbase.table");

  public static final AttributeKey<List<String>> REGION_NAMES_KEY =
    AttributeKey.stringArrayKey("db.hbase.regions");

  public static final AttributeKey<String> RPC_SERVICE_KEY =
    AttributeKey.stringKey("db.hbase.rpc.service");

  public static final AttributeKey<String> RPC_METHOD_KEY =
    AttributeKey.stringKey("db.hbase.rpc.method");

  public static final AttributeKey<String> SERVER_NAME_KEY =
    AttributeKey.stringKey("db.hbase.server.name");

  public static final AttributeKey<String> REMOTE_HOST_KEY = SemanticAttributes.NET_PEER_NAME;

  public static final AttributeKey<Long> REMOTE_PORT_KEY = SemanticAttributes.NET_PEER_PORT;

  private TraceUtil() {
  }

  public static Tracer getGlobalTracer() {
    return GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
  }

  /**
   * Create a {@link Kind#INTERNAL} span.
   */
  public static Span createSpan(String name) {
    return createSpan(name, Kind.INTERNAL);
  }

  /**
   * Create a {@link Kind#INTERNAL} span and set table related attributes.
   */
  public static Span createTableSpan(String spanName, TableName tableName) {
    return createSpan(spanName).setAttribute(NAMESPACE_KEY, tableName.getNamespaceAsString())
      .setAttribute(TABLE_KEY, tableName.getNameAsString());
  }

  /**
   * Create a span with the given {@code kind}. Notice that, OpenTelemetry only expects one
   * {@link Kind#CLIENT} span and one {@link Kind#SERVER} span for a traced request, so use this
   * with caution when you want to create spans with kind other than {@link Kind#INTERNAL}.
   */
  private static Span createSpan(String name, Kind kind) {
    return getGlobalTracer().spanBuilder(name).setSpanKind(kind).startSpan();
  }

  /**
   * Create a span which parent is from remote, i.e, passed through rpc.
   * </p>
   * We will set the kind of the returned span to {@link Kind#SERVER}, as this should be the top
   * most span at server side.
   */
  public static Span createRemoteSpan(String name, Context ctx) {
    return getGlobalTracer().spanBuilder(name).setParent(ctx).setSpanKind(Kind.SERVER).startSpan();
  }

  /**
   * Trace an asynchronous operation for a table.
   */
  public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
    String spanName, TableName tableName) {
    Span span = createTableSpan(spanName, tableName);
    try (Scope scope = span.makeCurrent()) {
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
    try (Scope scope = span.makeCurrent()) {
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
    Supplier<List<CompletableFuture<T>>> action, String spanName, TableName tableName) {
    Span span = createTableSpan(spanName, tableName);
    try (Scope scope = span.makeCurrent()) {
      List<CompletableFuture<T>> futures = action.get();
      endSpan(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])), span);
      return futures;
    }
  }

  /**
   * Finish the {@code span} when the given {@code future} is completed.
   */
  private static void endSpan(CompletableFuture<?> future, Span span) {
    FutureUtils.addListener(future, (resp, error) -> {
      if (error != null) {
        span.recordException(error);
        span.setStatus(StatusCode.ERROR);
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end();
    });
  }

  public static void trace(Runnable action, String spanName) {
    trace(action, () -> createSpan(spanName));
  }

  public static void trace(Runnable action, Supplier<Span> creator) {
    Span span = creator.get();
    try (Scope scope = span.makeCurrent()) {
      action.run();
      span.setStatus(StatusCode.OK);
    } catch (Throwable e) {
      span.recordException(e);
      span.setStatus(StatusCode.ERROR);
    } finally {
      span.end();
    }
  }
}
