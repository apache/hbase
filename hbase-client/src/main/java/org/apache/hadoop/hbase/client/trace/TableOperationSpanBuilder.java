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

package org.apache.hadoop.hbase.client.trace;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionCoprocessorServiceExec;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.trace.SemanticAttributes;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct {@link io.opentelemetry.api.trace.Span} instances originating from client activity.
 */
@InterfaceAudience.Private
public class TableOperationSpanBuilder implements Supplier<Span> {

  private static final String unknown = "UNKNOWN";

  String name;
  SpanKind spanKind;
  TableName tableName;
  final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  public TableOperationSpanBuilder(final ClusterConnection connection) {
    attributes.put(SemanticAttributes.DB_SYSTEM, SemanticAttributes.DB_SYSTEM_VALUE);
    attributes.put(SemanticAttributes.DB_CONNECTION_STRING,
      connection.getConnectionRegistry().getConnectionString());
    attributes.put(SemanticAttributes.DB_USER, connection.getUser());
    if (connection instanceof ConnectionUtils.ShortCircuitingClusterConnection) {
      spanKind = SpanKind.INTERNAL;
    }
  }

  public TableOperationSpanBuilder(final AsyncConnectionImpl connection) {
    attributes.put(SemanticAttributes.DB_SYSTEM, SemanticAttributes.DB_SYSTEM_VALUE);
    attributes.put(SemanticAttributes.DB_CONNECTION_STRING,
      connection.getConnectionRegistry().getConnectionString());
    attributes.put(SemanticAttributes.DB_USER, connection.getUser());
  }

  @Override public Span get() {
    return build();
  }

  public TableOperationSpanBuilder setName(final String name) {
    this.name = name;
    return this;
  }

  public TableOperationSpanBuilder setSpanKind(final SpanKind spanKind) {
    this.spanKind = spanKind;
    return this;
  }

  public TableOperationSpanBuilder setOperation(final Scan scan) {
    return setOperation(valueFrom(scan));
  }

  public TableOperationSpanBuilder setOperation(final Row row) {
    return setOperation(valueFrom(row));
  }

  public TableOperationSpanBuilder setOperation(final SemanticAttributes.Operation operation) {
    attributes.put(SemanticAttributes.DB_OPERATION, operation.name());
    return this;
  }

  public TableOperationSpanBuilder setContainerOperations(final Row... rows) {
    final SemanticAttributes.Operation[] ops = Arrays.stream(rows)
      .map(TableOperationSpanBuilder::valueFrom)
      .toArray(SemanticAttributes.Operation[]::new);
    return setContainerOperations(ops);
  }

  public TableOperationSpanBuilder setContainerOperations(
    final Collection<? extends Row> operations
  ) {
    final SemanticAttributes.Operation[] ops = operations.stream()
      .map(TableOperationSpanBuilder::valueFrom)
      .toArray(SemanticAttributes.Operation[]::new);
    return setContainerOperations(ops);
  }

  private static Set<SemanticAttributes.Operation> unpackRowOperations(final Row row) {
    final Set<SemanticAttributes.Operation> ops = new HashSet<>();
    if (row instanceof CheckAndMutate) {
      final CheckAndMutate cam = (CheckAndMutate) row;
      ops.addAll(unpackRowOperations(cam));
    }
    if (row instanceof RowMutations) {
      final RowMutations mutations = (RowMutations) row;
      ops.addAll(unpackRowOperations(mutations));
    }
    return ops;
  }

  private static Set<SemanticAttributes.Operation> unpackRowOperations(final CheckAndMutate cam) {
    final Set<SemanticAttributes.Operation> ops = new HashSet<>();
    final SemanticAttributes.Operation op = valueFrom(cam.getAction());
    switch (op) {
      case BATCH:
      case CHECK_AND_MUTATE:
        ops.addAll(unpackRowOperations(cam.getAction()));
        break;
      default:
        ops.add(op);
    }
    return ops;
  }

  public TableOperationSpanBuilder setContainerOperations(
    final SemanticAttributes.Operation... operations
  ) {
    final List<String> ops = Arrays.stream(operations)
      .map(op -> op == null ? unknown : op.name())
      .sorted()
      .distinct()
      .collect(Collectors.toList());
    attributes.put(SemanticAttributes.CONTAINER_DB_OPERATIONS_KEY, ops);
    return this;
  }

  public TableOperationSpanBuilder setTableName(final TableName tableName) {
    this.tableName = tableName;
    attributes.put(SemanticAttributes.NAMESPACE_KEY, tableName.getNamespaceAsString());
    attributes.put(SemanticAttributes.DB_NAME, tableName.getNamespaceAsString());
    attributes.put(SemanticAttributes.TABLE_KEY, tableName.getNameAsString());
    return this;
  }

  @SuppressWarnings("unchecked")
  public Span build() {
    final String name = attributes.getOrDefault(SemanticAttributes.DB_OPERATION, unknown)
        + " "
        + (tableName != null ? tableName.getNameWithNamespaceInclAsString() : unknown);
    final SpanBuilder builder = TraceUtil.getGlobalTracer().spanBuilder(name);
    if (spanKind != null) { builder.setSpanKind(spanKind); }
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }

  private static SemanticAttributes.Operation valueFrom(final Scan scan) {
    if (scan == null) { return null; }
    return SemanticAttributes.Operation.SCAN;
  }

  private static SemanticAttributes.Operation valueFrom(final Row row) {
    if (row == null) { return null; }
    if (row instanceof Append) { return SemanticAttributes.Operation.APPEND; }
    if (row instanceof CheckAndMutate) { return SemanticAttributes.Operation.CHECK_AND_MUTATE; }
    if (row instanceof Delete) { return SemanticAttributes.Operation.DELETE; }
    if (row instanceof Get) { return SemanticAttributes.Operation.GET; }
    if (row instanceof Increment) { return SemanticAttributes.Operation.INCREMENT; }
    if (row instanceof Put) { return SemanticAttributes.Operation.PUT; }
    if (row instanceof RegionCoprocessorServiceExec) {
      return SemanticAttributes.Operation.COPROC_EXEC;
    }
    if (row instanceof RowMutations) { return SemanticAttributes.Operation.BATCH; }
    return null;
  }
}
