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

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.CONTAINER_DB_OPERATIONS_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_OPERATION;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionCoprocessorServiceExec;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.Operation;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct {@link Span} instances originating from "table operations" -- the verbs in our public
 * API that interact with data in tables.
 */
@InterfaceAudience.Private
public class TableOperationSpanBuilder<B extends TableOperationSpanBuilder<B>>
  extends TableSpanBuilder<B> {

  // n.b. The results of this class are tested implicitly by way of the likes of
  // `TestAsyncTableTracing` and friends.

  private static final String unknown = "UNKNOWN";

  public TableOperationSpanBuilder(final AsyncConnectionImpl conn) {
    super(conn);
  }

  @Override
  @SuppressWarnings("unchecked")
  public B self() {
    return (B) this;
  }

  @Override
  public B setName(String name) {
    throw new UnsupportedOperationException();
  }

  public B setOperation(final Scan scan) {
    return setOperation(valueFrom(scan));
  }

  public B setOperation(final Row row) {
    return setOperation(valueFrom(row));
  }

  public B setOperation(final Operation operation) {
    attributes.put(DB_OPERATION, operation.name());
    return self();
  }

  // `setContainerOperations` perform a recursive descent expansion of all the operations
  // contained within the provided "batch" object.

  public B setContainerOperations(final RowMutations mutations) {
    final Operation[] ops = mutations.getMutations()
      .stream()
      .flatMap(row -> Stream.concat(Stream.of(valueFrom(row)), unpackRowOperations(row).stream()))
      .toArray(Operation[]::new);
    return setContainerOperations(ops);
  }

  public B setContainerOperations(final Row row) {
    final Operation[] ops =
      Stream.concat(Stream.of(valueFrom(row)), unpackRowOperations(row).stream())
      .toArray(Operation[]::new);
    return setContainerOperations(ops);
  }

  public B setContainerOperations(
    final Collection<? extends Row> operations
  ) {
    final Operation[] ops = operations.stream()
      .flatMap(row -> Stream.concat(Stream.of(valueFrom(row)), unpackRowOperations(row).stream()))
      .toArray(Operation[]::new);
    return setContainerOperations(ops);
  }

  private static Set<Operation> unpackRowOperations(final Row row) {
    final Set<Operation> ops = new HashSet<>();
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

  public B setContainerOperations(
    final Operation... operations
  ) {
    final List<String> ops = Arrays.stream(operations)
      .map(op -> op == null ? unknown : op.name())
      .sorted()
      .distinct()
      .collect(Collectors.toList());
    attributes.put(CONTAINER_DB_OPERATIONS_KEY, ops);
    return self();
  }

  private static Set<Operation> unpackRowOperations(final CheckAndMutate cam) {
    final Set<Operation> ops = new HashSet<>();
    final Operation op = valueFrom(cam.getAction());
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

  @SuppressWarnings("unchecked")
  @Override
  public Span build() {
    final String name = attributes.getOrDefault(DB_OPERATION, unknown)
        + " "
        + (tableName != null ? tableName.getNameWithNamespaceInclAsString() : unknown);
    final SpanBuilder builder = TraceUtil.getGlobalTracer()
      .spanBuilder(name)
      // TODO: what about clients embedded in Master/RegionServer/Gateways/&c?
      .setSpanKind(SpanKind.CLIENT);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }

  private static Operation valueFrom(final Scan scan) {
    if (scan == null) { return null; }
    return Operation.SCAN;
  }

  private static Operation valueFrom(final Row row) {
    if (row == null) { return null; }
    if (row instanceof Append) { return Operation.APPEND; }
    if (row instanceof CheckAndMutate) { return Operation.CHECK_AND_MUTATE; }
    if (row instanceof Delete) { return Operation.DELETE; }
    if (row instanceof Get) { return Operation.GET; }
    if (row instanceof Increment) { return Operation.INCREMENT; }
    if (row instanceof Put) { return Operation.PUT; }
    if (row instanceof RegionCoprocessorServiceExec) {
      return Operation.COPROC_EXEC;
    }
    if (row instanceof RowMutations) { return Operation.BATCH; }
    return null;
  }
}
