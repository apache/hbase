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

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_NAME;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_OPERATION;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.NAMESPACE_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.TABLE_KEY;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
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
 * Construct {@link io.opentelemetry.api.trace.Span} instances originating from
 * "table operations" -- the verbs in our public API that interact with data in tables.
 */
@InterfaceAudience.Private
public class TableOperationSpanBuilder implements Supplier<Span> {

  // n.b. The results of this class are tested implicitly by way of the likes of
  // `TestAsyncTableTracing` and friends.

  private static final String unknown = "UNKNOWN";

  private TableName tableName;
  private final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  @Override public Span get() {
    return build();
  }

  public TableOperationSpanBuilder setOperation(final Scan scan) {
    return setOperation(valueFrom(scan));
  }

  public TableOperationSpanBuilder setOperation(final Row row) {
    return setOperation(valueFrom(row));
  }

  public TableOperationSpanBuilder setOperation(final Operation operation) {
    attributes.put(DB_OPERATION, operation.name());
    return this;
  }

  public TableOperationSpanBuilder setTableName(final TableName tableName) {
    this.tableName = tableName;
    attributes.put(NAMESPACE_KEY, tableName.getNamespaceAsString());
    attributes.put(DB_NAME, tableName.getNamespaceAsString());
    attributes.put(TABLE_KEY, tableName.getNameAsString());
    return this;
  }

  @SuppressWarnings("unchecked")
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
