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
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.TABLE_KEY;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct {@link Span} instances involving data tables.
 */
@InterfaceAudience.Private
public class TableSpanBuilder implements Supplier<Span> {

  private String name;
  private final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  public TableSpanBuilder(ClusterConnection conn) {
    ConnectionSpanBuilder.populateConnectionAttributes(attributes, conn);
  }

  public TableSpanBuilder(AsyncConnectionImpl conn) {
    ConnectionSpanBuilder.populateConnectionAttributes(attributes, conn);
  }

  @Override
  public Span get() {
    return build();
  }

  public TableSpanBuilder setName(final String name) {
    this.name = name;
    return this;
  }

  public TableSpanBuilder setTableName(final TableName tableName) {
    populateTableNameAttributes(attributes, tableName);
    return this;
  }

  @SuppressWarnings("unchecked")
  public Span build() {
    final SpanBuilder builder =
      TraceUtil.getGlobalTracer().spanBuilder(name).setSpanKind(SpanKind.INTERNAL);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }

  /**
   * Static utility method that performs the primary logic of this builder. It is visible to other
   * classes in this package so that other builders can use this functionality as a mix-in.
   * @param attributes the attributes map to be populated.
   * @param tableName  the source of attribute values.
   */
  static void populateTableNameAttributes(final Map<AttributeKey<?>, Object> attributes,
    final TableName tableName) {
    attributes.put(DB_NAME, tableName.getNamespaceAsString());
    attributes.put(TABLE_KEY, tableName.getNameAsString());
  }
}
