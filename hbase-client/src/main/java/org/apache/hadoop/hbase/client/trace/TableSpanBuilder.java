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
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.NAMESPACE_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.TABLE_KEY;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct {@link Span} instances involving data tables.
 */
@InterfaceAudience.Private
public class TableSpanBuilder<B extends TableSpanBuilder<B>> extends ConnectionSpanBuilder<B> {

  protected TableName tableName;

  public TableSpanBuilder(AsyncConnectionImpl conn) {
    super(conn);
  }

  @Override
  @SuppressWarnings("unchecked")
  public B self() {
    return (B) this;
  }

  public B setTableName(final TableName tableName) {
    this.tableName = tableName;
    attributes.put(NAMESPACE_KEY, tableName.getNamespaceAsString());
    attributes.put(DB_NAME, tableName.getNamespaceAsString());
    attributes.put(TABLE_KEY, tableName.getNameAsString());
    return self();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Span build() {
    final Span span = super.build();
    attributes.forEach((k, v) -> span.setAttribute((AttributeKey<? super Object>) k, v));
    return span;
  }
}
