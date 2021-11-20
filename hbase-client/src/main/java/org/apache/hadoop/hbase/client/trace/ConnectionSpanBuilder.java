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

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_CONNECTION_STRING;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_SYSTEM;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_SYSTEM_VALUE;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.DB_USER;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct {@link Span} instances originating from the client side of a connection.
 */
@InterfaceAudience.Private
public class ConnectionSpanBuilder<B extends ConnectionSpanBuilder<B>>
  extends HBaseSpanBuilder<ConnectionSpanBuilder<B>>
  implements Supplier<Span> {

  protected String name;
  protected final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  @Override
  public Span get() {
    return build();
  }

  public ConnectionSpanBuilder(final AsyncConnectionImpl conn) {
    addAttribute(DB_SYSTEM, DB_SYSTEM_VALUE);
    addAttribute(DB_CONNECTION_STRING, conn.getConnectionRegistry().getConnectionString());
    addAttribute(DB_USER, Optional.ofNullable(conn.getUser()).map(Object::toString).orElse(null));
  }

  @Override
  @SuppressWarnings("unchecked")
  public B self() {
    return (B) this;
  }

  public B setName(final String name) {
    this.name = name;
    return self();
  }

  public <T> B addAttribute(final AttributeKey<T> key, T value) {
    attributes.put(key, value);
    return self();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Span build() {
    final SpanBuilder builder = TraceUtil.getGlobalTracer()
      .spanBuilder(name)
      // TODO: what about clients embedded in Master/RegionServer/Gateways/&c?
      .setSpanKind(SpanKind.CLIENT);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }
}
