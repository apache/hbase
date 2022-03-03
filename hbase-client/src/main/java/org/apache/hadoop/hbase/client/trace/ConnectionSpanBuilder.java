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
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Construct {@link Span} instances originating from the client side of a connection.
 */
@InterfaceAudience.Private
public class ConnectionSpanBuilder implements Supplier<Span> {

  private String name;
  private final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  public ConnectionSpanBuilder(final AsyncConnectionImpl conn) {
    populateConnectionAttributes(attributes, conn);
  }

  @Override
  public Span get() {
    return build();
  }

  public ConnectionSpanBuilder setName(final String name) {
    this.name = name;
    return this;
  }

  public <T> ConnectionSpanBuilder addAttribute(final AttributeKey<T> key, T value) {
    attributes.put(key, value);
    return this;
  }

  @SuppressWarnings("unchecked")
  public Span build() {
    final SpanBuilder builder = TraceUtil.getGlobalTracer()
      .spanBuilder(name)
      // TODO: what about clients embedded in Master/RegionServer/Gateways/&c?
      .setSpanKind(SpanKind.CLIENT);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }

  /**
   * Static utility method that performs the primary logic of this builder. It is visible to other
   * classes in this package so that other builders can use this functionality as a mix-in.
   * @param attributes the attributes map to be populated.
   * @param conn the source of connection attribute values.
   */
  static void populateConnectionAttributes(
    final Map<AttributeKey<?>, Object> attributes,
    final AsyncConnectionImpl conn
  ) {
    final Supplier<String> connStringSupplier = () -> conn.getConnectionRegistry()
      .getConnectionString();
    populateConnectionAttributes(attributes, connStringSupplier, conn::getUser);
  }

  /**
   * Static utility method that performs the primary logic of this builder. It is visible to other
   * classes in this package so that other builders can use this functionality as a mix-in.
   * @param attributes the attributes map to be populated.
   * @param connectionStringSupplier the source of the {@code db.connection_string} attribute value.
   * @param userSupplier the source of the {@code db.user} attribute value.
   */
  static void populateConnectionAttributes(
    final Map<AttributeKey<?>, Object> attributes,
    final Supplier<String> connectionStringSupplier,
    final Supplier<User> userSupplier
  ) {
    attributes.put(DB_SYSTEM, DB_SYSTEM_VALUE);
    attributes.put(DB_CONNECTION_STRING, connectionStringSupplier.get());
    attributes.put(DB_USER, Optional.ofNullable(userSupplier.get())
      .map(Object::toString)
      .orElse(null));
  }
}
