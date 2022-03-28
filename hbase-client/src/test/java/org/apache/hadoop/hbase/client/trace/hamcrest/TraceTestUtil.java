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
package org.apache.hadoop.hbase.client.trace.hamcrest;

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasAttributes;
import static org.hamcrest.Matchers.allOf;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.hadoop.hbase.client.ConnectionImplementation;
import org.hamcrest.Matcher;

public final class TraceTestUtil {

  private TraceTestUtil() {
  }

  /**
   * All {@link Span}s involving {@code conn} should include these attributes.
   */
  public static Matcher<SpanData> buildConnectionAttributesMatcher(AsyncConnectionImpl conn) {
    return hasAttributes(
      allOf(containsEntry("db.system", "hbase"), containsEntry("db.connection_string", "nothing"),
        containsEntry("db.user", conn.getUser().toString())));
  }

  /**
   * All {@link Span}s involving {@code conn} should include these attributes.
   * @see #buildConnectionAttributesMatcher(AsyncConnectionImpl)
   */
  public static Matcher<SpanData> buildConnectionAttributesMatcher(ConnectionImplementation conn) {
    return hasAttributes(
      allOf(containsEntry("db.system", "hbase"), containsEntry("db.connection_string", "nothing"),
        containsEntry("db.user", conn.getUser().toString())));
  }

  /**
   * All {@link Span}s involving {@code tableName} should include these attributes.
   */
  public static Matcher<SpanData> buildTableAttributesMatcher(TableName tableName) {
    return hasAttributes(allOf(containsEntry("db.name", tableName.getNamespaceAsString()),
      containsEntry("db.hbase.table", tableName.getNameAsString())));
  }
}
