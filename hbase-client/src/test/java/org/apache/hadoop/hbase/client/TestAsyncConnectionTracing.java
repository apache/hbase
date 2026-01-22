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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncConnectionTracing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncConnectionTracing.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private ServerName masterServer =
    ServerName.valueOf("localhost", 12345, EnvironmentEdgeManager.currentTime());

  private AsyncConnection conn;

  @Rule
  public OpenTelemetryRule traceRule = OpenTelemetryRule.create();

  @Before
  public void setUp() throws IOException {
    User user = UserProvider.instantiate(CONF).getCurrent();
    ConnectionRegistry registry = new DoNothingConnectionRegistry(CONF, user) {

      @Override
      public CompletableFuture<ServerName> getActiveMaster() {
        return CompletableFuture.completedFuture(masterServer);
      }
    };
    conn = new AsyncConnectionImpl(CONF, registry, "test", TableName.valueOf("hbase:meta"), null, user);
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  private void assertTrace(String methodName, ServerName serverName) {
    Waiter.waitFor(CONF, 1000,
      () -> traceRule.getSpans().stream()
        .anyMatch(span -> span.getName().equals("AsyncConnection." + methodName)
          && span.getKind() == SpanKind.INTERNAL && span.hasEnded()));
    SpanData data = traceRule.getSpans().stream()
      .filter(s -> s.getName().equals("AsyncConnection." + methodName)).findFirst().get();
    assertEquals(StatusCode.OK, data.getStatus().getStatusCode());
    if (serverName != null) {
      assertEquals(serverName.getServerName(),
        data.getAttributes().get(HBaseSemanticAttributes.SERVER_NAME_KEY));
    }
  }

  @Test
  public void testHbck() {
    conn.getHbck().join();
    assertTrace("getHbck", masterServer);
  }

  @Test
  public void testHbckWithServerName() throws IOException {
    ServerName serverName =
      ServerName.valueOf("localhost", 23456, EnvironmentEdgeManager.currentTime());
    conn.getHbck(serverName);
    assertTrace("getHbck", serverName);
  }

  @Test
  public void testClose() throws IOException {
    conn.close();
    assertTrace("close", null);
  }
}
