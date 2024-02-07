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

import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTracingBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestTracingBase.class);

  protected static final ServerName MASTER_HOST = ServerName.valueOf("localhost", 16010, 12345);
  protected static final RegionLocations META_REGION_LOCATION =
    new RegionLocations(new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO, MASTER_HOST));

  protected Configuration conf;

  @ClassRule
  public static OpenTelemetryRule TRACE_RULE = OpenTelemetryRule.create();

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      RegistryForTracingTest.class.getName());
    TRACE_RULE.clearSpans();
  }

  protected void assertTrace(String className, String methodName, ServerName serverName,
    TableName tableName) {
    String expectedSpanName = String.format("%s.%s", className, methodName);
    Waiter.waitFor(conf, 1000,
      () -> TRACE_RULE.getSpans().stream().anyMatch(span -> span.getName().equals(expectedSpanName)
        && span.getKind() == SpanKind.INTERNAL && span.hasEnded()));
    SpanData data = TRACE_RULE.getSpans().stream().filter(s -> s.getName().equals(expectedSpanName))
      .findFirst().get();
    assertEquals(StatusCode.OK, data.getStatus().getStatusCode());

    if (serverName != null) {
      Optional<SpanData> foundServerName =
        TRACE_RULE.getSpans().stream().filter(s -> s.getName().equals(expectedSpanName))
          .filter(s -> Objects.equals(serverName.getServerName(),
            s.getAttributes().get(HBaseSemanticAttributes.SERVER_NAME_KEY)))
          .findAny();
      assertTrue(foundServerName.isPresent());
    }

    if (tableName != null) {
      assertEquals(tableName.getNamespaceAsString(),
        data.getAttributes().get(HBaseSemanticAttributes.DB_NAME));
      assertEquals(tableName.getNameAsString(),
        data.getAttributes().get(HBaseSemanticAttributes.TABLE_KEY));
    }
  }

  protected SpanData waitSpan(String name) {
    return waitSpan(hasName(name));
  }

  protected SpanData waitSpan(Matcher<SpanData> matcher) {
    Matcher<SpanData> spanLocator = allOf(matcher, hasEnded());
    try {
      Waiter.waitFor(conf, 1000, new MatcherPredicate<>("waiting for span",
        () -> TRACE_RULE.getSpans(), hasItem(spanLocator)));
    } catch (AssertionError e) {
      LOG.error("AssertionError while waiting for matching span. Span reservoir contains: {}",
        TRACE_RULE.getSpans());
      throw e;
    }
    return TRACE_RULE.getSpans().stream().filter(spanLocator::matches).findFirst()
      .orElseThrow(AssertionError::new);
  }

  static class RegistryForTracingTest implements ConnectionRegistry {

    public RegistryForTracingTest(Configuration conf, User user) {
    }

    @Override
    public CompletableFuture<RegionLocations> getMetaRegionLocations() {
      return CompletableFuture.completedFuture(META_REGION_LOCATION);
    }

    @Override
    public CompletableFuture<String> getClusterId() {
      return CompletableFuture.completedFuture(HConstants.CLUSTER_ID_DEFAULT);
    }

    @Override
    public CompletableFuture<ServerName> getActiveMaster() {
      return CompletableFuture.completedFuture(MASTER_HOST);
    }

    @Override
    public String getConnectionString() {
      return "nothing";
    }

    @Override
    public void close() {

    }
  }

}
