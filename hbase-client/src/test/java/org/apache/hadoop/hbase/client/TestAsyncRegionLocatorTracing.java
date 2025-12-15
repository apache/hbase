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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntryWithStringValuesOf;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasAttributes;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasKind;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.apache.hadoop.hbase.client.trace.hamcrest.TraceTestUtil.buildConnectionAttributesMatcher;
import static org.apache.hadoop.hbase.client.trace.hamcrest.TraceTestUtil.buildTableAttributesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncRegionLocatorTracing {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncRegionLocatorTracing.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncRegionLocatorTracing.class);

  private static final Configuration CONF = HBaseConfiguration.create();

  private AsyncConnectionImpl conn;

  private RegionLocations locs;

  @Rule
  public OpenTelemetryRule traceRule = OpenTelemetryRule.create();

  @Before
  public void setUp() throws IOException {
    RegionInfo metaRegionInfo = RegionInfoBuilder.newBuilder(MetaTableName.getInstance()).build();
    locs = new RegionLocations(
      new HRegionLocation(metaRegionInfo,
        ServerName.valueOf("127.0.0.1", 12345, EnvironmentEdgeManager.currentTime())),
      new HRegionLocation(RegionReplicaUtil.getRegionInfoForReplica(metaRegionInfo, 1),
        ServerName.valueOf("127.0.0.2", 12345, EnvironmentEdgeManager.currentTime())),
      new HRegionLocation(RegionReplicaUtil.getRegionInfoForReplica(metaRegionInfo, 2),
        ServerName.valueOf("127.0.0.3", 12345, EnvironmentEdgeManager.currentTime())));
    User user = UserProvider.instantiate(CONF).getCurrent();
    conn = new AsyncConnectionImpl(CONF, new DoNothingConnectionRegistry(CONF, user) {

      @Override
      public CompletableFuture<RegionLocations> getMetaRegionLocations() {
        return CompletableFuture.completedFuture(locs);
      }
    }, "test", null, user);
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  private SpanData waitSpan(String name) {
    return waitSpan(hasName(name));
  }

  private SpanData waitSpan(Matcher<SpanData> matcher) {
    Matcher<SpanData> spanLocator = allOf(matcher, hasEnded());
    try {
      Waiter.waitFor(CONF, 1000, new MatcherPredicate<>("waiting for span",
        () -> traceRule.getSpans(), hasItem(spanLocator)));
    } catch (AssertionError e) {
      LOG.error("AssertionError while waiting for matching span. Span reservoir contains: {}",
        traceRule.getSpans());
      throw e;
    }
    return traceRule.getSpans().stream().filter(spanLocator::matches).findFirst()
      .orElseThrow(AssertionError::new);
  }

  @Test
  public void testClearCache() {
    conn.getLocator().clearCache();
    SpanData span = waitSpan("AsyncRegionLocator.clearCache");
    assertThat(span, allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
      buildConnectionAttributesMatcher(conn)));
  }

  @Test
  public void testClearCacheServerName() {
    ServerName sn = ServerName.valueOf("127.0.0.1", 12345, EnvironmentEdgeManager.currentTime());
    conn.getLocator().clearCache(sn);
    SpanData span = waitSpan("AsyncRegionLocator.clearCache");
    assertThat(span,
      allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
        buildConnectionAttributesMatcher(conn),
        hasAttributes(containsEntry("db.hbase.server.name", sn.getServerName()))));
  }

  @Test
  public void testClearCacheTableName() {
    conn.getLocator().clearCache(MetaTableName.getInstance());
    SpanData span = waitSpan("AsyncRegionLocator.clearCache");
    assertThat(span,
      allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
        buildConnectionAttributesMatcher(conn),
        buildTableAttributesMatcher(MetaTableName.getInstance())));
  }

  @Test
  public void testGetRegionLocation() {
    conn.getLocator().getRegionLocation(MetaTableName.getInstance(), HConstants.EMPTY_START_ROW,
      RegionLocateType.CURRENT, TimeUnit.SECONDS.toNanos(1)).join();
    SpanData span = waitSpan("AsyncRegionLocator.getRegionLocation");
    assertThat(span,
      allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
        buildConnectionAttributesMatcher(conn),
        buildTableAttributesMatcher(MetaTableName.getInstance()),
        hasAttributes(containsEntryWithStringValuesOf("db.hbase.regions",
          locs.getDefaultRegionLocation().getRegion().getRegionNameAsString()))));
  }

  @Test
  public void testGetRegionLocations() {
    conn.getLocator().getRegionLocations(MetaTableName.getInstance(), HConstants.EMPTY_START_ROW,
      RegionLocateType.CURRENT, false, TimeUnit.SECONDS.toNanos(1)).join();
    SpanData span = waitSpan("AsyncRegionLocator.getRegionLocations");
    String[] expectedRegions =
      Arrays.stream(locs.getRegionLocations()).map(HRegionLocation::getRegion)
        .map(RegionInfo::getRegionNameAsString).toArray(String[]::new);
    assertThat(span, allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
      buildConnectionAttributesMatcher(conn),
      buildTableAttributesMatcher(MetaTableName.getInstance()), hasAttributes(
        containsEntryWithStringValuesOf("db.hbase.regions", containsInAnyOrder(expectedRegions)))));
  }
}
