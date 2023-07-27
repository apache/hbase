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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntryWithStringValuesOf;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasAttributes;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasKind;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.apache.hadoop.hbase.client.trace.hamcrest.TraceTestUtil.buildConnectionAttributesMatcher;
import static org.apache.hadoop.hbase.client.trace.hamcrest.TraceTestUtil.buildTableAttributesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ClientTests.class, MediumTests.class })
public class TestRegionLocatorTracing extends TestTracingBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocatorTracing.class);

  ConnectionImplementation conn;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    conn = new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent(),
      Collections.emptyMap());
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  @Test
  public void testGetRegionLocation() throws IOException {
    conn.getRegionLocator(TableName.META_TABLE_NAME).getRegionLocation(HConstants.EMPTY_START_ROW);
    SpanData span = waitSpan("HRegionLocator.getRegionLocation");
    assertThat(span,
      allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
        buildConnectionAttributesMatcher(conn),
        buildTableAttributesMatcher(TableName.META_TABLE_NAME),
        hasAttributes(containsEntryWithStringValuesOf("db.hbase.regions",
          META_REGION_LOCATION.getDefaultRegionLocation().getRegion().getRegionNameAsString()))));
  }

  @Test
  public void testGetRegionLocations() throws IOException {
    conn.getRegionLocator(TableName.META_TABLE_NAME).getRegionLocations(HConstants.EMPTY_START_ROW);
    SpanData span = waitSpan("HRegionLocator.getRegionLocations");
    // TODO: Use a value of `META_REGION_LOCATION` that contains multiple region locations.
    String[] expectedRegions =
      Arrays.stream(META_REGION_LOCATION.getRegionLocations()).map(HRegionLocation::getRegion)
        .map(RegionInfo::getRegionNameAsString).toArray(String[]::new);
    assertThat(span, allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
      buildConnectionAttributesMatcher(conn),
      buildTableAttributesMatcher(TableName.META_TABLE_NAME), hasAttributes(
        containsEntryWithStringValuesOf("db.hbase.regions", containsInAnyOrder(expectedRegions)))));
  }

  @Test
  public void testGetAllRegionLocations() throws IOException {
    conn.getRegionLocator(TableName.META_TABLE_NAME).getAllRegionLocations();
    SpanData span = waitSpan("HRegionLocator.getAllRegionLocations");
    // TODO: Use a value of `META_REGION_LOCATION` that contains multiple region locations.
    String[] expectedRegions =
      Arrays.stream(META_REGION_LOCATION.getRegionLocations()).map(HRegionLocation::getRegion)
        .map(RegionInfo::getRegionNameAsString).toArray(String[]::new);
    assertThat(span, allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
      buildConnectionAttributesMatcher(conn),
      buildTableAttributesMatcher(TableName.META_TABLE_NAME), hasAttributes(
        containsEntryWithStringValuesOf("db.hbase.regions", containsInAnyOrder(expectedRegions)))));
  }

  @Test
  public void testClearRegionLocationCache() throws IOException {
    conn.getRegionLocator(TableName.META_TABLE_NAME).clearRegionLocationCache();
    SpanData span = waitSpan("HRegionLocator.clearRegionLocationCache");
    assertThat(span,
      allOf(hasStatusWithCode(StatusCode.OK), hasKind(SpanKind.INTERNAL),
        buildConnectionAttributesMatcher(conn),
        buildTableAttributesMatcher(TableName.META_TABLE_NAME)));
  }

}
