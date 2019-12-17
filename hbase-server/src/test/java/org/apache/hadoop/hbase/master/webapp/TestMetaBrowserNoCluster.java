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
package org.apache.hadoop.hbase.master.webapp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.webapp.TestMetaBrowser.MockRequestBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Cluster-backed correctness tests for the functionality provided by {@link MetaBrowser}.
 */
@Category({ MasterTests.class, SmallTests.class})
public class TestMetaBrowserNoCluster {

  @ClassRule
  public static final HBaseClassTestRule testRule =
    HBaseClassTestRule.forClass(TestMetaBrowserNoCluster.class);

  @Mock
  private AsyncConnection connection;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void buildFirstPageQueryStringNoParams() {
    final HttpServletRequest request = new MockRequestBuilder().build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);

    assertEquals("hbase:meta", metaBrowser.getName());
    assertNull(metaBrowser.getScanLimit());
    assertNull(metaBrowser.getScanRegionState());
    assertNull(metaBrowser.getScanStart());
    assertNull(metaBrowser.getScanTable());
    assertEquals("/table.jsp?name=hbase%3Ameta", metaBrowser.buildFirstPageUrl());
  }

  @Test
  public void buildFirstPageQueryStringNonNullParams() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit(50)
      .setRegionState(RegionState.State.ABNORMALLY_CLOSED)
      .setTable("foo%3Abar")
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);

    assertEquals(50, metaBrowser.getScanLimit().intValue());
    assertEquals(RegionState.State.ABNORMALLY_CLOSED, metaBrowser.getScanRegionState());
    assertEquals(TableName.valueOf("foo", "bar"), metaBrowser.getScanTable());
    assertEquals(
      "/table.jsp?name=hbase%3Ameta"
        + "&scan_limit=50"
        + "&scan_region_state=ABNORMALLY_CLOSED"
        + "&scan_table=foo%3Abar",
      metaBrowser.buildNextPageUrl(null));
  }

  @Test
  public void buildNextPageQueryString() {
    final HttpServletRequest request = new MockRequestBuilder().build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);

    assertEquals(
      "/table.jsp?name=hbase%3Ameta&scan_start=%255Cx80%255Cx00%255Cx7F",
      metaBrowser.buildNextPageUrl(new byte[] { Byte.MIN_VALUE, (byte) 0, Byte.MAX_VALUE }));
  }

  @Test
  public void unparseableLimitParam() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit("foo")
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
    assertNull(metaBrowser.getScanLimit());
    assertThat(metaBrowser.getErrorMessages(), contains(
      "Requested SCAN_LIMIT value 'foo' cannot be parsed as an integer."));
  }

  @Test
  public void zeroLimitParam() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit(0)
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
    assertEquals(MetaBrowser.SCAN_LIMIT_DEFAULT, metaBrowser.getScanLimit().intValue());
    assertThat(metaBrowser.getErrorMessages(), contains(
      "Requested SCAN_LIMIT value 0 is <= 0."));
  }

  @Test
  public void negativeLimitParam() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit(-10)
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
    assertEquals(MetaBrowser.SCAN_LIMIT_DEFAULT, metaBrowser.getScanLimit().intValue());
    assertThat(metaBrowser.getErrorMessages(), contains(
      "Requested SCAN_LIMIT value -10 is <= 0."));
  }

  @Test
  public void excessiveLimitParam() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit(10_001)
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
    assertEquals(MetaBrowser.SCAN_LIMIT_MAX, metaBrowser.getScanLimit().intValue());
    assertThat(metaBrowser.getErrorMessages(), contains(
      "Requested SCAN_LIMIT value 10001 exceeds maximum value 10000."));
  }

  @Test
  public void invalidRegionStateParam() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setRegionState("foo")
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
    assertNull(metaBrowser.getScanRegionState());
    assertThat(metaBrowser.getErrorMessages(), contains(
      "Requested SCAN_REGION_STATE value 'foo' cannot be parsed as a RegionState."));
  }

  @Test
  public void multipleErrorMessages() {
    final HttpServletRequest request = new MockRequestBuilder()
      .setLimit("foo")
      .setRegionState("bar")
      .build();
    final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
    assertThat(metaBrowser.getErrorMessages(), containsInAnyOrder(
      "Requested SCAN_LIMIT value 'foo' cannot be parsed as an integer.",
      "Requested SCAN_REGION_STATE value 'bar' cannot be parsed as a RegionState."
    ));
  }
}
