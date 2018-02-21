/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestRegionSizeReportingChore {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionSizeReportingChore.class);

  @Test
  public void testDefaultConfigurationProperties() {
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);
    RegionSizeReportingChore chore = new RegionSizeReportingChore(rs);
    assertEquals(
        RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_DELAY_DEFAULT,
        chore.getInitialDelay());
    assertEquals(
        RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_PERIOD_DEFAULT, chore.getPeriod());
    assertEquals(
        TimeUnit.valueOf(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_TIMEUNIT_DEFAULT),
        chore.getTimeUnit());
  }

  @Test
  public void testNonDefaultConfigurationProperties() {
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);
    final int period = RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_PERIOD_DEFAULT + 1;
    final long delay = RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_DELAY_DEFAULT + 1L;
    final String timeUnit = TimeUnit.SECONDS.name();
    conf.setInt(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_PERIOD_KEY, period);
    conf.setLong(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_DELAY_KEY, delay);
    conf.set(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_TIMEUNIT_KEY, timeUnit);
    RegionSizeReportingChore chore = new RegionSizeReportingChore(rs);
    assertEquals(delay, chore.getInitialDelay());
    assertEquals(period, chore.getPeriod());
    assertEquals(TimeUnit.valueOf(timeUnit), chore.getTimeUnit());
  }

  @Test
  public void testRemovableOfNonOnlineRegions() {
    final Configuration conf = getDefaultHBaseConfiguration();
    final HRegionServer rs = mockRegionServer(conf);
    RegionSizeReportingChore chore = new RegionSizeReportingChore(rs);

    RegionInfo infoA = RegionInfoBuilder.newBuilder(TableName.valueOf("T1"))
        .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();
    RegionInfo infoB = RegionInfoBuilder.newBuilder(TableName.valueOf("T1"))
        .setStartKey(Bytes.toBytes("b")).setEndKey(Bytes.toBytes("d")).build();
    RegionInfo infoC = RegionInfoBuilder.newBuilder(TableName.valueOf("T1"))
        .setStartKey(Bytes.toBytes("c")).setEndKey(Bytes.toBytes("d")).build();

    RegionSizeStore store = new RegionSizeStoreImpl();
    store.put(infoA, 1024L);
    store.put(infoB, 1024L);
    store.put(infoC, 1024L);

    // If there are no online regions, all entries should be removed.
    chore.removeNonOnlineRegions(store, Collections.<RegionInfo> emptySet());
    assertTrue(store.isEmpty());

    store.put(infoA, 1024L);
    store.put(infoB, 1024L);
    store.put(infoC, 1024L);

    // Remove a single region
    chore.removeNonOnlineRegions(store, new HashSet<>(Arrays.asList(infoA, infoC)));
    assertEquals(2, store.size());
    assertNotNull(store.getRegionSize(infoA));
    assertNotNull(store.getRegionSize(infoC));
  }

  /**
   * Creates an HBase Configuration object for the default values.
   */
  private Configuration getDefaultHBaseConfiguration() {
    final Configuration conf = HBaseConfiguration.create();
    conf.addResource("hbase-default.xml");
    return conf;
  }

  private HRegionServer mockRegionServer(Configuration conf) {
    HRegionServer rs = mock(HRegionServer.class);
    when(rs.getConfiguration()).thenReturn(conf);
    return rs;
  }
}
