/**
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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResultImpl;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicationBufferManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicationBufferManager.class);

  private Configuration conf;

  private RegionServerServices rsServices;

  private RegionReplicationBufferManager manager;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    rsServices = mock(RegionServerServices.class);
    when(rsServices.getConfiguration()).thenReturn(conf);
  }

  @After
  public void tearDown() {
    if (manager != null) {
      manager.stop();
    }
  }

  private HRegion mockRegion(RegionInfo regionInfo, long pendingSize) throws IOException {
    HRegion region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    if (pendingSize < 0) {
      when(region.getRegionReplicationSink()).thenReturn(Optional.empty());
    } else {
      RegionReplicationSink sink = mock(RegionReplicationSink.class);
      when(sink.pendingSize()).thenReturn(pendingSize);
      when(region.getRegionReplicationSink()).thenReturn(Optional.of(sink));
    }
    return region;
  }

  @Test
  public void testScheduleFlush() throws IOException, InterruptedException {
    conf.setLong(RegionReplicationBufferManager.MAX_PENDING_SIZE, 1024 * 1024);
    manager = new RegionReplicationBufferManager(rsServices);
    RegionInfo info1 = RegionInfoBuilder.newBuilder(TableName.valueOf("info1")).build();
    RegionInfo info2 = RegionInfoBuilder.newBuilder(TableName.valueOf("info2")).build();
    HRegion region1 = mockRegion(info1, 1000);
    HRegion region2 = mockRegion(info2, 10000);
    when(rsServices.getRegions()).thenReturn(Arrays.asList(region1, region2));
    CountDownLatch arrive = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    when(region2.flushcache(anyBoolean(), anyBoolean(), any())).then(i -> {
      arrive.countDown();
      resume.await();
      FlushResultImpl result = mock(FlushResultImpl.class);
      when(result.isFlushSucceeded()).thenReturn(true);
      return result;
    });
    // hit the soft limit, should trigger a flush
    assertTrue(manager.increase(1000 * 1024));
    arrive.await();

    // we should have called getRegions once to find the region to flush
    verify(rsServices, times(1)).getRegions();

    // hit the hard limit, but since the background thread is running as we haven't call the
    // resume.countDown yet, the schedule of the new background flush task should be discard
    // silently.
    assertFalse(manager.increase(100 * 1024));
    resume.countDown();

    // wait several seconds and then check the getRegions call, we should not call it second time
    Thread.sleep(2000);
    verify(rsServices, times(1)).getRegions();
  }
}
