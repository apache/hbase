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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestMemStoreFlusher {

  private String name;

  public MemStoreFlusher msf;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    this.name = testInfo.getTestMethod().get().getName();
    Configuration conf = new Configuration();
    conf.set("hbase.hstore.flusher.count", "0");
    msf = new MemStoreFlusher(conf, null);
  }

  @Test
  public void testReplaceDelayedFlushEntry() {
    RegionInfo hri =
      RegionInfoBuilder.newBuilder(TableName.valueOf(name)).setRegionId(1).setReplicaId(0).build();
    HRegion r = mock(HRegion.class);
    doReturn(hri).when(r).getRegionInfo();

    // put a delayed task with 30s delay
    msf.requestDelayedFlush(r, 30000);
    assertEquals(1, msf.getFlushQueueSize());
    assertTrue(msf.regionsInQueue.get(r).isDelay());

    // put a non-delayed task, then the delayed one should be replaced
    assertTrue(msf.requestFlush(r, FlushLifeCycleTracker.DUMMY));
    assertEquals(1, msf.getFlushQueueSize());
    assertFalse(msf.regionsInQueue.get(r).isDelay());
  }

  @Test
  public void testNotReplaceDelayedFlushEntryWhichExpired() {
    RegionInfo hri =
      RegionInfoBuilder.newBuilder(TableName.valueOf(name)).setRegionId(1).setReplicaId(0).build();
    HRegion r = mock(HRegion.class);
    doReturn(hri).when(r).getRegionInfo();

    // put a delayed task with 100ms delay
    msf.requestDelayedFlush(r, 100);
    assertEquals(1, msf.getFlushQueueSize());
    assertTrue(msf.regionsInQueue.get(r).isDelay());

    Threads.sleep(200);

    // put a non-delayed task, and the delayed one is expired, so it should not be replaced
    assertFalse(msf.requestFlush(r, FlushLifeCycleTracker.DUMMY));
    assertEquals(1, msf.getFlushQueueSize());
    assertTrue(msf.regionsInQueue.get(r).isDelay());
  }

  @Test
  public void testChangeFlusherCount() {
    Configuration conf = new Configuration();
    conf.set("hbase.hstore.flusher.count", "0");
    HRegionServer rs = mock(HRegionServer.class);
    doReturn(false).when(rs).isStopped();
    doReturn(new RegionServerAccounting(conf)).when(rs).getRegionServerAccounting();

    msf = new MemStoreFlusher(conf, rs);
    msf.start(Threads.LOGGING_EXCEPTION_HANDLER);

    Configuration newConf = new Configuration();

    newConf.set("hbase.hstore.flusher.count", "3");
    msf.onConfigurationChange(newConf);
    assertEquals(3, msf.getFlusherCount());

    newConf.set("hbase.hstore.flusher.count", "0");
    msf.onConfigurationChange(newConf);
    assertEquals(1, msf.getFlusherCount());
  }
}
