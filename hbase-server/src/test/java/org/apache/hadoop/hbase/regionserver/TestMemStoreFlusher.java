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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMemStoreFlusher {
  public MemStoreFlusher msf;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hbase.hstore.flusher.count", "0");
    msf = new MemStoreFlusher(conf, null);
  }

  @Test
  public void testReplaceDelayedFlushEntry() {
    HRegionInfo hri = new HRegionInfo(1, TableName.valueOf("TestTable"), 0);
    HRegion r = mock(HRegion.class);
    doReturn(hri).when(r).getRegionInfo();

    // put a delayed task with 30s delay
    msf.requestDelayedFlush(r, 30000, false);
    assertEquals(1, msf.getFlushQueueSize());
    assertTrue(msf.regionsInQueue.get(r).isDelay());

    // put a non-delayed task, then the delayed one should be replaced
    assertTrue(msf.requestFlush(r, false));
    assertEquals(1, msf.getFlushQueueSize());
    assertFalse(msf.regionsInQueue.get(r).isDelay());
  }

  @Test
  public void testNotReplaceDelayedFlushEntryWhichExpired() {
    HRegionInfo hri = new HRegionInfo(1, TableName.valueOf("TestTable"), 0);
    HRegion r = mock(HRegion.class);
    doReturn(hri).when(r).getRegionInfo();

    // put a delayed task with 100ms delay
    msf.requestDelayedFlush(r, 100, false);
    assertEquals(1, msf.getFlushQueueSize());
    assertTrue(msf.regionsInQueue.get(r).isDelay());

    Threads.sleep(200);

    // put a non-delayed task, and the delayed one is expired, so it should not be replaced
    assertFalse(msf.requestFlush(r, false));
    assertEquals(1, msf.getFlushQueueSize());
    assertTrue(msf.regionsInQueue.get(r).isDelay());
  }
}
