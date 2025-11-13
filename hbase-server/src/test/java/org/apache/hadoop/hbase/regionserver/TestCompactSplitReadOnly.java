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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCompactSplitReadOnly {

  private CompactSplit compactSplit;
  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    // enable read-only mode
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    // create CompactSplit with conf-only constructor (available for tests)
    compactSplit = new CompactSplit(conf);
  }

  @After
  public void tearDown() {
    // ensure thread pools are shutdown to avoid leakage
    compactSplit.interruptIfNecessary();
  }

  @Test
  public void testRequestSystemCompactionIgnoredWhenReadOnly() throws IOException {
    // Mock HRegion and HStore minimal behavior
    HRegion region = mock(HRegion.class);
    HStore store = mock(HStore.class);

    // Ensure compaction queues start empty
    assertEquals(0, compactSplit.getCompactionQueueSize());

    // Call requestSystemCompaction for a single store (selectNow = false)
    compactSplit.requestSystemCompaction(region, store, "test-readonly");

    // Because read-only mode is enabled, no compaction should be queued
    assertEquals(0, compactSplit.getCompactionQueueSize());
  }

  @Test
  public void testSelectCompactionIgnoredWhenReadOnly() throws IOException {
    HRegion region = mock(HRegion.class);
    HStore store = mock(HStore.class);

    // Ensure compaction queues start empty
    assertEquals(0, compactSplit.getCompactionQueueSize());

    // Call requestCompaction which uses selectNow = true and should call selectCompaction
    compactSplit.requestCompaction(region, store, "test-select-readonly", 0,
      CompactionLifeCycleTracker.DUMMY, (User) null);

    // Because read-only mode is enabled, selectCompaction should be skipped and no task queued
    assertEquals(0, compactSplit.getCompactionQueueSize());
  }
}
