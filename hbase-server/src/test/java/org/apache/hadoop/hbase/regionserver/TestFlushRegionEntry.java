/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.MemStoreFlusher.FlushRegionEntry;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({RegionServerTests.class, SmallTests.class})
public class TestFlushRegionEntry {
  @Before
  public void setUp() throws Exception {
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    edge.setValue(12345);
    EnvironmentEdgeManager.injectEdge(edge);
  }

  @Test
  public void test() {
    FlushRegionEntry entry = new FlushRegionEntry(Mockito.mock(HRegion.class), true);
    FlushRegionEntry other = new FlushRegionEntry(Mockito.mock(HRegion.class), true);

    assertEquals(entry.hashCode(), other.hashCode());
    assertEquals(entry, other);
  }

  @After
  public void teardown() {
    EnvironmentEdgeManager.reset();
  }
}