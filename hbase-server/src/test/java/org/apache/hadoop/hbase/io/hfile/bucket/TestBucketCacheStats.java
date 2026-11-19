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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestBucketCacheStats {

  private ManualEnvironmentEdge edge;
  private BucketCacheStats stats;

  @BeforeEach
  public void setUp() {
    edge = new ManualEnvironmentEdge();
    edge.setValue(1_000_000L);
    EnvironmentEdgeManager.injectEdge(edge);
    // A single period window does not start the metrics roller thread.
    stats = new BucketCacheStats(1, 0);
  }

  @AfterEach
  public void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  private void ioHits(int count) {
    for (int i = 0; i < count; i++) {
      stats.ioHit(1000);
    }
  }

  @Test
  public void testIOHitsPerSecondHasNoSideEffect() {
    ioHits(600);
    edge.incValue(60_000);
    assertEquals(10, stats.getIOHitsPerSecond());
    // Repeated reads, e.g. the RegionServer UI and the stats log, must see the same rate.
    assertEquals(10, stats.getIOHitsPerSecond());
    assertTrue(stats.toString().contains("ioHitsPerSecond=10"));
    assertEquals(10, stats.getIOHitsPerSecond());

    edge.incValue(60_000);
    assertEquals(5, stats.getIOHitsPerSecond());
  }

  @Test
  public void testIOHitsPerSecondWindowRestartsOnReset() {
    ioHits(600);
    edge.incValue(60_000);
    assertEquals(10, stats.getIOHitsPerSecond());

    stats.reset();
    assertEquals(0, stats.getIOHitsPerSecond());

    ioHits(300);
    edge.incValue(10_000);
    assertEquals(30, stats.getIOHitsPerSecond());
  }

  @Test
  public void testIOHitsPerSecondWithSubSecondElapsedTime() {
    ioHits(10);
    assertEquals(0, stats.getIOHitsPerSecond());

    edge.incValue(500);
    assertEquals(20, stats.getIOHitsPerSecond());

    edge.incValue(1_000);
    assertEquals(6, stats.getIOHitsPerSecond());
  }
}
