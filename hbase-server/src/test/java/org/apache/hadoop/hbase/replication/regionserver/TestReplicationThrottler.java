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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ReplicationTests.TAG)
@Tag(SmallTests.TAG)
public class TestReplicationThrottler {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationThrottler.class);

  /**
   * unit test for throttling
   */
  @Test
  public void testThrottling() {
    LOG.info("testThrottling");

    // throttle bandwidth is 100 and 10 bytes/cycle respectively
    ReplicationThrottler throttler1 = new ReplicationThrottler(100);
    ReplicationThrottler throttler2 = new ReplicationThrottler(10);

    long ticks1 = throttler1.getNextSleepInterval(1000);
    long ticks2 = throttler2.getNextSleepInterval(1000);

    // 1. the first push size is 1000, though 1000 bytes exceeds 100/10
    // bandwidthes, but no sleep since it's the first push of current
    // cycle, amortizing occurs when next push arrives
    assertEquals(0, ticks1);
    assertEquals(0, ticks2);

    throttler1.addPushSize(1000);
    throttler2.addPushSize(1000);

    ticks1 = throttler1.getNextSleepInterval(5);
    ticks2 = throttler2.getNextSleepInterval(5);

    // 2. when the second push(5) arrives and throttling(5) is called, the
    // current cyclePushSize is 1000 bytes, this should make throttler1
    // sleep 1000/100 = 10 cycles = 1s and make throttler2 sleep 1000/10
    // = 100 cycles = 10s before the second push occurs -- amortize case
    // after amortizing, both cycleStartTick and cyclePushSize are reset
    //
    // Note: in a slow machine, the sleep interval might be less than ideal ticks.
    // If it is 75% of expected value, its is still acceptable.
    if (ticks1 != 1000 && ticks1 != 999) {
      assertTrue(ticks1 >= 750 && ticks1 <= 1000);
    }
    if (ticks2 != 10000 && ticks2 != 9999) {
      assertTrue(ticks2 >= 7500 && ticks2 <= 10000);
    }

    throttler1.resetStartTick();
    throttler2.resetStartTick();

    throttler1.addPushSize(5);
    throttler2.addPushSize(5);

    ticks1 = throttler1.getNextSleepInterval(45);
    ticks2 = throttler2.getNextSleepInterval(45);

    // 3. when the third push(45) arrives and throttling(45) is called, the
    // current cyclePushSize is 5 bytes, 50-byte makes throttler1 no
    // sleep, but can make throttler2 delay to next cycle
    // note: in real case, sleep time should cover time elapses during push
    // operation
    assertTrue(ticks1 == 0);
    if (ticks2 != 100 && ticks2 != 99) {
      assertTrue(ticks1 >= 75 && ticks1 <= 100);
    }

    throttler2.resetStartTick();

    throttler1.addPushSize(45);
    throttler2.addPushSize(45);

    ticks1 = throttler1.getNextSleepInterval(60);
    ticks2 = throttler2.getNextSleepInterval(60);

    // 4. when the fourth push(60) arrives and throttling(60) is called, throttler1
    // delay to next cycle since 45+60 == 105; and throttler2 should firstly sleep
    // ceiling(45/10)= 5 cycles = 500ms to amortize previous push
    //
    // Note: in real case, sleep time should cover time elapses during push operation
    if (ticks1 != 100 && ticks1 != 99) {
      assertTrue(ticks1 >= 75 && ticks1 <= 100);
    }
    if (ticks2 != 500 && ticks2 != 499) {
      assertTrue(ticks1 >= 375 && ticks1 <= 500);
    }
  }

  /**
   * HBASE-30234: a batch whose size exceeds Integer.MAX_VALUE (~2GB) must be treated as a large
   * positive size instead of being silently truncated to a negative int. With truncation, the
   * "delay to next cycle" branch would compare a negative sum against the bandwidth and wrongly
   * return 0 (no throttling); with the fix it correctly delays the push to the next cycle.
   */
  @Test
  public void testLargeSizeDoesNotOverflow() {
    LOG.info("testLargeSizeDoesNotOverflow");
    // Freeze the clock so the assertion is deterministic (no cycle boundary is crossed).
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    edge.setValue(1000);
    EnvironmentEdgeManager.injectEdge(edge);
    try {
      // bandwidth of 1 byte/cycle so any positive push exceeds it
      ReplicationThrottler throttler = new ReplicationThrottler(1);
      // prime the current cycle so cyclePushSize > 0 (enables the "delay to next cycle" branch)
      throttler.addPushSize(1);
      // a size larger than Integer.MAX_VALUE; as an int this would wrap to a negative value
      long hugeSize = (long) Integer.MAX_VALUE + 1L;
      long ticks = throttler.getNextSleepInterval(hugeSize);
      // delayed to next cycle: cycleStartTick(1000) + one cycle(100) - now(1000) == 100ms.
      // A truncated (negative) size would fail the bandwidth check and wrongly return 0.
      assertEquals(100, ticks);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }
}
