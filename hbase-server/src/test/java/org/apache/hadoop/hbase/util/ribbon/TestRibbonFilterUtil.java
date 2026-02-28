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
package org.apache.hadoop.hbase.util.ribbon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link RibbonFilterUtil}.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestRibbonFilterUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRibbonFilterUtil.class);

  @Test
  public void testComputeNumSlots() {
    // Basic computation: keys * (1 + overhead)
    assertEquals(1050, RibbonFilterUtil.computeNumSlots(1000, 0.05));
    assertEquals(1100, RibbonFilterUtil.computeNumSlots(1000, 0.10));

    // Zero keys returns 0
    assertEquals(0, RibbonFilterUtil.computeNumSlots(0, 0.05));

    // Small number of keys should still get minimum slots (bandwidth)
    int minSlots = RibbonFilterUtil.computeNumSlots(10, 0.05);
    assertTrue("Should have at least bandwidth slots",
      minSlots >= RibbonFilterUtil.DEFAULT_BANDWIDTH);

    // Overhead clamping
    int slots1 = RibbonFilterUtil.computeNumSlots(1000, 0.001); // below min
    int slots2 = RibbonFilterUtil.computeNumSlots(1000, RibbonFilterUtil.MIN_OVERHEAD_RATIO);
    assertEquals(slots1, slots2);
  }

  @Test
  public void testComputeOptimalOverhead() {
    // Based on paper's Equation 7: ε = (4 + r/4) / w where r = -log₂(fpRate)
    // Hand-calculated expected values:
    // - 1% FPR: r = 6.644, ε = (4 + 1.661) / 64 = 0.0885
    // - 0.1% FPR: r = 9.966, ε = (4 + 2.491) / 64 = 0.1014
    int bandwidth = RibbonFilterUtil.DEFAULT_BANDWIDTH;

    double overhead1 = RibbonFilterUtil.computeOptimalOverheadForFpRate(0.01, bandwidth);
    double overhead2 = RibbonFilterUtil.computeOptimalOverheadForFpRate(0.001, bandwidth);

    assertEquals("Overhead for 1% FPR", 0.0885, overhead1, 0.001);
    assertEquals("Overhead for 0.1% FPR", 0.1014, overhead2, 0.001);

    // Lower FPR requires higher overhead
    assertTrue("Lower FPR requires higher overhead", overhead2 > overhead1);
  }

  @Test
  public void testComputeFingerprintBits() {
    // ceil(-log₂(fpRate))
    assertEquals(7, RibbonFilterUtil.computeFingerprintBits(0.01)); // 1% FPR
    assertEquals(10, RibbonFilterUtil.computeFingerprintBits(0.001)); // 0.1% FPR
    assertEquals(1, RibbonFilterUtil.computeFingerprintBits(0.5)); // 50% FPR
  }
}
