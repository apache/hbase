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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link RibbonBanding}.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestRibbonBanding {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRibbonBanding.class);

  private static final int COEFF_BITS = InterleavedRibbonSolution.COEFF_BITS;

  @Test
  public void testGaussianEliminationXOR() {
    // Test Gaussian elimination with XOR cascade
    RibbonBanding banding = new RibbonBanding(1000, COEFF_BITS);

    // Add entries that will trigger XOR cascade
    banding.add(0, 0b1111L); // 1111 at position 0
    banding.add(0, 0b1011L); // XOR with first -> 0100, shifts to position 2
    banding.add(0, 0b1101L); // XOR with first -> 0010, shifts to position 1

    assertEquals(3, banding.getNumAdded());
    assertEquals(0b1111L, banding.getCoeffRow(0));
    assertEquals(0b0001L, banding.getCoeffRow(1)); // 0010 shifted by 1
    assertEquals(0b0001L, banding.getCoeffRow(2)); // 0100 shifted by 2

    // Test redundant equation (XOR results in 0)
    banding.add(0, 0b1111L); // Same as first, XOR gives 0, absorbed
    assertEquals(3, banding.getNumAdded()); // Still 3
  }
}
