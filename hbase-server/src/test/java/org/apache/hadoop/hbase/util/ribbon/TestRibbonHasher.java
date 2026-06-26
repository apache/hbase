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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link RibbonHasher}.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestRibbonHasher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRibbonHasher.class);

  @Test
  public void testBasicHashing() {
    int numSlots = 1000;
    int bandwidth = RibbonFilterUtil.DEFAULT_BANDWIDTH;
    RibbonHasher hasher = new RibbonHasher(numSlots, bandwidth, Hash.MURMUR_HASH3);

    // Test hash consistency: same key produces same result
    byte[] key = Bytes.toBytes(0);
    RibbonHasher.RibbonHashResult r1 = hasher.hash(key, 0, key.length);
    RibbonHasher.RibbonHashResult r2 = hasher.hash(key, 0, key.length);
    assertEquals(r1.start(), r2.start());
    assertEquals(r1.coeffRow(), r2.coeffRow());
    assertEquals(r1.resultRow(), r2.resultRow());

    // Test hash properties for many keys
    int maxStart = numSlots - bandwidth + 1;
    for (int i = 0; i < 1000; i++) {
      byte[] k = Bytes.toBytes(i);
      RibbonHasher.RibbonHashResult result = hasher.hash(k, 0, k.length);

      // start must be in valid range [0, numSlots - bandwidth]
      assertTrue("Start should be >= 0", result.start() >= 0);
      assertTrue("Start should be < " + maxStart, result.start() < maxStart);

      // First coefficient bit is always 1 (kFirstCoeffAlwaysOne optimization)
      assertEquals("First coeff bit should be 1", 1, result.coeffRow() & 1);

      // For Homogeneous Ribbon Filter, resultRow is always 0
      assertEquals("Result row should be 0", 0, result.resultRow());
    }
  }
}
