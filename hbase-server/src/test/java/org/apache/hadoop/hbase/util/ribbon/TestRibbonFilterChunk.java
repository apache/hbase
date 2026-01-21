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

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link RibbonFilterChunk}.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestRibbonFilterChunk {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRibbonFilterChunk.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRibbonFilterChunk.class);

  @Test
  public void testBasicRibbon() {
    RibbonFilterChunk rf1 = new RibbonFilterChunk(BloomType.ROW);
    RibbonFilterChunk rf2 = new RibbonFilterChunk(BloomType.ROW);
    rf1.allocRibbon(1000);
    rf2.allocRibbon(1000);

    byte[] key1 = Bytes.toBytes(1);
    byte[] key2 = Bytes.toBytes(2);

    rf1.addKey(key1);
    rf2.addKey(key2);

    rf1.finalizeRibbon();
    rf2.finalizeRibbon();

    // No false negatives
    assertTrue(rf1.contains(key1, 0, key1.length));
    assertTrue(rf2.contains(key2, 0, key2.length));
  }

  @Test
  public void testSerialization() throws Exception {
    RibbonFilterChunk rf = new RibbonFilterChunk(BloomType.ROW);
    rf.allocRibbon(1000);

    for (int i = 0; i < 100; i++) {
      rf.addKey(Bytes.toBytes(i));
    }
    rf.finalizeRibbon();

    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    rf.writeRibbon(new DataOutputStream(bOut));

    assertTrue(bOut.size() > 0);
    LOG.info("Serialized {} keys as {} bytes", rf.getKeyCount(), bOut.size());
  }

  @Test
  public void testFalsePositiveRate() {
    int numKeys = 10000;
    int numTests = 100000;

    RibbonFilterChunk rf = new RibbonFilterChunk(BloomType.ROW);
    rf.allocRibbon(numKeys);

    for (int i = 0; i < numKeys; i++) {
      rf.addKey(Bytes.toBytes(i));
    }
    rf.finalizeRibbon();

    // Verify no false negatives
    for (int i = 0; i < numKeys; i++) {
      byte[] key = Bytes.toBytes(i);
      assertTrue("Key " + i + " should be found", rf.contains(key, 0, key.length));
    }

    // Measure false positive rate with non-added keys
    int falsePositives = 0;
    for (int i = 0; i < numTests; i++) {
      byte[] key = Bytes.toBytes(numKeys + i);
      if (rf.contains(key, 0, key.length)) {
        falsePositives++;
      }
    }

    double actualFpr = (double) falsePositives / numTests;
    LOG.info("FPR: {} / {} = {}%", falsePositives, numTests,
      String.format("%.2f", actualFpr * 100));
    assertTrue("FPR should be less than 5% for 1% target", actualFpr < 0.05);
  }

  @Test
  public void testLazyAllocation() {
    // Ribbon Filter uses lazy allocation - filter is sized based on actual keys added,
    // not the maxKeys passed to allocRibbon. This is for the space efficiency.
    int maxKeys = 10000;
    int actualKeys = 100;

    RibbonFilterChunk rf = new RibbonFilterChunk(BloomType.ROW);
    rf.allocRibbon(maxKeys);

    for (int i = 0; i < actualKeys; i++) {
      rf.addKey(Bytes.toBytes(i));
    }
    rf.finalizeRibbon();

    // Verify all keys are found
    for (int i = 0; i < actualKeys; i++) {
      byte[] key = Bytes.toBytes(i);
      assertTrue("Should find key " + i, rf.contains(key, 0, key.length));
    }

    // numSlots should be sized for actualKeys, not maxKeys
    // Use the same overhead ratio that the default constructor uses (for 1% FPR)
    double defaultOverhead =
      RibbonFilterUtil.computeOptimalOverheadForFpRate(0.01, RibbonFilterUtil.DEFAULT_BANDWIDTH);
    int slotsForMaxKeys = InterleavedRibbonSolution
      .roundUpNumSlots(RibbonFilterUtil.computeNumSlots(maxKeys, defaultOverhead));
    assertTrue("Filter should NOT be sized for maxKeys", rf.getNumSlots() < slotsForMaxKeys);

    LOG.info("maxKeys={}, actualKeys={}, slotsForMaxKeys={}, actualSlots={}", maxKeys, actualKeys,
      slotsForMaxKeys, rf.getNumSlots());
  }

  @Test
  public void testDifferentFpRates() {
    int numKeys = 10000;

    // Test with 1% FP rate (default)
    RibbonFilterChunk rf1 = new RibbonFilterChunk(BloomType.ROW);
    rf1.allocRibbon(numKeys);
    for (int i = 0; i < numKeys; i++) {
      rf1.addKey(Bytes.toBytes(i));
    }
    rf1.finalizeRibbon();

    // Test with 0.1% FP rate
    double fpRate2 = 0.001;
    double overhead2 =
      RibbonFilterUtil.computeOptimalOverheadForFpRate(fpRate2, RibbonFilterUtil.DEFAULT_BANDWIDTH);
    RibbonFilterChunk rf2 = new RibbonFilterChunk(RibbonFilterUtil.DEFAULT_BANDWIDTH,
      RibbonFilterUtil.getDefaultHashType(), BloomType.ROW, fpRate2, overhead2);
    rf2.allocRibbon(numKeys);
    for (int i = 0; i < numKeys; i++) {
      rf2.addKey(Bytes.toBytes(i));
    }
    rf2.finalizeRibbon();

    LOG.info("1% FPR: byteSize={}, bitsPerKey={}", rf1.getByteSize(),
      String.format("%.2f", rf1.getByteSize() * 8.0 / numKeys));
    LOG.info("0.1% FPR: byteSize={}, bitsPerKey={}", rf2.getByteSize(),
      String.format("%.2f", rf2.getByteSize() * 8.0 / numKeys));

    // Lower FP rate should use more space
    assertTrue("Lower FP rate should use more bits per key", rf2.getByteSize() > rf1.getByteSize());

    // Verify all keys are found
    for (int i = 0; i < numKeys; i++) {
      byte[] key = Bytes.toBytes(i);
      assertTrue(rf1.contains(key, 0, key.length));
      assertTrue(rf2.contains(key, 0, key.length));
    }
  }
}
