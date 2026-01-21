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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link InterleavedRibbonSolution}.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestInterleavedRibbonSolution {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestInterleavedRibbonSolution.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestInterleavedRibbonSolution.class);

  private static final int COEFF_BITS = InterleavedRibbonSolution.COEFF_BITS;

  @Test
  public void testBandingAndBackSubstitution() {
    int numSlots = InterleavedRibbonSolution.roundUpNumSlots(1000);
    int numKeys = 800;

    RibbonBanding banding = new RibbonBanding(numSlots, COEFF_BITS);
    RibbonHasher hasher = new RibbonHasher(numSlots, COEFF_BITS, Hash.MURMUR_HASH3);

    RibbonHasher.RibbonHashResult[] results = new RibbonHasher.RibbonHashResult[numKeys];
    for (int i = 0; i < numKeys; i++) {
      byte[] key = Bytes.toBytes(i);
      results[i] = hasher.hash(key, 0, key.length);
      banding.add(results[i].start(), results[i].coeffRow());
    }

    assertEquals(numKeys, banding.getNumAdded());

    InterleavedRibbonSolution solution = new InterleavedRibbonSolution(numSlots, 0.01);
    solution.backSubstFrom(banding);

    // All added keys must be found (no false negatives)
    for (int i = 0; i < numKeys; i++) {
      assertTrue("Key " + i + " must be found",
        solution.contains(results[i].start(), results[i].coeffRow(), results[i].resultRow()));
    }
  }

  @Test
  public void testContains() {
    int numSlots = InterleavedRibbonSolution.roundUpNumSlots(500);
    int numKeys = 400;

    RibbonBanding banding = new RibbonBanding(numSlots, COEFF_BITS);
    RibbonHasher hasher = new RibbonHasher(numSlots, COEFF_BITS, Hash.MURMUR_HASH3);

    RibbonHasher.RibbonHashResult[] results = new RibbonHasher.RibbonHashResult[numKeys];
    for (int i = 0; i < numKeys; i++) {
      byte[] key = Bytes.toBytes(i);
      results[i] = hasher.hash(key, 0, key.length);
      banding.add(results[i].start(), results[i].coeffRow());
    }

    InterleavedRibbonSolution solution = new InterleavedRibbonSolution(numSlots, 0.01);
    solution.backSubstFrom(banding);

    // Verify static contains method with ByteBuff (used for reading from HFile)
    long[] segments = solution.getSegments();
    ByteBuffer bb = ByteBuffer.allocate(segments.length * Long.BYTES);
    bb.asLongBuffer().put(segments);
    SingleByteBuff buf = new SingleByteBuff(bb);

    for (int i = 0; i < numKeys; i++) {
      assertTrue("Key " + i + " must be found via static contains",
        InterleavedRibbonSolution.contains(results[i].start(), results[i].coeffRow(),
          results[i].resultRow(), buf, 0, numSlots, solution.getUpperNumColumns(),
          solution.getUpperStartBlock()));
    }
  }

  @Test
  public void testFalsePositiveRate() {
    int numSlots = InterleavedRibbonSolution.roundUpNumSlots(5000);
    int numKeys = 4000;
    int numTests = 50000;
    double targetFpRate = 0.01;

    RibbonBanding banding = new RibbonBanding(numSlots, COEFF_BITS);
    RibbonHasher hasher = new RibbonHasher(numSlots, COEFF_BITS, Hash.MURMUR_HASH3);

    for (int i = 0; i < numKeys; i++) {
      byte[] key = Bytes.toBytes(i);
      RibbonHasher.RibbonHashResult result = hasher.hash(key, 0, key.length);
      banding.add(result.start(), result.coeffRow());
    }

    InterleavedRibbonSolution solution = new InterleavedRibbonSolution(numSlots, targetFpRate);
    solution.backSubstFrom(banding);

    // Test with keys that were NOT added (use range numKeys to numKeys + numTests)
    int falsePositives = 0;
    for (int i = 0; i < numTests; i++) {
      byte[] testKey = Bytes.toBytes(numKeys + i);
      RibbonHasher.RibbonHashResult result = hasher.hash(testKey, 0, testKey.length);
      if (solution.contains(result.start(), result.coeffRow(), result.resultRow())) {
        falsePositives++;
      }
    }

    double actualFpr = (double) falsePositives / numTests;
    LOG.info("False positives: {} / {}", falsePositives, numTests);
    LOG.info("Actual FPR: {}", String.format("%.4f%%", actualFpr * 100));

    // FPR should be within reasonable bounds (allow 5x margin for statistical variance)
    assertTrue("FPR should be less than 5% for 1% target", actualFpr < 0.05);
  }

  @Test
  public void testColumnLayout() {
    int numSlots = 1024;

    // Different FP rates should result in different column layouts
    InterleavedRibbonSolution sol1 = new InterleavedRibbonSolution(numSlots, 0.01); // 1%
    InterleavedRibbonSolution sol2 = new InterleavedRibbonSolution(numSlots, 0.001); // 0.1%

    LOG.info("1% FPR: columns={}, byteSize={}", sol1.getUpperNumColumns(), sol1.getByteSize());
    LOG.info("0.1% FPR: columns={}, byteSize={}", sol2.getUpperNumColumns(), sol2.getByteSize());

    // Lower FPR needs more columns (bits per key)
    assertTrue(sol2.getUpperNumColumns() > sol1.getUpperNumColumns());
    assertTrue(sol2.getByteSize() > sol1.getByteSize());
  }

  @Test
  public void testRoundUpNumSlots() {
    // Minimum is 2 * COEFF_BITS = 128
    assertEquals(128, InterleavedRibbonSolution.roundUpNumSlots(1));
    assertEquals(128, InterleavedRibbonSolution.roundUpNumSlots(128));
    assertEquals(192, InterleavedRibbonSolution.roundUpNumSlots(129));
    assertEquals(1024, InterleavedRibbonSolution.roundUpNumSlots(1000));
  }
}
