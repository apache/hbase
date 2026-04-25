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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestEWMABlockSizePredicator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEWMABlockSizePredicator.class);

  private static final int BLOCK_SIZE_64KB = 64 * 1024;
  private static final int BLOCK_SIZE_1MB = 1024 * 1024;

  private static HFileContext contextWithBlockSize(int blockSize) {
    return new HFileContextBuilder().withBlockSize(blockSize).build();
  }

  /**
   * Verify that double-precision arithmetic preserves fractional compression ratios and that the
   * adjusted block size reflects the full ratio.
   */
  @Test
  public void testDoublePrecisionRatio() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_64KB);

    // 3.4:1 ratio — the fractional part matters
    predicator.updateLatestBlockSizes(ctx, 68000, 20000);

    assertEquals(3.4, predicator.getEwmaRatio(), 0.001);
    int expectedAdjusted = (int) (BLOCK_SIZE_64KB * 3.4);
    assertEquals(expectedAdjusted, predicator.getAdjustedBlockSize());
  }

  /**
   * Feed a sequence of blocks with a consistent ratio and verify the EWMA remains stable.
   */
  @Test
  public void testConvergenceWithConstantRatio() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_64KB);

    for (int i = 0; i < 5; i++) {
      predicator.updateLatestBlockSizes(ctx, 60000, 20000); // 3.0:1
      assertEquals("EWMA should be stable at block " + (i + 1), 3.0, predicator.getEwmaRatio(),
        0.001);
    }

    int expectedAdjusted = (int) (BLOCK_SIZE_64KB * 3.0);
    assertEquals(expectedAdjusted, predicator.getAdjustedBlockSize());
  }

  /**
   * Feed blocks with alternating high/low compression ratios and verify the EWMA converges toward
   * the mean and that the adjusted block size swing decreases over successive pairs.
   */
  @Test
  public void testSmoothingUnderVariance() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_64KB);
    double meanRatio = 3.0;

    // Alternating ratios: 4.0:1 and 2.0:1 (mean = 3.0)
    int[][] blocks = { { 80000, 20000 }, // 4.0:1
      { 40000, 20000 }, // 2.0:1
      { 80000, 20000 }, // 4.0:1
      { 40000, 20000 }, // 2.0:1
      { 80000, 20000 }, // 4.0:1
      { 40000, 20000 }, // 2.0:1
    };

    int lastAdj = 0;
    int firstPairSwing = 0;
    int lastPairSwing = 0;

    for (int i = 0; i < blocks.length; i++) {
      predicator.updateLatestBlockSizes(ctx, blocks[i][0], blocks[i][1]);
      int adj = predicator.getAdjustedBlockSize();

      if (i > 0) {
        int swing = Math.abs(adj - lastAdj);
        if (i <= 2) {
          firstPairSwing += swing;
        }
        if (i >= blocks.length - 2) {
          lastPairSwing += swing;
        }
      }
      lastAdj = adj;
    }

    assertTrue("Swing should decrease as EWMA converges: first pair swing=" + firstPairSwing
      + " last pair swing=" + lastPairSwing, lastPairSwing < firstPairSwing);

    // After several alternating blocks, the ratio should be near the mean.
    // With alpha=0.5 the EWMA is biased toward the most recent sample, so the
    // tolerance must account for ending on a low-ratio block (exact value: 2.6875).
    assertEquals(meanRatio, predicator.getEwmaRatio(), 0.5);
  }

  /**
   * Before any block has been written, {@code shouldFinishBlock} returns {@code true} (falling
   * through to default sizing). After the first block, the predicator gates on the adjusted size.
   */
  @Test
  public void testColdStartBehavior() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();

    assertTrue("Cold start: shouldFinishBlock should return true before initialization",
      predicator.shouldFinishBlock(BLOCK_SIZE_64KB));
    assertTrue("Cold start: shouldFinishBlock should return true for any size",
      predicator.shouldFinishBlock(1));

    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_64KB);
    predicator.updateLatestBlockSizes(ctx, 68000, 20000); // 3.4:1

    int adjustedSize = predicator.getAdjustedBlockSize();

    assertFalse("After init: block below configured size should not finish",
      predicator.shouldFinishBlock(BLOCK_SIZE_64KB - 1));
    assertFalse("After init: block at configured size should not finish (needs to grow)",
      predicator.shouldFinishBlock(BLOCK_SIZE_64KB));
    assertTrue("After init: block at adjusted size should finish",
      predicator.shouldFinishBlock(adjustedSize));
    assertTrue("After init: block above adjusted size should finish",
      predicator.shouldFinishBlock(adjustedSize + 1));
  }

  /**
   * Verify the predicator works correctly with a large configured block size.
   */
  @Test
  public void testLargeBlockSize() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_1MB);

    predicator.updateLatestBlockSizes(ctx, 340000, 100000); // 3.4:1

    assertEquals(3.4, predicator.getEwmaRatio(), 0.001);
    int expectedAdjusted = (int) (BLOCK_SIZE_1MB * 3.4);
    assertEquals(expectedAdjusted, predicator.getAdjustedBlockSize());
  }

  /**
   * Verify that the EWMA alpha can be configured. A lower alpha should smooth more aggressively,
   * producing a slower response to a sudden ratio change.
   */
  @Test
  public void testConfigurableAlpha() {
    Configuration conf = new Configuration(false);
    conf.setDouble(EWMABlockSizePredicator.EWMA_ALPHA_KEY, 0.2);

    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    predicator.setConf(conf);
    assertEquals(0.2, predicator.getAlpha(), 0.001);

    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_64KB);

    // Seed with 3.0:1
    predicator.updateLatestBlockSizes(ctx, 60000, 20000);
    assertEquals(3.0, predicator.getEwmaRatio(), 0.001);

    // Spike to 5.0:1 — with alpha=0.2: ewma = 0.2*5.0 + 0.8*3.0 = 3.4
    predicator.updateLatestBlockSizes(ctx, 100000, 20000);
    assertEquals(3.4, predicator.getEwmaRatio(), 0.001);

    // With default alpha=0.5 the same spike yields 4.0 — lower alpha is more conservative
    EWMABlockSizePredicator fast = new EWMABlockSizePredicator();
    fast.updateLatestBlockSizes(ctx, 60000, 20000);
    fast.updateLatestBlockSizes(ctx, 100000, 20000);
    assertEquals(4.0, fast.getEwmaRatio(), 0.001);

    assertTrue("Lower alpha should dampen the spike more",
      predicator.getEwmaRatio() < fast.getEwmaRatio());
  }

  /**
   * The default alpha is used when no configuration is set or when the configuration omits the
   * alpha key.
   */
  @Test
  public void testDefaultAlphaWithoutConfiguration() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    assertEquals(EWMABlockSizePredicator.DEFAULT_ALPHA, predicator.getAlpha(), 0.0);

    Configuration emptyConf = new Configuration(false);
    predicator.setConf(emptyConf);
    assertEquals(EWMABlockSizePredicator.DEFAULT_ALPHA, predicator.getAlpha(), 0.0);
  }

  /**
   * Verify that {@code compressed <= 0} is handled gracefully: the update is skipped and the EWMA
   * state is not corrupted.
   */
  @Test
  public void testCompressedSizeZeroOrNegative() {
    EWMABlockSizePredicator predicator = new EWMABlockSizePredicator();
    HFileContext ctx = contextWithBlockSize(BLOCK_SIZE_64KB);

    // compressed=0 before initialization — should remain uninitialized
    predicator.updateLatestBlockSizes(ctx, 68000, 0);
    assertTrue("Should still be in cold-start state after compressed=0",
      predicator.shouldFinishBlock(BLOCK_SIZE_64KB));

    // Initialize with a valid block
    predicator.updateLatestBlockSizes(ctx, 68000, 20000);
    double ratioAfterInit = predicator.getEwmaRatio();
    int adjustedAfterInit = predicator.getAdjustedBlockSize();
    assertEquals(3.4, ratioAfterInit, 0.001);

    // compressed=0 after initialization — state should be unchanged
    predicator.updateLatestBlockSizes(ctx, 68000, 0);
    assertEquals(ratioAfterInit, predicator.getEwmaRatio(), 0.0);
    assertEquals(adjustedAfterInit, predicator.getAdjustedBlockSize());

    // compressed=-1 — state should be unchanged
    predicator.updateLatestBlockSizes(ctx, 68000, -1);
    assertEquals(ratioAfterInit, predicator.getEwmaRatio(), 0.0);
    assertEquals(adjustedAfterInit, predicator.getAdjustedBlockSize());
  }
}
