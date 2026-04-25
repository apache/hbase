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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link BlockCompressedSizePredicator} that uses an Exponentially Weighted Moving Average (EWMA)
 * of the compression ratio to predict the uncompressed block size needed to produce compressed
 * blocks close to the configured target block size.
 */
@InterfaceAudience.Private
public class EWMABlockSizePredicator implements BlockCompressedSizePredicator, Configurable {

  public static final String EWMA_ALPHA_KEY = "hbase.block.compressed.size.predicator.ewma.alpha";
  static final double DEFAULT_ALPHA = 0.5;

  private Configuration conf;
  private double alpha = DEFAULT_ALPHA;
  private double ewmaRatio;
  private int adjustedBlockSize;
  private int configuredMaxBlockSize;
  private boolean initialized;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.alpha = conf.getDouble(EWMA_ALPHA_KEY, DEFAULT_ALPHA);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Estimates the effective compression ratio and adjusts the block size limit. On the first block,
   * the EWMA is seeded with the observed ratio. On subsequent blocks, the ratio is smoothed:
   * {@code ewmaRatio = alpha * currentRatio + (1 - alpha) * ewmaRatio}. If {@code compressed} is
   * zero or negative, the update is skipped to avoid corrupting the EWMA state with
   * {@code Infinity} or {@code NaN}.
   * @param context      HFileContext containing the configured max block size.
   * @param uncompressed the uncompressed (encoded) size of last block written.
   * @param compressed   the compressed size of last block written.
   */
  @Override
  public void updateLatestBlockSizes(HFileContext context, int uncompressed, int compressed) {
    configuredMaxBlockSize = context.getBlocksize();
    if (compressed <= 0) {
      return;
    }
    double currentRatio = (double) uncompressed / compressed;
    if (!initialized) {
      ewmaRatio = currentRatio;
      initialized = true;
    } else {
      ewmaRatio = alpha * currentRatio + (1.0 - alpha) * ewmaRatio;
    }
    adjustedBlockSize = (int) (configuredMaxBlockSize * ewmaRatio);
  }

  /**
   * Returns {@code true} if the block should be finished. Before any block has been written (cold
   * start), returns {@code true} to fall through to the default uncompressed-size-based decision.
   * After the first block, returns {@code true} only when the uncompressed size reaches the
   * EWMA-adjusted target.
   * @param uncompressed the current uncompressed size of the block being written.
   */
  @Override
  public boolean shouldFinishBlock(int uncompressed) {
    if (!initialized) {
      return true;
    }
    if (uncompressed >= configuredMaxBlockSize) {
      return uncompressed >= adjustedBlockSize;
    }
    return false;
  }

  /** Returns the current EWMA compression ratio estimate. Visible for testing. */
  double getEwmaRatio() {
    return ewmaRatio;
  }

  /** Returns the current adjusted block size. Visible for testing. */
  int getAdjustedBlockSize() {
    return adjustedBlockSize;
  }

  /** Returns the current alpha value. Visible for testing. */
  double getAlpha() {
    return alpha;
  }
}
