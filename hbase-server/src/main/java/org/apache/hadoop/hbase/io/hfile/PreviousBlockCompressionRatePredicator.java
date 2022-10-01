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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This BlockCompressedSizePredicator implementation adjusts the block size limit based on the
 * compression rate of the block contents read so far. For the first block, adjusted size would be
 * zero, so it performs a compression of current block contents and calculate compression rate and
 * adjusted size. For subsequent blocks, decision whether the block should be finished or not will
 * be based on the compression rate calculated for the previous block.
 */
@InterfaceAudience.Private
public class PreviousBlockCompressionRatePredicator implements BlockCompressedSizePredicator {

  private int adjustedBlockSize;
  private int compressionRatio = 1;
  private int configuredMaxBlockSize;

  /**
   * Recalculates compression rate for the last block and adjusts the block size limit as:
   * BLOCK_SIZE * (uncompressed/compressed).
   * @param context      HFIleContext containing the configured max block size.
   * @param uncompressed the uncompressed size of last block written.
   * @param compressed   the compressed size of last block written.
   */
  @Override
  public void updateLatestBlockSizes(HFileContext context, int uncompressed, int compressed) {
    configuredMaxBlockSize = context.getBlocksize();
    compressionRatio = uncompressed / compressed;
    adjustedBlockSize = context.getBlocksize() * compressionRatio;
  }

  /**
   * Returns <b>true</b> if the passed uncompressed size is larger than the limit calculated by
   * <code>updateLatestBlockSizes</code>.
   * @param uncompressed true if the block should be finished.
   */
  @Override
  public boolean shouldFinishBlock(int uncompressed) {
    if (uncompressed >= configuredMaxBlockSize) {
      return uncompressed >= adjustedBlockSize;
    }
    return false;
  }
}
