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

import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.encoding.EncodedDataBlock;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * This BlockCompressedSizePredicator implementation adjusts the block size limit based on the
 * compression rate of the block contents read so far. For the first block, adjusted size would be
 * zero, so it performs a compression of current block contents and calculate compression rate and
 * adjusted size. For subsequent blocks, it only performs this calculation once the previous block
 * adjusted size has been reached, and the block is about to be closed.
 */
@InterfaceAudience.Private
public class PreviousBlockCompressionRatePredicator implements BlockCompressedSizePredicator {
  
  int adjustedBlockSize;

  /**
   * Calculates an adjusted block size limit based on the compression rate of current block
   * contents. This calculation is only performed if this is the first block, otherwise, if the
   * adjusted size from previous block has been reached by the current one.
   * @param context the meta file information for the current file.
   * @param uncompressedBlockSize the total uncompressed size read for the block so far.
   * @param contents The byte array containing the block content so far.
   * @return the adjusted block size limit based on block compression rate.
   * @throws IOException
   */
  @Override 
  public int calculateCompressionSizeLimit(HFileContext context, int uncompressedBlockSize, 
      ByteArrayOutputStream contents) throws IOException {
    // In order to avoid excessive compression size calculations, we do it only once when
    // the uncompressed size has reached BLOCKSIZE. We then use this compression size to
    // calculate the compression rate, and adjust the block size limit by this ratio.
    if (adjustedBlockSize == 0 || uncompressedBlockSize >= adjustedBlockSize) {
      int compressedSize = EncodedDataBlock.getCompressedSize(context.getCompression(),
        context.getCompression().getCompressor(), contents.getBuffer(), 0,
        contents.size());
      adjustedBlockSize = uncompressedBlockSize / compressedSize;
      adjustedBlockSize *= context.getBlocksize();
    }
    return adjustedBlockSize;
  }
}
