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

import java.io.IOException;

/**
 * This BlockCompressedSizePredicator implementation doesn't actually performs any predicate
 * and simply return the configured BLOCK_SIZE value, without any adjustments. This is the default
 * implementation if <b>hbase.block.compressed.size.predicator</b> property is not defined.
 */
@InterfaceAudience.Private
public class UncompressedBlockSizePredicator implements BlockCompressedSizePredicator {

  /**
   * Returns the configured BLOCK_SIZE as the block size limit, without applying any compression
   * rate adjustments.
   * @param context the meta file information for the current file.
   * @param uncompressedBlockSize the total uncompressed size read for the block so far.
   * @return the configured BLOCK_SIZE as the block size limit, without applying any compression
   * rate adjustments.
   * @throws IOException
   */
  @Override
  public int calculateCompressionSizeLimit(HFileContext context, int uncompressedBlockSize)
    throws IOException {
    return context.getBlocksize();
  }

  /**
   * Empty implementation. Does nothing.
   * @param uncompressed the uncompressed size of last block written.
   * @param compressed the compressed size of last block written.
   */
  @Override
  public void updateLatestBlockSizes(int uncompressed, int compressed) {}

}
