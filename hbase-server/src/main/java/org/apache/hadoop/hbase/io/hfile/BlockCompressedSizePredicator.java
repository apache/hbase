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
 * Allows for defining different compression rate predicates on its implementing classes. Useful
 * when compression is in place, and we want to define block size based on the compressed size,
 * rather than the default behaviour that considers the uncompressed size only. Since we don't
 * actually know the compressed size until we actual apply compression in the block byte buffer, we
 * need to "predicate" this compression rate and minimize compression execution to avoid excessive
 * resources usage. Different approaches for predicating the compressed block size can be defined by
 * implementing classes. The <code>updateLatestBlockSizes</code> allows for updating uncompressed
 * and compressed size values, and is called during block finishing (when we finally apply
 * compression on the block data). Final block size predicate logic is implemented in
 * <code>shouldFinishBlock</code>, which is called by the block writer once uncompressed size has
 * reached the configured BLOCK size, and additional checks should be applied to decide if the block
 * can be finished.
 */
@InterfaceAudience.Private
public interface BlockCompressedSizePredicator {

  String BLOCK_COMPRESSED_SIZE_PREDICATOR = "hbase.block.compressed.size.predicator";

  String MAX_BLOCK_SIZE_UNCOMPRESSED = "hbase.block.max.size.uncompressed";

  /**
   * Updates the predicator with both compressed and uncompressed sizes of latest block written. To
   * be called once the block is finshed and flushed to disk after compression.
   * @param context      the HFileContext containg the configured max block size.
   * @param uncompressed the uncompressed size of last block written.
   * @param compressed   the compressed size of last block written.
   */
  void updateLatestBlockSizes(HFileContext context, int uncompressed, int compressed);

  /**
   * Decides if the block should be finished based on the comparison of its uncompressed size
   * against an adjusted size based on a predicated compression factor.
   * @param uncompressed true if the block should be finished.
   */
  boolean shouldFinishBlock(int uncompressed);

}
