/*
 * Copyright The Apache Software Foundation
 *
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

/**
 * Interface for a secondary level block cache that deals with byte arrays
 * identical to what is written to disk (i.e., usually encoded and compressed),
 * as opposed to HFile objects.
 */
public interface L2Cache {
  /**
   * Retrieve a block from the L2Cache. The block is retrieved as a byte
   * array, in the same exact format as it is stored on disk.
   * @param hfileName Filename associated with the block
   * @param dataBlockOffset Offset in the file
   * @return
   */
  public byte[] getRawBlock(String hfileName, long dataBlockOffset);


  /**
   * Add a block to the L2Cache. The block must be represented by a
   * byte array identical to what would be written to disk.
   * @param hfileName Filename associated with the block
   * @param dataBlockOffset Offset in the file
   * @param rawBlock The exact byte representation of the block
   */
  public void cacheRawBlock(String hfileName, long dataBlockOffset,
      byte[] rawBlock);

  /**
   * Evict all blocks matching a given filename. This operation should be
   * efficient and can be called on each close of a store file.
   * @param hfileName Filename whose blocks to evict
   */
  public int evictBlocksByHfileName(String hfileName);

  /**
   * @return true if the cache has been shutdown
   */
  public boolean isShutdown();

  /**
   * Shutdown the cache
   */
  public void shutdown();
}
