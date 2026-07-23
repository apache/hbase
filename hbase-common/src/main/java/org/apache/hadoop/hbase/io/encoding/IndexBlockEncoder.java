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
package org.apache.hadoop.hbase.io.encoding;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface IndexBlockEncoder {
  /**
   * Starts encoding for a block of Index data.
   */
  void startBlockEncoding(boolean rootIndexBlock, DataOutput out) throws IOException;

  /**
   * Encodes index block.
   */
  void encode(List<byte[]> blockKeys, List<Long> blockOffsets, List<Integer> onDiskDataSizes,
    DataOutput out) throws IOException;

  /**
   * Ends encoding for a block of index data.
   */
  void endBlockEncoding(DataOutput out) throws IOException;

  /**
   * Create a HFileIndexBlock seeker which find data within a block.
   * @return A newly created seeker.
   */
  IndexEncodedSeeker createSeeker();

  /**
   * An interface which enable to seek while underlying data is encoded. It works on one HFile Index
   * Block.
   */
  interface IndexEncodedSeeker extends HeapSize {
    /**
     * Init with root index block.
     */
    void initRootIndex(ByteBuff buffer, int numEntries, CellComparator comparator, int treeLevel)
      throws IOException;

    /**
     * Get i's entry in root index block.
     */
    Cell getRootBlockKey(int i);

    int rootBlockContainingKey(Cell key);

    long rootBlockBlockOffsets(int rootLevelIndex);

    int rootBlockOnDiskDataSizes(int rootLevelIndex);

    /**
     * Search non-root index block.
     */
    SearchResult locateNonRootIndexEntry(ByteBuff nonRootBlock, Cell key) throws IOException;
  }

  class SearchResult {
    public int entryIndex;
    public long offset;
    public int onDiskSize;
  }
}
