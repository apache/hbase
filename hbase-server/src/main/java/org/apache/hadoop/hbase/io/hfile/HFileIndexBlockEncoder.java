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

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Controls what kind of index block encoding is used. If index block encoding is not set or the
 * given block is not a index block (encoded or not), methods should just return the unmodified
 * block.
 */
@InterfaceAudience.Private
public interface HFileIndexBlockEncoder {
  /** Type of encoding used for index blocks in HFile. Stored in file info. */
  byte[] INDEX_BLOCK_ENCODING = Bytes.toBytes("INDEX_BLOCK_ENCODING");

  /**
   * Save metadata in HFile which will be written to disk
   * @param writer writer for a given HFile
   * @exception IOException on disk problems
   */
  void saveMetadata(HFile.Writer writer) throws IOException;

  void encode(BlockIndexChunk blockIndexChunk, boolean rootIndexBlock, DataOutput out)
    throws IOException;

  /** Returns the index block encoding */
  IndexBlockEncoding getIndexBlockEncoding();

  EncodedSeeker createSeeker();

  interface EncodedSeeker extends HeapSize {
    void initRootIndex(HFileBlock blk, int numEntries, CellComparator comparator, int treeLevel)
      throws IOException;

    boolean isEmpty();

    ExtendedCell getRootBlockKey(int i);

    int getRootBlockCount();

    ExtendedCell midkey(HFile.CachingBlockReader cachingBlockReader) throws IOException;

    int rootBlockContainingKey(Cell key);

    BlockWithScanInfo loadDataBlockWithScanInfo(ExtendedCell key, HFileBlock currentBlock,
      boolean cacheBlocks, boolean pread, boolean isCompaction,
      DataBlockEncoding expectedDataBlockEncoding, HFile.CachingBlockReader cachingBlockReader)
      throws IOException;
  }
}
