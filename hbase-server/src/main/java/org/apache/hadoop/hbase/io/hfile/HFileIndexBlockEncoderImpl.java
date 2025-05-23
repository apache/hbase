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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Do different kinds of index block encoding according to column family options.
 */
@InterfaceAudience.Private
public class HFileIndexBlockEncoderImpl implements HFileIndexBlockEncoder {
  private final IndexBlockEncoding indexBlockEncoding;

  /**
   * Do index block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileIndexBlockEncoderImpl(IndexBlockEncoding encoding) {
    this.indexBlockEncoding = encoding != null ? encoding : IndexBlockEncoding.NONE;
  }

  public static HFileIndexBlockEncoder createFromFileInfo(HFileInfo fileInfo) throws IOException {
    IndexBlockEncoding encoding = IndexBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(INDEX_BLOCK_ENCODING);
    if (dataBlockEncodingType != null) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = IndexBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException(
          "Invalid data block encoding type in file info: " + dataBlockEncodingStr, ex);
      }
    }

    if (encoding == IndexBlockEncoding.NONE) {
      return NoOpIndexBlockEncoder.INSTANCE;
    }
    return new HFileIndexBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
    writer.appendFileInfo(INDEX_BLOCK_ENCODING, indexBlockEncoding.getNameInBytes());
  }

  @Override
  public IndexBlockEncoding getIndexBlockEncoding() {
    return indexBlockEncoding;
  }

  @Override
  public void encode(BlockIndexChunk blockIndexChunk, boolean rootIndexBlock, DataOutput out)
    throws IOException {
    IndexBlockEncoder encoder = this.indexBlockEncoding.getEncoder();
    encoder.startBlockEncoding(rootIndexBlock, out);
    encoder.encode(blockIndexChunk.getBlockKeys(), blockIndexChunk.getBlockOffsets(),
      blockIndexChunk.getOnDiskDataSizes(), out);
    encoder.endBlockEncoding(out);
  }

  @Override
  public EncodedSeeker createSeeker() {
    return new IndexBlockEncodedSeeker(this.indexBlockEncoding.getEncoder().createSeeker());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(indexBlockEncoding=" + indexBlockEncoding + ")";
  }

  protected static class IndexBlockEncodedSeeker implements EncodedSeeker {
    private int rootIndexNumEntries;
    protected int searchTreeLevel;
    private IndexBlockEncoder.IndexEncodedSeeker encodedSeeker;

    /**
     * Pre-computed mid-key
     */
    private AtomicReference<Cell> midKey = new AtomicReference<>();

    IndexBlockEncodedSeeker(IndexBlockEncoder.IndexEncodedSeeker encodedSeeker) {
      this.encodedSeeker = encodedSeeker;
    }

    @Override
    public long heapSize() {
      long heapSize = ClassSize.align(ClassSize.OBJECT);

      if (encodedSeeker != null) {
        heapSize += ClassSize.REFERENCE;
        heapSize += ClassSize.align(encodedSeeker.heapSize());
      }

      // the midkey atomicreference
      heapSize += ClassSize.REFERENCE;
      // rootIndexNumEntries searchTreeLevel
      heapSize += 2 * Bytes.SIZEOF_INT;
      return ClassSize.align(heapSize);
    }

    @Override
    public void initRootIndex(HFileBlock blk, int numEntries, CellComparator comparator,
      int treeLevel) throws IOException {
      this.rootIndexNumEntries = numEntries;
      this.searchTreeLevel = treeLevel;
      ByteBuff data = blk.getBufferWithoutHeader();
      encodedSeeker.initRootIndex(data, numEntries, comparator, treeLevel);
    }

    @Override
    public boolean isEmpty() {
      return rootIndexNumEntries <= 0;
    }

    @Override
    public Cell getRootBlockKey(int i) {
      return encodedSeeker.getRootBlockKey(i);
    }

    @Override
    public int getRootBlockCount() {
      return rootIndexNumEntries;
    }

    @Override
    public Cell midkey(HFile.CachingBlockReader cachingBlockReader) throws IOException {
      if (rootIndexNumEntries == 0) {
        throw new IOException("HFile empty");
      }

      Cell targetMidKey = this.midKey.get();
      if (targetMidKey != null) {
        return targetMidKey;
      }
      targetMidKey = getRootBlockKey(rootIndexNumEntries / 2);
      this.midKey.set(targetMidKey);
      return targetMidKey;
    }

    @Override
    public int rootBlockContainingKey(Cell key) {
      return encodedSeeker.rootBlockContainingKey(key);
    }

    @Override
    public BlockWithScanInfo loadDataBlockWithScanInfo(Cell key, HFileBlock currentBlock,
      boolean cacheBlocks, boolean pread, boolean isCompaction,
      DataBlockEncoding expectedDataBlockEncoding, HFile.CachingBlockReader cachingBlockReader)
      throws IOException {
      int rootLevelIndex = rootBlockContainingKey(key);
      if (rootLevelIndex < 0 || rootLevelIndex >= rootIndexNumEntries) {
        return null;
      }

      // Read the next-level (intermediate or leaf) index block.
      long currentOffset = encodedSeeker.rootBlockBlockOffsets(rootLevelIndex);
      int currentOnDiskSize = encodedSeeker.rootBlockOnDiskDataSizes(rootLevelIndex);

      int lookupLevel = 1; // How many levels deep we are in our lookup.
      IndexBlockEncoder.SearchResult searchResult = null;

      HFileBlock block = null;
      while (true) {
        try {
          // Must initialize it with null here, because if don't and once an exception happen in
          // readBlock, then we'll release the previous assigned block twice in the finally block.
          // (See HBASE-22422)
          block = null;
          if (currentBlock != null && currentBlock.getOffset() == currentOffset) {
            // Avoid reading the same block again, even with caching turned off.
            // This is crucial for compaction-type workload which might have
            // caching turned off. This is like a one-block cache inside the
            // scanner.
            block = currentBlock;
          } else {
            // Call HFile's caching block reader API. We always cache index
            // blocks, otherwise we might get terrible performance.
            boolean shouldCache = cacheBlocks || (lookupLevel < searchTreeLevel);
            BlockType expectedBlockType;
            if (lookupLevel < searchTreeLevel - 1) {
              expectedBlockType = BlockType.INTERMEDIATE_INDEX;
            } else if (lookupLevel == searchTreeLevel - 1) {
              expectedBlockType = BlockType.LEAF_INDEX;
            } else {
              // this also accounts for ENCODED_DATA
              expectedBlockType = BlockType.DATA;
            }
            block = cachingBlockReader.readBlock(currentOffset, currentOnDiskSize, shouldCache,
              pread, isCompaction, true, expectedBlockType, expectedDataBlockEncoding);
          }

          if (block == null) {
            throw new IOException("Failed to read block at offset " + currentOffset
              + ", onDiskSize=" + currentOnDiskSize);
          }

          // Found a data block, break the loop and check our level in the tree.
          if (block.getBlockType().isData()) {
            break;
          }

          // Not a data block. This must be a leaf-level or intermediate-level
          // index block. We don't allow going deeper than searchTreeLevel.
          if (++lookupLevel > searchTreeLevel) {
            throw new IOException("Search Tree Level overflow: lookupLevel=" + lookupLevel
              + ", searchTreeLevel=" + searchTreeLevel);
          }

          // Locate the entry corresponding to the given key in the non-root
          // (leaf or intermediate-level) index block.
          ByteBuff buffer = block.getBufferWithoutHeader();
          searchResult = encodedSeeker.locateNonRootIndexEntry(buffer, key);
          if (searchResult.entryIndex == -1) {
            // This has to be changed
            // For now change this to key value
            throw new IOException("The key " + CellUtil.getCellKeyAsString(key) + " is before the"
              + " first key of the non-root index block " + block);
          }

          currentOffset = searchResult.offset;
          currentOnDiskSize = searchResult.onDiskSize;

        } finally {
          if (block != null && !block.getBlockType().isData()) {
            // Release the block immediately if it is not the data block
            block.release();
          }
        }
      }

      if (lookupLevel != searchTreeLevel) {
        assert block.getBlockType().isData();
        // Though we have retrieved a data block we have found an issue
        // in the retrieved data block. Hence returned the block so that
        // the ref count can be decremented
        if (block != null) {
          block.release();
        }
        throw new IOException("Reached a data block at level " + lookupLevel
          + " but the number of levels is " + searchTreeLevel);
      }

      // set the next indexed key for the current block.
      return new BlockWithScanInfo(block, null);
    }
  }
}
