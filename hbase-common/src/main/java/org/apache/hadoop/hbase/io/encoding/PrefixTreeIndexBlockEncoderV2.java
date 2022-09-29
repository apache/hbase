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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.PrefixTreeDataWidth;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.TokenizerNode;
import org.apache.hadoop.hbase.io.util.UFIntTool;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class PrefixTreeIndexBlockEncoderV2 implements IndexBlockEncoder {
  private static byte VERSION = 0;

  @Override
  public void startBlockEncoding(boolean rootIndexBlock, DataOutput out) throws IOException {
  }

  @Override
  public void encode(List<byte[]> blockKeys, List<Long> blockOffsets, List<Integer> onDiskDataSizes,
    DataOutput out) throws IOException {
    List<KeyValue.KeyOnlyKeyValue> rowKeys = new ArrayList<>(blockKeys.size());
    for (int i = 0; i < blockKeys.size(); i++) {
      byte[] key = blockKeys.get(i);
      KeyValue.KeyOnlyKeyValue rowKey = new KeyValue.KeyOnlyKeyValue(key, 0, key.length);
      rowKeys.add(rowKey);
    }

    TokenizerNode node = PrefixTreeUtilV2.buildPrefixTree(rowKeys);
    PrefixTreeDataWidth dataWidth = new PrefixTreeDataWidth();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrefixTreeUtilV2.serializePrefixTree(node, dataWidth, outputStream);
    byte[] data = outputStream.toByteArray();

    out.write(VERSION);
    PrefixTreeUtilV2.serialize(out, dataWidth);
    out.writeInt(blockKeys.size());
    out.writeInt(data.length);
    out.write(data);

    long minBlockOffset = blockOffsets.get(0);
    long maxBlockOffset = blockOffsets.get(blockOffsets.size() - 1);
    int minOnDiskDataSize = Integer.MAX_VALUE;
    int maxOnDiskDataSize = Integer.MIN_VALUE;
    for (int i = 0; i < onDiskDataSizes.size(); ++i) {
      if (minOnDiskDataSize > onDiskDataSizes.get(i)) {
        minOnDiskDataSize = onDiskDataSizes.get(i);
      }
      if (maxOnDiskDataSize < onDiskDataSizes.get(i)) {
        maxOnDiskDataSize = onDiskDataSizes.get(i);
      }
    }

    int blockOffsetWidth = UFIntTool.numBytes(maxBlockOffset - minBlockOffset);
    int onDiskDataSizeWidth = UFIntTool.numBytes(maxOnDiskDataSize - minOnDiskDataSize);

    out.write(blockOffsetWidth);
    out.write(onDiskDataSizeWidth);
    out.writeLong(minBlockOffset);
    out.writeInt(minOnDiskDataSize);

    outputStream.reset();
    for (int i = 0; i < blockOffsets.size(); ++i) {
      UFIntTool.writeBytes(blockOffsetWidth, (blockOffsets.get(i) - minBlockOffset), outputStream);
      UFIntTool.writeBytes(onDiskDataSizeWidth, (onDiskDataSizes.get(i) - minOnDiskDataSize),
        outputStream);
    }
    data = outputStream.toByteArray();
    out.write(data);
  }

  @Override
  public void endBlockEncoding(DataOutput out) throws IOException {
  }

  @Override
  public IndexEncodedSeeker createSeeker() {
    return new PrefixTreeIndexBlockEncodedSeeker();
  }

  static class PrefixTreeIndexBlockEncodedSeeker implements IndexEncodedSeeker {

    private PrefixTreeDataWidth dataWidth = new PrefixTreeDataWidth();
    private ByteBuffer prefixTreeNodeData = null;
    private ByteBuffer blockOffsetAndSizeData = null;
    private int blockOffsetWidth;
    private int onDiskDataSizeWidth;
    private long minBlockOffset;
    private int minOnDiskDataSize;

    @Override
    public long heapSize() {
      long heapSize = ClassSize.align(ClassSize.OBJECT);

      if (prefixTreeNodeData != null) {
        heapSize += ClassSize.align(ClassSize.BYTE_BUFFER + prefixTreeNodeData.capacity());
      }
      if (blockOffsetAndSizeData != null) {
        heapSize += ClassSize.align(ClassSize.BYTE_BUFFER + blockOffsetAndSizeData.capacity());
      }

      // dataWidth
      heapSize += ClassSize.REFERENCE;
      // blockOffsetWidth onDiskDataSizeWidth minOnDiskDataSize
      heapSize += 3 * Bytes.SIZEOF_INT;
      // PrefixTreeDataWidth's data.
      heapSize += 5 * Bytes.SIZEOF_INT;
      // minBlockOffset
      heapSize += Bytes.SIZEOF_LONG;
      return ClassSize.align(heapSize);
    }

    @Override
    public void initRootIndex(ByteBuff data, int numEntries, CellComparator comparator,
      int treeLevel) throws IOException {
      byte version = data.get();
      if (version != VERSION) {
        throw new IOException("Corrupted data, version should be 0, but it is " + version);
      }
      PrefixTreeUtilV2.deserialize(data, dataWidth);
      int numEntry = data.getInt();
      int prefixNodeLength = data.getInt();

      ObjectIntPair<ByteBuffer> tmpPair = new ObjectIntPair<>();
      data.asSubByteBuffer(data.position(), prefixNodeLength, tmpPair);
      ByteBuffer dup = tmpPair.getFirst().duplicate();
      dup.position(tmpPair.getSecond());
      dup.limit(tmpPair.getSecond() + prefixNodeLength);
      prefixTreeNodeData = dup.slice();

      data.skip(prefixNodeLength);
      blockOffsetWidth = data.get();
      onDiskDataSizeWidth = data.get();
      minBlockOffset = data.getLong();
      minOnDiskDataSize = data.getInt();
      int blockOffsetsAndonDiskDataSize = numEntry * (blockOffsetWidth + onDiskDataSizeWidth);

      data.asSubByteBuffer(data.position(), blockOffsetsAndonDiskDataSize, tmpPair);
      dup = tmpPair.getFirst().duplicate();
      dup.position(tmpPair.getSecond());
      dup.limit(tmpPair.getSecond() + blockOffsetsAndonDiskDataSize);
      blockOffsetAndSizeData = dup.slice();
    }

    @Override
    public Cell getRootBlockKey(int i) {
      byte[] row = PrefixTreeUtilV2.get(prefixTreeNodeData, 0, dataWidth, i);
      return PrivateCellUtil.createFirstOnRow(row);
    }

    @Override
    public int rootBlockContainingKey(Cell key) {
      return PrefixTreeUtilV2.search(prefixTreeNodeData, 0, key, 0, dataWidth);
    }

    @Override
    public long rootBlockBlockOffsets(int rootLevelIndex) {
      int pos = rootLevelIndex * (blockOffsetWidth + onDiskDataSizeWidth);
      return UFIntTool.fromBytes(blockOffsetAndSizeData, pos, blockOffsetWidth) + minBlockOffset;
    }

    @Override
    public int rootBlockOnDiskDataSizes(int rootLevelIndex) {
      int pos = rootLevelIndex * (blockOffsetWidth + onDiskDataSizeWidth);
      int currentOnDiskSize = (int) UFIntTool.fromBytes(blockOffsetAndSizeData,
        pos + blockOffsetWidth, onDiskDataSizeWidth) + minOnDiskDataSize;
      return currentOnDiskSize;
    }

    @Override
    public SearchResult locateNonRootIndexEntry(ByteBuff nonRootBlock, Cell key)
      throws IOException {
      PrefixTreeDataWidth meta = new PrefixTreeDataWidth();
      byte version = nonRootBlock.get();
      if (version != VERSION) {
        throw new IOException("Corrupted data, version should be 0, but it is " + version);
      }
      PrefixTreeUtilV2.deserialize(nonRootBlock, meta);
      int numEntry = nonRootBlock.getInt();
      int prefixNodeLength = nonRootBlock.getInt();

      ObjectIntPair<ByteBuffer> tmpPair = new ObjectIntPair<>();
      nonRootBlock.asSubByteBuffer(nonRootBlock.position(), prefixNodeLength, tmpPair);
      ByteBuffer dup = tmpPair.getFirst().duplicate();
      dup.position(tmpPair.getSecond());
      dup.limit(tmpPair.getSecond() + prefixNodeLength);
      ByteBuffer prefixTreeNodeData = dup.slice();

      nonRootBlock.skip(prefixNodeLength);

      int entryIndex = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, key, 0, meta);
      SearchResult result = new SearchResult();
      result.entryIndex = entryIndex;

      if (entryIndex >= 0 && entryIndex < numEntry) {
        int blockOffsetWidth = nonRootBlock.get();
        int onDiskDataSizeWidth = nonRootBlock.get();
        long minBlockOffset = nonRootBlock.getLong();
        int minOnDiskDataSize = nonRootBlock.getInt();

        int pos = nonRootBlock.position() + entryIndex * (blockOffsetWidth + onDiskDataSizeWidth);
        result.offset = UFIntTool.fromBytes(nonRootBlock, pos, blockOffsetWidth) + minBlockOffset;
        result.onDiskSize =
          (int) UFIntTool.fromBytes(nonRootBlock, pos + blockOffsetWidth, onDiskDataSizeWidth)
            + minOnDiskDataSize;
      }

      return result;
    }
  }
}
