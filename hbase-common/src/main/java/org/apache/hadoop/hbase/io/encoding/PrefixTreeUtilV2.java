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
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.PrefixTreeDataWidth;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.TokenizerNode;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.TokenizerNodeMeta;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.io.util.UFIntTool;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class PrefixTreeUtilV2 {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixTreeUtilV2.class);

  /**
   * Build tree from begin
   * @return the tree
   */
  public static TokenizerNode buildPrefixTree(List<KeyValue.KeyOnlyKeyValue> rowKeys) {
    // root node.
    TokenizerNode node = new TokenizerNode();
    int start = 0;
    // Get max common prefix
    int common = maxCommonPrefix(rowKeys, 0, rowKeys.size() - 1, 0);
    if (common > 0) {
      byte[] commonB =
        Bytes.copy(rowKeys.get(0).getRowArray(), rowKeys.get(0).getRowOffset(), common);
      node.nodeData = commonB;
      for (int i = 0; i < rowKeys.size(); i++) {
        if (rowKeys.get(i).getRowLength() == common) {
          node.numOccurrences++;
          if (node.index == null) {
            node.index = new ArrayList<>(1);
          }
          node.index.add(i);
          if (node.keys == null) {
            node.keys = new ArrayList<>(1);
          }
          node.keys.add(rowKeys.get(i));
          start = i + 1;
        } else {
          break;
        }
      }
    } else {
      // Only root node data can be empty.
      node.nodeData = new byte[0];
    }
    if (start <= rowKeys.size() - 1) {
      constructAndSplitChild(node, rowKeys, start, rowKeys.size() - 1, common);
    }
    return node;
  }

  /**
   * Calculate max common prefix
   * @return the max common prefix num bytes
   */
  static int maxCommonPrefix(List<KeyValue.KeyOnlyKeyValue> rowKeys, int start, int end,
    int startPos) {
    // only one entry.
    if (start == end) {
      return rowKeys.get(start).getRowLength() - startPos;
    }
    int common = 0;
    KeyValue.KeyOnlyKeyValue startRowKey = rowKeys.get(start);
    for (int round = 0; round <= startRowKey.getRowLength() - startPos - 1; round++) {
      boolean same = true;
      for (int i = start + 1; i <= end; i++) {
        KeyValue.KeyOnlyKeyValue rowKey = rowKeys.get(i);
        if (startPos + common > rowKey.getRowLength() - 1) {
          same = false;
          break;
        }
        if (
          startRowKey.getRowArray()[startRowKey.getRowOffset() + startPos + common]
              != rowKey.getRowArray()[rowKey.getRowOffset() + startPos + common]
        ) {
          same = false;
          break;
        }
      }
      if (same) {
        common++;
      } else {
        break;
      }
    }
    return common;
  }

  /**
   * No common prefix split it.
   */
  static void constructAndSplitChild(TokenizerNode node, List<KeyValue.KeyOnlyKeyValue> rowKeys,
    int start, int end, int startPos) {
    int middle = start;
    KeyValue.KeyOnlyKeyValue startRowKey = rowKeys.get(start);
    for (int i = start + 1; i <= end; i++) {
      if (startPos > rowKeys.get(i).getRowLength() - 1) {
        middle = i - 1;
        break;
      }
      KeyValue.KeyOnlyKeyValue rowKey = rowKeys.get(i);
      if (
        startRowKey.getRowArray()[startRowKey.getRowOffset() + startPos]
            != rowKey.getRowArray()[rowKey.getRowOffset() + startPos]
      ) {
        middle = i - 1;
        break;
      }
      if (i == end) {
        middle = end;
      }
    }
    constructCommonNodeAndChild(node, rowKeys, start, middle, startPos);
    if (middle + 1 <= end) {
      // right
      constructCommonNodeAndChild(node, rowKeys, middle + 1, end, startPos);
    }
  }

  /**
   * Get max common prefix as node and build children.
   */
  static TokenizerNode constructCommonNodeAndChild(TokenizerNode node,
    List<KeyValue.KeyOnlyKeyValue> rowKeys, int start, int end, int startPos) {
    int common = maxCommonPrefix(rowKeys, start, end, startPos);
    if (common > 0) {
      TokenizerNode child = new TokenizerNode();
      child.parent = node;
      node.children.add(child);
      byte[] commonB = Bytes.copy(rowKeys.get(start).getRowArray(),
        rowKeys.get(start).getRowOffset() + startPos, common);
      child.nodeData = commonB;
      int newStart = start;
      for (int i = start; i <= end; i++) {
        if (rowKeys.get(i).getRowLength() == (startPos + common)) {
          child.numOccurrences++;
          if (child.index == null) {
            child.index = new ArrayList<>(1);
          }
          child.index.add(i);
          if (child.keys == null) {
            child.keys = new ArrayList<>(1);
          }
          child.keys.add(rowKeys.get(i));
          newStart = i + 1;
        } else {
          break;
        }
      }
      if (start != end && newStart <= end) {
        if (newStart == start) {
          // no common prefix.
          constructAndSplitChild(child, rowKeys, newStart, end, startPos + common);
        } else {
          // can have common prefix.
          constructCommonNodeAndChild(child, rowKeys, newStart, end, startPos + common);
        }
      }
    } else {
      // no common prefix, split
      constructAndSplitChild(node, rowKeys, start, end, startPos);
    }
    return node;
  }

  static void getNodeMetaInfo(TokenizerNode node, TokenizerNodeMeta meta) {
    if (node.nodeData.length > meta.maxNodeDataLength) {
      meta.maxNodeDataLength = node.nodeData.length;
    }
    meta.totalNodeDataLength += node.nodeData.length;
    meta.countNodeDataNum++;

    if (node.children.size() > meta.maxFanOut) {
      meta.maxFanOut = node.children.size();
    }
    meta.totalChildNum += node.children.size();
    meta.countChildNum++;

    if (node.numOccurrences > meta.maxNumOccurrences) {
      meta.maxNumOccurrences = node.numOccurrences;
    }
    meta.totalIndexNum += node.numOccurrences;
    meta.countNumOccurrences++;

    if (node.index != null) {
      for (Integer entry : node.index) {
        if (entry > meta.maxIndex) {
          meta.maxIndex = entry;
        }
      }
    }
    if (node.keys != null) {
      for (KeyValue.KeyOnlyKeyValue keyValue : node.keys) {
        int qualifierLength = keyValue.getQualifierLength();
        if (qualifierLength > 0) {
          meta.countQualifierNum++;
          if (qualifierLength > meta.maxQualifierLength) {
            meta.maxQualifierLength = qualifierLength;
          }
          meta.totalQualifierLength += qualifierLength;
          node.qualifierNum++;
          node.qualifierLength += qualifierLength;
        }
      }
    }
    if (node.children.isEmpty()) {
      meta.leafNodes.add(node);
    } else {
      meta.nonLeafNodes.add(node);
    }
    for (TokenizerNode child : node.children) {
      getNodeMetaInfo(child, meta);
    }
  }

  public static void serializePrefixTree(TokenizerNode node, PrefixTreeDataWidth dataWidth,
    ByteArrayOutputStream os) throws IOException {
    TokenizerNodeMeta meta = new TokenizerNodeMeta();
    getNodeMetaInfo(node, meta);

    dataWidth.nodeDataLengthWidth = UFIntTool.numBytes(meta.maxNodeDataLength);
    dataWidth.fanOutWidth = UFIntTool.numBytes(meta.maxFanOut);
    dataWidth.occurrencesWidth = UFIntTool.numBytes(meta.maxNumOccurrences * 2 + 1);
    dataWidth.indexWidth = UFIntTool.numBytes(meta.maxIndex);
    dataWidth.qualifierLengthWidth = UFIntTool.numBytes(meta.maxQualifierLength);

    calculateSerializeInfo(meta, dataWidth);

    serialize(meta, os, dataWidth);
  }

  static void calculateSerializeInfo(TokenizerNodeMeta meta, PrefixTreeDataWidth dataWidth) {
    int totalLength = 0;
    int nextNodeOffsetNum = 0;
    for (TokenizerNode leafNode : meta.leafNodes) {
      totalLength += dataWidth.nodeDataLengthWidth;
      totalLength += leafNode.nodeData.length;
      if (leafNode.parent != null) {
        // exclude child's first bytes, child's first byte stored in parent node.
        totalLength = totalLength - 1;
      }
      // fan Out
      totalLength += dataWidth.fanOutWidth;
      // fan Byte
      totalLength += leafNode.children.size();
      nextNodeOffsetNum += leafNode.children.size();

      totalLength += dataWidth.occurrencesWidth;
      totalLength += (leafNode.numOccurrences * dataWidth.indexWidth);

      if (leafNode.qualifierNum > 0) {
        // qualifier
        for (int i = 0; i < leafNode.numOccurrences; i++) {
          int qualifierLength = leafNode.keys.get(i).getQualifierLength();
          if (qualifierLength > 0) {
            totalLength += dataWidth.qualifierLengthWidth;
            totalLength += qualifierLength;
            totalLength += (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG);
          } else {
            totalLength += dataWidth.qualifierLengthWidth;
          }
        }
      }
    }
    for (TokenizerNode nonLeafNode : meta.nonLeafNodes) {
      totalLength += dataWidth.nodeDataLengthWidth;
      totalLength += nonLeafNode.nodeData.length;
      if (nonLeafNode.parent != null) {
        // exclude child's first bytes, child's first byte stored in parent node.
        totalLength = totalLength - 1;
      }
      // fan Out
      totalLength += dataWidth.fanOutWidth;
      // fan Byte
      totalLength += nonLeafNode.children.size();
      nextNodeOffsetNum += nonLeafNode.children.size();

      totalLength += dataWidth.occurrencesWidth;
      totalLength += (nonLeafNode.numOccurrences * dataWidth.indexWidth);

      if (nonLeafNode.qualifierNum > 0) {
        // qualifier
        for (int i = 0; i < nonLeafNode.numOccurrences; i++) {
          int qualifierLength = nonLeafNode.keys.get(i).getQualifierLength();
          if (qualifierLength > 0) {
            totalLength += dataWidth.qualifierLengthWidth;
            totalLength += qualifierLength;
            totalLength += (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG);
          } else {
            totalLength += dataWidth.qualifierLengthWidth;
          }
        }
      }
    }

    int totalBytesWithoutOffsets = totalLength;
    // figure out how wide our offset FInts are
    int offsetWidth = 0;
    while (true) {
      ++offsetWidth;
      int numBytesFinder = totalBytesWithoutOffsets + (offsetWidth * nextNodeOffsetNum);
      if (numBytesFinder < UFIntTool.maxValueForNumBytes(offsetWidth)) {
        totalLength = numBytesFinder;
        break;
      } // it fits
    }
    dataWidth.childNodeOffsetWidth = offsetWidth;

    // track the starting position of each node in final output
    int negativeIndex = 0;
    for (int i = meta.leafNodes.size() - 1; i >= 0; i--) {
      TokenizerNode leaf = meta.leafNodes.get(i);
      int leafNodeWidth = dataWidth.nodeDataLengthWidth + leaf.nodeData.length;
      if (leaf.parent != null) {
        // leaves store all but their first token byte
        leafNodeWidth = leafNodeWidth - 1;
      }
      // leaf node, no children.
      leafNodeWidth += dataWidth.fanOutWidth;
      // no fanOut bytes and nextNodeOffset
      // index
      leafNodeWidth += dataWidth.occurrencesWidth + leaf.numOccurrences * dataWidth.indexWidth;
      if (leaf.qualifierNum > 0) {
        // qualifier
        for (int j = 0; j < leaf.numOccurrences; j++) {
          int qualifierLength = leaf.keys.get(j).getQualifierLength();
          if (qualifierLength > 0) {
            leafNodeWidth += dataWidth.qualifierLengthWidth;
            leafNodeWidth += qualifierLength;
            leafNodeWidth += (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG);
          } else {
            leafNodeWidth += dataWidth.qualifierLengthWidth;
          }
        }
      }
      negativeIndex += leafNodeWidth;
      leaf.nodeWidth = leafNodeWidth;
      leaf.negativeIndex = negativeIndex;
    }
    for (int i = meta.nonLeafNodes.size() - 1; i >= 0; i--) {
      TokenizerNode nonLeaf = meta.nonLeafNodes.get(i);
      int leafNodeWidth = dataWidth.nodeDataLengthWidth + nonLeaf.nodeData.length;
      if (nonLeaf.parent != null) {
        leafNodeWidth = leafNodeWidth - 1;
      }
      // fanOut, children's first byte, and children's offset.
      leafNodeWidth += dataWidth.fanOutWidth + nonLeaf.children.size()
        + nonLeaf.children.size() * dataWidth.childNodeOffsetWidth;
      // index
      leafNodeWidth += dataWidth.occurrencesWidth + nonLeaf.numOccurrences * dataWidth.indexWidth;
      if (nonLeaf.qualifierNum > 0) {
        // qualifier
        for (int j = 0; j < nonLeaf.numOccurrences; j++) {
          int qualifierLength = nonLeaf.keys.get(j).getQualifierLength();
          if (qualifierLength > 0) {
            leafNodeWidth += dataWidth.qualifierLengthWidth;
            leafNodeWidth += qualifierLength;
            leafNodeWidth += (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG);
          } else {
            leafNodeWidth += dataWidth.qualifierLengthWidth;
          }
        }
      }
      negativeIndex += leafNodeWidth;
      nonLeaf.nodeWidth = leafNodeWidth;
      nonLeaf.negativeIndex = negativeIndex;
    }
  }

  static void serialize(TokenizerNodeMeta meta, ByteArrayOutputStream os,
    PrefixTreeDataWidth dataWidth) throws IOException {
    for (int i = 0; i < meta.nonLeafNodes.size(); i++) {
      serialize(meta.nonLeafNodes.get(i), os, dataWidth);
    }
    for (int i = 0; i < meta.leafNodes.size(); i++) {
      serialize(meta.leafNodes.get(i), os, dataWidth);
    }
  }

  static void serialize(TokenizerNode node, ByteArrayOutputStream os, PrefixTreeDataWidth dataWidth)
    throws IOException {
    if (node.parent != null) {
      // The first byte do not need to store, it store in the parent.
      if (node.nodeData.length - 1 > 0) {
        UFIntTool.writeBytes(dataWidth.nodeDataLengthWidth, node.nodeData.length - 1, os);
        os.write(node.nodeData, 1, node.nodeData.length - 1);
      } else {
        UFIntTool.writeBytes(dataWidth.nodeDataLengthWidth, 0, os);
      }
    } else {
      UFIntTool.writeBytes(dataWidth.nodeDataLengthWidth, node.nodeData.length, os);
      os.write(node.nodeData, 0, node.nodeData.length);
    }
    UFIntTool.writeBytes(dataWidth.fanOutWidth, node.children.size(), os);
    for (TokenizerNode child : node.children) {
      // child's first byte.
      os.write(child.nodeData[0]);
    }
    for (TokenizerNode child : node.children) {
      UFIntTool.writeBytes(dataWidth.childNodeOffsetWidth, node.negativeIndex - child.negativeIndex,
        os);
    }
    int occurrences = node.numOccurrences << 1;
    if (node.qualifierNum > 0) {
      occurrences = occurrences | 0x01;
    }
    UFIntTool.writeBytes(dataWidth.occurrencesWidth, occurrences, os);
    for (int i = 0; i < node.numOccurrences; i++) {
      UFIntTool.writeBytes(dataWidth.indexWidth, node.index.get(i), os);
    }
    if (node.qualifierNum > 0) {
      for (int i = 0; i < node.numOccurrences; i++) {
        KeyValue.KeyOnlyKeyValue keyOnlyKeyValue = node.keys.get(i);
        if (keyOnlyKeyValue.getQualifierLength() > 0) {
          UFIntTool.writeBytes(dataWidth.qualifierLengthWidth, keyOnlyKeyValue.getQualifierLength(),
            os);
          os.write(keyOnlyKeyValue.getQualifierArray(), keyOnlyKeyValue.getQualifierOffset(),
            keyOnlyKeyValue.getQualifierLength());
          // write timestamp
          StreamUtils.writeLong(os, keyOnlyKeyValue.getTimestamp());
          // write the type
          os.write(keyOnlyKeyValue.getTypeByte());
        } else {
          UFIntTool.writeBytes(dataWidth.qualifierLengthWidth, 0, os);
        }
      }
    }
  }

  public static void serialize(DataOutput out, PrefixTreeDataWidth dataWidth) throws IOException {
    out.writeByte(dataWidth.nodeDataLengthWidth);
    out.writeByte(dataWidth.fanOutWidth);
    out.writeByte(dataWidth.occurrencesWidth);
    out.writeByte(dataWidth.indexWidth);
    out.writeByte(dataWidth.childNodeOffsetWidth);
    out.writeByte(dataWidth.qualifierLengthWidth);
  }

  public static void deserialize(ByteBuff data, PrefixTreeDataWidth dataWidth) {
    dataWidth.nodeDataLengthWidth = data.get();
    dataWidth.fanOutWidth = data.get();
    dataWidth.occurrencesWidth = data.get();
    dataWidth.indexWidth = data.get();
    dataWidth.childNodeOffsetWidth = data.get();
    dataWidth.qualifierLengthWidth = data.get();
  }

  /**
   * Get the node index, that search key >= index and search key < (index + 1)
   */
  public static int search(ByteBuffer data, int bbStartPos, Cell skey, int keyStartPos,
    PrefixTreeDataWidth meta) {
    int nodeDataLength = getNodeDataLength(data, bbStartPos, meta);
    int cs = 0;
    if (nodeDataLength > 0) {
      cs = compareTo(skey, keyStartPos, Math.min(skey.getRowLength() - keyStartPos, nodeDataLength),
        data, bbStartPos + meta.nodeDataLengthWidth, nodeDataLength);
    }

    int pos = bbStartPos + meta.nodeDataLengthWidth + nodeDataLength;
    int fanOut = getNodeFanOut(data, pos, meta);
    pos += meta.fanOutWidth + fanOut + fanOut * meta.childNodeOffsetWidth;
    int tmpNumOccurrences = getNodeNumOccurrences(data, pos, meta);
    int numOccurrences = tmpNumOccurrences >> 1;
    int hasQualifier = tmpNumOccurrences & 0x01;

    pos += meta.occurrencesWidth;

    if (cs == 0) {
      // continue search
      if (fanOut == 0) {
        // no children, should be numOccurrences > 0
        if (skey.getRowLength() == keyStartPos + nodeDataLength) {
          if (hasQualifier == 0) {
            // == current node
            return getNodeIndex(data, pos, 0, meta);
          } else {
            // compare qualifier
            int qualifierPos = pos + numOccurrences * meta.indexWidth;
            if (skey.getQualifierLength() == 0) {
              int firstQualifierLength = getQualifierLength(data, qualifierPos, meta);
              if (firstQualifierLength == 0) {
                return getNodeIndex(data, pos, 0, meta);
              } else {
                // search key has no qualifier, but first index node has.
                return getNodeIndex(data, pos, 0, meta) - 1;
              }
            } else {
              for (int i = 0; i < numOccurrences; i++) {
                int qualifierLength = getQualifierLength(data, qualifierPos, meta);
                qualifierPos += meta.qualifierLengthWidth;
                int qualifierCR = compareQualifierTo(skey, data, qualifierPos, qualifierLength);
                if (qualifierCR == 0) {
                  // the same qualifier.
                  int timestampPos = qualifierPos + qualifierLength;
                  long timestamp = ByteBufferUtils.toLong(data, timestampPos);
                  byte byteType = ByteBufferUtils.toByte(data, timestampPos + Bytes.SIZEOF_LONG);
                  // higher numbers sort before those of lesser numbers.
                  if (skey.getTimestamp() > timestamp) {
                    return getNodeIndex(data, pos, i, meta) - 1;
                  } else if (skey.getTimestamp() < timestamp) {
                    return getNodeIndex(data, pos, i, meta);
                  }
                  // higher numbers sort before those of lesser numbers.
                  if ((0xff & skey.getTypeByte() - (0xff & byteType)) > 0) {
                    return getNodeIndex(data, pos, i, meta) - 1;
                  } else {
                    return getNodeIndex(data, pos, i, meta);
                  }
                } else if (qualifierCR < 0) {
                  return getNodeIndex(data, pos, i, meta) - 1;
                }
                if (qualifierLength > 0) {
                  qualifierPos += (qualifierLength + Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE);
                }
              }
              return getNodeIndex(data, pos, numOccurrences - 1, meta);
            }
          }
        } else {
          // > current node.
          return getNodeIndex(data, pos, numOccurrences - 1, meta);
        }
      }
      if (skey.getRowLength() > keyStartPos + nodeDataLength) {
        int fanOffset = bbStartPos + meta.nodeDataLengthWidth + nodeDataLength + meta.fanOutWidth;
        byte searchForByte = getCellByte(skey, keyStartPos + nodeDataLength);

        int fanIndexInBlock =
          unsignedBinarySearch(data, fanOffset, fanOffset + fanOut, searchForByte);
        int nodeOffsetStartPos = fanOffset + fanOut;
        if (fanIndexInBlock >= 0) {
          // found it, but need to adjust for position of fan in overall block
          int fanIndex = fanIndexInBlock - fanOffset;
          int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, fanIndex, meta);
          return search(data, bbStartPos + nodeOffset, skey, keyStartPos + nodeDataLength + 1,
            meta);
        } else {
          int fanIndex = fanIndexInBlock + fanOffset;// didn't find it, so compensate in reverse
          int insertionPoint = (-fanIndex - 1) - 1;
          if (insertionPoint < 0) {
            // < first children
            int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, 0, meta);
            return getFirstLeafNode(data, bbStartPos + nodeOffset, meta) - 1;
          } else {
            int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, insertionPoint, meta);
            return getLastLeafNode(data, bbStartPos + nodeOffset, meta);
          }
        }
      } else {
        // skey.length == keyStartPos + nodeDataLength
        if (numOccurrences > 0) {
          // == current node and current node is a leaf node.
          if (hasQualifier == 0) {
            // == current node
            return getNodeIndex(data, pos, 0, meta);
          } else {
            // need compare qualifier
            int qualifierPos = pos + numOccurrences * meta.indexWidth;
            if (skey.getQualifierLength() == 0) {
              int firstQualifierLength = getQualifierLength(data, qualifierPos, meta);
              if (firstQualifierLength == 0) {
                return getNodeIndex(data, pos, 0, meta);
              } else {
                // search key has no qualifier, but first index node has.
                return getNodeIndex(data, pos, 0, meta) - 1;
              }
            } else {
              for (int i = 0; i < numOccurrences; i++) {
                int qualifierLength = getQualifierLength(data, qualifierPos, meta);
                qualifierPos += meta.qualifierLengthWidth;
                int qualifierCR = compareQualifierTo(skey, data, qualifierPos, qualifierLength);
                if (qualifierCR == 0) {
                  // the same qualifier.
                  int timestampPos = qualifierPos + qualifierLength;
                  long timestamp = ByteBufferUtils.toLong(data, timestampPos);
                  byte byteType = ByteBufferUtils.toByte(data, timestampPos + Bytes.SIZEOF_LONG);
                  // higher numbers sort before those of lesser numbers.
                  if (skey.getTimestamp() > timestamp) {
                    return getNodeIndex(data, pos, i, meta) - 1;
                  } else if (skey.getTimestamp() < timestamp) {
                    return getNodeIndex(data, pos, i, meta);
                  }
                  // higher numbers sort before those of lesser numbers.
                  if ((0xff & skey.getTypeByte() - (0xff & byteType)) > 0) {
                    return getNodeIndex(data, pos, i, meta) - 1;
                  } else {
                    return getNodeIndex(data, pos, i, meta);
                  }
                } else if (qualifierCR < 0) {
                  return getNodeIndex(data, pos, i, meta) - 1;
                }
                if (qualifierLength > 0) {
                  qualifierPos += (qualifierLength + Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE);
                }
              }
              return getNodeIndex(data, pos, numOccurrences - 1, meta);
            }
          }
        } else {
          // need -1, == current node and current node not a leaf node.
          return getFirstLeafNode(data, bbStartPos, meta) - 1;
        }
      }
    } else if (cs > 0) {
      // search key bigger than (>) current node, get biggest
      if (fanOut == 0) {
        if (numOccurrences > 0) {
          return getNodeIndex(data, pos, numOccurrences - 1, meta);
        } else {
          throw new IllegalStateException(
            "numOccurrences = " + numOccurrences + ", fanOut = " + fanOut + " not expected.");
        }
      } else {
        return getLastLeafNode(data, bbStartPos, meta);
      }
    } else {
      // search key small than (<) current node, get smallest.
      if (numOccurrences > 0) {
        return getNodeIndex(data, pos, 0, meta) - 1;
      } else {
        return getFirstLeafNode(data, bbStartPos, meta) - 1;
      }
    }
  }

  static int compareTo(Cell skey, int o1, int l1, ByteBuffer data, int o2, int l2) {
    if (skey instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell byteBufferExtendedCell = ((ByteBufferExtendedCell) skey);
      return ByteBufferUtils.compareTo(byteBufferExtendedCell.getRowByteBuffer(),
        byteBufferExtendedCell.getRowPosition() + o1, l1, data, o2, l2);
    }
    return ByteBufferUtils.compareTo(skey.getRowArray(), skey.getRowOffset() + o1, l1, data, o2,
      l2);
  }

  static int compareQualifierTo(Cell skey, ByteBuffer data, int o2, int l2) {
    if (skey instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell byteBufferExtendedCell = ((ByteBufferExtendedCell) skey);
      return ByteBufferUtils.compareTo(byteBufferExtendedCell.getQualifierByteBuffer(),
        byteBufferExtendedCell.getQualifierPosition(), byteBufferExtendedCell.getQualifierLength(),
        data, o2, l2);
    }
    return ByteBufferUtils.compareTo(skey.getQualifierArray(), skey.getQualifierOffset(),
      skey.getQualifierLength(), data, o2, l2);
  }

  static byte getCellByte(Cell skey, int position) {
    if (skey instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell byteBufferExtendedCell = ((ByteBufferExtendedCell) skey);
      return byteBufferExtendedCell.getRowByteBuffer()
        .get(byteBufferExtendedCell.getRowPosition() + position);
    }
    return skey.getRowArray()[skey.getRowOffset() + position];
  }

  static int getNodeDataLength(ByteBuffer data, int offset, PrefixTreeDataWidth meta) {
    int dataLength = (int) UFIntTool.fromBytes(data, offset, meta.nodeDataLengthWidth);
    return dataLength;
  }

  static int getNodeFanOut(ByteBuffer data, int offset, PrefixTreeDataWidth meta) {
    int fanOut = (int) UFIntTool.fromBytes(data, offset, meta.fanOutWidth);
    return fanOut;
  }

  static int getNodeNumOccurrences(ByteBuffer data, int offset, PrefixTreeDataWidth meta) {
    int numOccurrences = (int) UFIntTool.fromBytes(data, offset, meta.occurrencesWidth);
    return numOccurrences;
  }

  static int getNodeOffset(ByteBuffer data, int offset, int index, PrefixTreeDataWidth meta) {
    int nodeOffset = (int) UFIntTool.fromBytes(data, offset + (index * meta.childNodeOffsetWidth),
      meta.childNodeOffsetWidth);
    return nodeOffset;
  }

  static int getNodeIndex(ByteBuffer data, int offset, int index, PrefixTreeDataWidth meta) {
    int nodeIndex =
      (int) UFIntTool.fromBytes(data, offset + (index * meta.indexWidth), meta.indexWidth);
    return nodeIndex;
  }

  static int getQualifierLength(ByteBuffer data, int offset, PrefixTreeDataWidth meta) {
    int nodeIndex = (int) UFIntTool.fromBytes(data, offset, meta.qualifierLengthWidth);
    return nodeIndex;
  }

  /**
   * Get the node's first leaf node
   */
  static int getFirstLeafNode(ByteBuffer data, int nodeStartPos, PrefixTreeDataWidth meta) {
    int dataLength = getNodeDataLength(data, nodeStartPos, meta);
    int pos = nodeStartPos + meta.nodeDataLengthWidth + dataLength;
    int fanOut = getNodeFanOut(data, pos, meta);
    pos += meta.fanOutWidth + fanOut + fanOut * meta.childNodeOffsetWidth;
    int tmpNumOccurrences = getNodeNumOccurrences(data, pos, meta);
    int numOccurrences = tmpNumOccurrences >> 1;
    pos += meta.occurrencesWidth;
    if (numOccurrences > 0 || fanOut == 0) {
      // return current node.
      return getNodeIndex(data, pos, 0, meta);
    } else {
      int nodeOffsetStartPos =
        nodeStartPos + meta.nodeDataLengthWidth + dataLength + meta.fanOutWidth + fanOut;
      int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, 0, meta);
      return getFirstLeafNode(data, nodeStartPos + nodeOffset, meta);
    }
  }

  /**
   * Get the node's last leaf node
   */
  static int getLastLeafNode(ByteBuffer data, int bbStartPos, PrefixTreeDataWidth meta) {
    int dataLength = getNodeDataLength(data, bbStartPos, meta);
    int pos = bbStartPos + meta.nodeDataLengthWidth + dataLength;
    int fanOut = getNodeFanOut(data, pos, meta);
    pos += meta.fanOutWidth + fanOut + fanOut * meta.childNodeOffsetWidth;
    int tmpNumOccurrences = getNodeNumOccurrences(data, pos, meta);
    int numOccurrences = tmpNumOccurrences >> 1;
    pos += meta.occurrencesWidth;
    if (fanOut == 0) {
      return getNodeIndex(data, pos, numOccurrences - 1, meta);
    } else {
      int nodeOffsetStartPos =
        bbStartPos + meta.nodeDataLengthWidth + dataLength + meta.fanOutWidth + fanOut;
      int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, fanOut - 1, meta);
      return getLastLeafNode(data, bbStartPos + nodeOffset, meta);
    }
  }

  public static int unsignedBinarySearch(ByteBuffer a, int fromIndex, int toIndex, byte key) {
    int unsignedKey = key & 0xff;
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = low + ((high - low) >> 1);
      int midVal = a.get(mid) & 0xff;

      if (midVal < unsignedKey) {
        low = mid + 1;
      } else if (midVal > unsignedKey) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }

  public static byte[] get(ByteBuffer data, int bbStartPos, PrefixTreeDataWidth dataWidth,
    int index) {
    return get(data, bbStartPos, dataWidth, index, new byte[0]);
  }

  static byte[] get(ByteBuffer data, int bbStartPos, PrefixTreeDataWidth meta, int index,
    byte[] prefix) {
    int dataLength = getNodeDataLength(data, bbStartPos, meta);
    byte[] bdata = new byte[dataLength];
    ByteBuffer dup = data.duplicate();
    dup.position(bbStartPos + meta.nodeDataLengthWidth);
    dup.get(bdata, 0, dataLength);
    bdata = Bytes.add(prefix, bdata);

    int pos = bbStartPos + meta.nodeDataLengthWidth + dataLength;
    int fanOut = getNodeFanOut(data, pos, meta);
    pos += meta.fanOutWidth + fanOut + fanOut * meta.childNodeOffsetWidth;
    int tmpNumOccurrences = getNodeNumOccurrences(data, pos, meta);
    int numOccurrences = tmpNumOccurrences >> 1;
    // int hasQualifier = tmpNumOccurrences &= 0x01;
    pos += meta.occurrencesWidth;
    if (numOccurrences > 0) {
      for (int i = 0; i < numOccurrences; i++) {
        int currentNodeIndex = getNodeIndex(data, pos, i, meta);
        if (currentNodeIndex == index) {
          return bdata;
        }
      }
    }
    if (fanOut == 0) {
      for (int i = 0; i < numOccurrences; i++) {
        int currentNodeIndex = getNodeIndex(data, pos, i, meta);
        if (currentNodeIndex == index) {
          return bdata;
        }
      }
      throw new IllegalStateException("Unexpected, not find index=" + index + " node's data.");
    } else {
      int nodeOffsetStartPos =
        bbStartPos + meta.nodeDataLengthWidth + dataLength + meta.fanOutWidth + fanOut;
      int locateIndex = locateWhichChild(data, bbStartPos, meta, index, fanOut, nodeOffsetStartPos);
      int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, locateIndex, meta);
      byte[] childBytes = new byte[1];
      childBytes[0] = data
        .get(bbStartPos + meta.nodeDataLengthWidth + dataLength + meta.fanOutWidth + locateIndex);
      bdata = Bytes.add(bdata, childBytes);
      return get(data, bbStartPos + nodeOffset, meta, index, bdata);
    }
  }

  static int locateWhichChild(ByteBuffer data, int bbStartPos, PrefixTreeDataWidth meta, int index,
    int fanOut, int nodeOffsetStartPos) {
    for (int i = 0; i < fanOut; i++) {
      int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, i, meta);
      int lastLeafNode = getLastLeafNode(data, bbStartPos + nodeOffset, meta);
      if (lastLeafNode >= index) {
        return i;
      }
    }
    throw new IllegalStateException("Unexpected unable to find index=" + index);
  }

}
