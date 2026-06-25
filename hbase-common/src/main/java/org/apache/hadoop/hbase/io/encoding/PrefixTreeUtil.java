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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.util.UFIntTool;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class PrefixTreeUtil {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixTreeUtil.class);

  /**
   * Build tree from begin
   * @return the tree
   */
  public static TokenizerNode buildPrefixTree(List<byte[]> rowKeys) {
    // root node.
    TokenizerNode node = new TokenizerNode();
    int start = 0;
    // Get max common prefix
    int common = maxCommonPrefix(rowKeys, 0, rowKeys.size() - 1, 0);
    if (common > 0) {
      byte[] commonB = Bytes.copy(rowKeys.get(0), 0, common);
      node.nodeData = commonB;
      for (int i = 0; i < rowKeys.size(); i++) {
        if (rowKeys.get(i).length == common) {
          node.numOccurrences++;
          if (node.index == null) {
            node.index = new ArrayList<>(1);
          }
          node.index.add(i);
          start = i + 1;
        } else {
          break;
        }
      }
    } else {
      // Only root node data can be empty.
      node.nodeData = new byte[0];
    }
    constructAndSplitChild(node, rowKeys, start, rowKeys.size() - 1, common);
    return node;
  }

  /**
   * Calculate max common prefix
   * @return the max common prefix num bytes
   */
  static int maxCommonPrefix(List<byte[]> rowKeys, int start, int end, int startPos) {
    // only one entry.
    if (start == end) {
      return rowKeys.get(start).length - startPos;
    }
    int common = 0;
    for (int round = 0; round <= rowKeys.get(start).length - startPos - 1; round++) {
      boolean same = true;
      for (int i = start + 1; i <= end; i++) {
        if (startPos + common > rowKeys.get(i).length - 1) {
          same = false;
          break;
        }
        if (rowKeys.get(start)[startPos + common] != rowKeys.get(i)[startPos + common]) {
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
  static void constructAndSplitChild(TokenizerNode node, List<byte[]> rowKeys, int start, int end,
    int startPos) {
    int middle = start;
    for (int i = start + 1; i <= end; i++) {
      if (startPos > rowKeys.get(i).length - 1) {
        middle = i - 1;
        break;
      }
      if (rowKeys.get(start)[startPos] != rowKeys.get(i)[startPos]) {
        middle = i - 1;
        break;
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
  static TokenizerNode constructCommonNodeAndChild(TokenizerNode node, List<byte[]> rowKeys,
    int start, int end, int startPos) {
    int common = maxCommonPrefix(rowKeys, start, end, startPos);
    if (common > 0) {
      TokenizerNode child = new TokenizerNode();
      child.parent = node;
      node.children.add(child);
      byte[] commonB = Bytes.copy(rowKeys.get(start), startPos, common);
      child.nodeData = commonB;
      int newStart = start;
      for (int i = start; i <= end; i++) {
        if (rowKeys.get(i).length == (startPos + common)) {
          child.numOccurrences++;
          if (child.index == null) {
            child.index = new ArrayList<>(1);
          }
          child.index.add(i);
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

  public static void getNodeMetaInfo(TokenizerNode node, TokenizerNodeMeta meta) {
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
    meta.countNumOccurrences++;
    if (node.index != null) {
      for (Integer entry : node.index) {
        if (entry > meta.maxIndex) {
          meta.maxIndex = entry;
        }
      }
    }
    if (node.children.isEmpty()) {
      meta.leafNodes.add(node);
      meta.totalIndexNum++;
    } else {
      meta.nonLeafNodes.add(node);
    }
    for (TokenizerNode child : node.children) {
      getNodeMetaInfo(child, meta);
    }
  }

  public static void serializePrefixTree(TokenizerNode node, PrefixTreeDataWidth dataWidth,
    ByteArrayOutputStream outputStream) throws IOException {
    TokenizerNodeMeta meta = new TokenizerNodeMeta();
    PrefixTreeUtil.getNodeMetaInfo(node, meta);
    int totalLength = 0;
    dataWidth.nodeDataLengthWidth = UFIntTool.numBytes(meta.maxNodeDataLength);
    totalLength += meta.totalNodeDataLength;
    totalLength += dataWidth.nodeDataLengthWidth * meta.countNodeDataNum;

    dataWidth.fanOutWidth = UFIntTool.numBytes(meta.maxFanOut);
    // fan Out
    totalLength += dataWidth.fanOutWidth * meta.countChildNum;
    // fan Byte
    totalLength += meta.totalChildNum;

    // nextnodeoffset
    totalLength += 4 * meta.countChildNum;

    dataWidth.occurrencesWidth = UFIntTool.numBytes(meta.maxNumOccurrences);
    totalLength += dataWidth.occurrencesWidth * meta.countNumOccurrences;

    dataWidth.indexWidth = UFIntTool.numBytes(meta.maxIndex);
    totalLength += dataWidth.indexWidth * meta.totalIndexNum;

    dataWidth.childNodeOffsetWidth = UFIntTool.numBytes(totalLength);

    // track the starting position of each node in final output
    int negativeIndex = 0;
    for (int i = meta.leafNodes.size() - 1; i >= 0; i--) {
      TokenizerNode leaf = meta.leafNodes.get(i);
      // no children
      int leafNodeWidth =
        dataWidth.nodeDataLengthWidth + leaf.nodeData.length + dataWidth.fanOutWidth
          + dataWidth.occurrencesWidth + leaf.numOccurrences * dataWidth.indexWidth;
      negativeIndex += leafNodeWidth;
      leaf.nodeWidth = leafNodeWidth;
      leaf.negativeIndex = negativeIndex;
    }
    for (int i = meta.nonLeafNodes.size() - 1; i >= 0; i--) {
      TokenizerNode nonLeaf = meta.nonLeafNodes.get(i);
      int leafNodeWidth =
        dataWidth.nodeDataLengthWidth + nonLeaf.nodeData.length + dataWidth.fanOutWidth
          + nonLeaf.children.size() + nonLeaf.children.size() * dataWidth.childNodeOffsetWidth
          + dataWidth.occurrencesWidth + nonLeaf.numOccurrences * dataWidth.indexWidth;
      negativeIndex += leafNodeWidth;
      nonLeaf.nodeWidth = leafNodeWidth;
      nonLeaf.negativeIndex = negativeIndex;
    }

    for (int i = 0; i < meta.nonLeafNodes.size(); i++) {
      serialize(meta.nonLeafNodes.get(i), outputStream, dataWidth);
    }
    for (int i = 0; i < meta.leafNodes.size(); i++) {
      serialize(meta.leafNodes.get(i), outputStream, dataWidth);
    }
  }

  static void serialize(TokenizerNode node, ByteArrayOutputStream os, PrefixTreeDataWidth dataWidth)
    throws IOException {
    UFIntTool.writeBytes(dataWidth.nodeDataLengthWidth, node.nodeData.length, os);
    os.write(node.nodeData, 0, node.nodeData.length);
    UFIntTool.writeBytes(dataWidth.fanOutWidth, node.children.size(), os);
    for (TokenizerNode child : node.children) {
      // child's first byte.
      os.write(child.nodeData[0]);
    }
    for (TokenizerNode child : node.children) {
      UFIntTool.writeBytes(dataWidth.childNodeOffsetWidth, node.negativeIndex - child.negativeIndex,
        os);
    }
    UFIntTool.writeBytes(dataWidth.occurrencesWidth, node.numOccurrences, os);
    for (int i = 0; i < node.numOccurrences; i++) {
      UFIntTool.writeBytes(dataWidth.indexWidth, node.index.get(i), os);
    }
  }

  public static void serialize(DataOutput out, PrefixTreeDataWidth dataWidth) throws IOException {
    out.writeByte(dataWidth.nodeDataLengthWidth);
    out.writeByte(dataWidth.fanOutWidth);
    out.writeByte(dataWidth.occurrencesWidth);
    out.writeByte(dataWidth.indexWidth);
    out.writeByte(dataWidth.childNodeOffsetWidth);
  }

  public static void deserialize(ByteBuff data, PrefixTreeDataWidth dataWidth) {
    dataWidth.nodeDataLengthWidth = data.get();
    dataWidth.fanOutWidth = data.get();
    dataWidth.occurrencesWidth = data.get();
    dataWidth.indexWidth = data.get();
    dataWidth.childNodeOffsetWidth = data.get();
  }

  /**
   * Get the node index, that search key >= index and search key < (index + 1)
   */
  public static int search(ByteBuffer data, int bbStartPos, byte[] skey, int keyStartPos,
    PrefixTreeDataWidth meta) {
    int nodeDataLength = getNodeDataLength(data, bbStartPos, meta);
    int cs = ByteBufferUtils.compareTo(skey, keyStartPos,
      Math.min(skey.length - keyStartPos, nodeDataLength), data,
      bbStartPos + meta.nodeDataLengthWidth, nodeDataLength);

    int pos = bbStartPos + meta.nodeDataLengthWidth + nodeDataLength;
    int fanOut = getNodeFanOut(data, pos, meta);
    pos += meta.fanOutWidth + fanOut + fanOut * meta.childNodeOffsetWidth;
    int numOccurrences = getNodeNumOccurrences(data, pos, meta);
    pos += meta.occurrencesWidth;

    if (cs == 0) {
      // continue search
      if (fanOut == 0) {
        // no children, should be numOccurrences > 0
        int index = getNodeIndex(data, pos, 0, meta);
        if (skey.length == keyStartPos + nodeDataLength) {
          // == current node
          return index;
        } else {
          // > current node.
          return index;
        }
      }
      if (skey.length > keyStartPos + nodeDataLength) {
        int fanOffset = bbStartPos + meta.nodeDataLengthWidth + nodeDataLength + meta.fanOutWidth;
        byte searchForByte = skey[keyStartPos + nodeDataLength];

        int fanIndexInBlock =
          unsignedBinarySearch(data, fanOffset, fanOffset + fanOut, searchForByte);
        int nodeOffsetStartPos = fanOffset + fanOut;
        if (fanIndexInBlock >= 0) {
          // found it, but need to adjust for position of fan in overall block
          int fanIndex = fanIndexInBlock - fanOffset;
          int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, fanIndex, meta);
          return search(data, bbStartPos + nodeOffset, skey, keyStartPos + nodeDataLength, meta);
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
          return getNodeIndex(data, pos, 0, meta);
        } else {
          // need -1, == current node and current node not a leaf node.
          return getFirstLeafNode(data, bbStartPos, meta) - 1;
        }
      }
    } else if (cs > 0) {
      // search key bigger than (>) current node, get biggest
      if (fanOut == 0) {
        if (numOccurrences > 0) {
          if (numOccurrences == 1) {
            return getNodeIndex(data, pos, 0, meta);
          } else {
            // TODO
            throw new IllegalStateException(
              "numOccurrences = " + numOccurrences + " > 1 not expected.");
          }
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

  /**
   * Get the node's first leaf node
   */
  static int getFirstLeafNode(ByteBuffer data, int bbStartPos, PrefixTreeDataWidth meta) {
    int dataLength = getNodeDataLength(data, bbStartPos, meta);
    int pos = bbStartPos + meta.nodeDataLengthWidth + dataLength;
    int fanOut = getNodeFanOut(data, pos, meta);
    pos += meta.fanOutWidth + fanOut + fanOut * meta.childNodeOffsetWidth;
    int numOccurrences = getNodeNumOccurrences(data, pos, meta);
    pos += meta.occurrencesWidth;
    if (numOccurrences > 0 || fanOut == 0) {
      // return current node.
      return getNodeIndex(data, pos, 0, meta);
    } else {
      int nodeOffsetStartPos =
        bbStartPos + meta.nodeDataLengthWidth + dataLength + meta.fanOutWidth + fanOut;
      int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, 0, meta);
      return getFirstLeafNode(data, bbStartPos + nodeOffset, meta);
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
    // int numOccurrences = getNodeNumOccurrences(data, pos, meta);
    pos += meta.occurrencesWidth;
    if (fanOut == 0) {
      return getNodeIndex(data, pos, 0, meta);
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
    int numOccurrences = getNodeNumOccurrences(data, pos, meta);
    pos += meta.occurrencesWidth;
    if (numOccurrences > 0) {
      int currentNodeIndex = getNodeIndex(data, pos, 0, meta);
      if (currentNodeIndex == index) {
        return bdata;
      }
    }
    if (fanOut == 0) {
      int currentNodeIndex = getNodeIndex(data, pos, 0, meta);
      if (currentNodeIndex == index) {
        return bdata;
      } else {
        throw new IllegalStateException(
          "Unexpected, search index=" + index + ", but find to " + currentNodeIndex);
      }
    } else {
      int nodeOffsetStartPos =
        bbStartPos + meta.nodeDataLengthWidth + dataLength + meta.fanOutWidth + fanOut;
      int locateIndex = locateWhichChild(data, bbStartPos, meta, index, fanOut, nodeOffsetStartPos);
      int nodeOffset = getNodeOffset(data, nodeOffsetStartPos, locateIndex, meta);
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

  /**
   * Used only when serialize for build the prefix tree.
   */
  public static class TokenizerNode {

    public byte[] nodeData = null;

    /**
     * ref to parent trie node
     */
    public TokenizerNode parent = null;

    /**
     * child nodes.
     */
    public ArrayList<TokenizerNode> children = new ArrayList<>();

    /*
     * A count of occurrences in the input byte[]s, not the trie structure. 0 for branch nodes, 1+
     * for nubs and leaves.
     */
    public int numOccurrences = 0;

    public List<Integer> index = null;

    public List<KeyValue.KeyOnlyKeyValue> keys = null;

    public int qualifierLength = 0;
    public int qualifierNum = 0;

    /*
     * A positive value indicating how many bytes before the end of the block this node will start.
     * If the section is 55 bytes and negativeOffset is 9, then the node will start at 46.
     */
    public int negativeIndex = 0;

    public int nodeWidth = 0;
  }

  public static class TokenizerNodeMeta {

    public int maxNodeDataLength = 0;
    public int totalNodeDataLength = 0;
    public int countNodeDataNum = 0;

    public int maxFanOut = 0;
    public int totalChildNum = 0;
    public int countChildNum = 0;

    public int maxNumOccurrences = 0;
    public int countNumOccurrences = 0;

    public int maxIndex = 0;
    public int totalIndexNum = 0;

    public int maxQualifierLength = 0;
    public int countQualifierNum = 0;
    public int totalQualifierLength = 0;

    public ArrayList<TokenizerNode> nonLeafNodes = new ArrayList<>();

    public ArrayList<TokenizerNode> leafNodes = new ArrayList<>();
  }

  public static class PrefixTreeDataWidth {
    public int nodeDataLengthWidth = 0;

    public int fanOutWidth = 0;

    public int occurrencesWidth = 0;

    public int indexWidth = 0;

    public int childNodeOffsetWidth = 0;

    public int qualifierLengthWidth = 0;
  }
}
