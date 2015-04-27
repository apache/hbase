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

package org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import com.google.common.collect.Lists;

/**
 * Data structure used in the first stage of PrefixTree encoding:
 * <ul>
 * <li>accepts a sorted stream of ByteRanges
 * <li>splits them into a set of tokens, each held by a {@link TokenizerNode}
 * <li>connects the TokenizerNodes via standard java references
 * <li>keeps a pool of TokenizerNodes and a reusable byte[] for holding all token content
 * </ul>
 * <p><br>
 * Mainly used for turning Cell rowKeys into a trie, but also used for family and qualifier
 * encoding.
 */
@InterfaceAudience.Private
public class Tokenizer{

  /***************** fields **************************/

  protected int numArraysAdded = 0;
  protected long lastNodeId = -1;
  protected ArrayList<TokenizerNode> nodes;
  protected int numNodes;
  protected TokenizerNode root;
  protected byte[] tokens;
  protected int tokensLength;

  protected int maxElementLength = 0;
  // number of levels in the tree assuming root level is 0
  protected int treeDepth = 0;


  /******************* construct *******************/

  public Tokenizer() {
    this.nodes = Lists.newArrayList();
    this.tokens = new byte[0];
  }

  public void reset() {
    numArraysAdded = 0;
    lastNodeId = -1;
    numNodes = 0;
    tokensLength = 0;
    root = null;
    maxElementLength = 0;
    treeDepth = 0;
  }


  /***************** building *************************/

  public void addAll(ArrayList<ByteRange> sortedByteRanges) {
    for (int i = 0; i < sortedByteRanges.size(); ++i) {
      ByteRange byteRange = sortedByteRanges.get(i);
      addSorted(byteRange);
    }
  }

  public void addSorted(final ByteRange bytes) {
    ++numArraysAdded;
    if (bytes.getLength() > maxElementLength) {
      maxElementLength = bytes.getLength();
    }
    if (root == null) {
      // nodeDepth of firstNode (non-root) is 1
      root = addNode(null, 1, 0, bytes, 0);
    } else {
      root.addSorted(bytes);
    }
  }

  public void incrementNumOccurrencesOfLatestValue(){
    CollectionUtils.getLast(nodes).incrementNumOccurrences(1);
  }

  protected long nextNodeId() {
    return ++lastNodeId;
  }

  protected TokenizerNode addNode(TokenizerNode parent, int nodeDepth, int tokenStartOffset,
      final ByteRange token, int inputTokenOffset) {
    int inputTokenLength = token.getLength() - inputTokenOffset;
    int tokenOffset = appendTokenAndRepointByteRange(token, inputTokenOffset);
    TokenizerNode node = null;
    if (nodes.size() <= numNodes) {
      node = new TokenizerNode(this, parent, nodeDepth, tokenStartOffset, tokenOffset,
          inputTokenLength);
      nodes.add(node);
    } else {
      node = nodes.get(numNodes);
      node.reset();
      node.reconstruct(this, parent, nodeDepth, tokenStartOffset, tokenOffset, inputTokenLength);
    }
    ++numNodes;
    return node;
  }

  protected int appendTokenAndRepointByteRange(final ByteRange token, int inputTokenOffset) {
    int newOffset = tokensLength;
    int inputTokenLength = token.getLength() - inputTokenOffset;
    int newMinimum = tokensLength + inputTokenLength;
    tokens = ArrayUtils.growIfNecessary(tokens, newMinimum, 2 * newMinimum);
    token.deepCopySubRangeTo(inputTokenOffset, inputTokenLength, tokens, tokensLength);
    tokensLength += inputTokenLength;
    return newOffset;
  }

  protected void submitMaxNodeDepthCandidate(int nodeDepth) {
    if (nodeDepth > treeDepth) {
      treeDepth = nodeDepth;
    }
  }


  /********************* read ********************/

  public int getNumAdded(){
    return numArraysAdded;
  }

  // for debugging
  public ArrayList<TokenizerNode> getNodes(boolean includeNonLeaves, boolean includeLeaves) {
    ArrayList<TokenizerNode> nodes = Lists.newArrayList();
    root.appendNodesToExternalList(nodes, includeNonLeaves, includeLeaves);
    return nodes;
  }

  public void appendNodes(List<TokenizerNode> appendTo, boolean includeNonLeaves,
      boolean includeLeaves) {
    root.appendNodesToExternalList(appendTo, includeNonLeaves, includeLeaves);
  }

  public List<byte[]> getArrays() {
    List<TokenizerNode> nodes = new ArrayList<TokenizerNode>();
    root.appendNodesToExternalList(nodes, true, true);
    List<byte[]> byteArrays = Lists.newArrayListWithCapacity(CollectionUtils.nullSafeSize(nodes));
    for (int i = 0; i < nodes.size(); ++i) {
      TokenizerNode node = nodes.get(i);
      for (int j = 0; j < node.getNumOccurrences(); ++j) {
        byte[] byteArray = node.getNewByteArray();
        byteArrays.add(byteArray);
      }
    }
    return byteArrays;
  }

  //currently unused, but working and possibly useful in the future
  public void getNode(TokenizerRowSearchResult resultHolder, byte[] key, int keyOffset,
      int keyLength) {
    root.getNode(resultHolder, key, keyOffset, keyLength);
  }


  /********************** write ***************************/

  public Tokenizer setNodeFirstInsertionIndexes() {
    root.setInsertionIndexes(0);
    return this;
  }

  public Tokenizer appendOutputArrayOffsets(List<Integer> offsets) {
    root.appendOutputArrayOffsets(offsets);
    return this;
  }


  /********************* print/debug ********************/

  protected static final Boolean INCLUDE_FULL_TREE_IN_TO_STRING = false;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getStructuralString());
    if (INCLUDE_FULL_TREE_IN_TO_STRING) {
      for (byte[] bytes : getArrays()) {
        if (sb.length() > 0) {
          sb.append("\n");
        }
        sb.append(Bytes.toString(bytes));
      }
    }
    return sb.toString();
  }

  public String getStructuralString() {
    List<TokenizerNode> nodes = getNodes(true, true);
    StringBuilder sb = new StringBuilder();
    for (TokenizerNode node : nodes) {
      String line = node.getPaddedTokenAndOccurrenceString();
      sb.append(line + "\n");
    }
    return sb.toString();
  }


  /****************** get/set ************************/

  public TokenizerNode getRoot() {
    return root;
  }

  public int getMaxElementLength() {
    return maxElementLength;
  }

  public int getTreeDepth() {
    return treeDepth;
  }

}
