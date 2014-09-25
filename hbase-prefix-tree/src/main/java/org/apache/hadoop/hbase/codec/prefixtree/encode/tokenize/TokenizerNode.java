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
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ByteRangeUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.apache.hadoop.hbase.util.Strings;

import com.google.common.collect.Lists;

/**
 * Individual node in a Trie structure.  Each node is one of 3 types:
 * <li>Branch: an internal trie node that may have a token and must have multiple children, but does
 * not represent an actual input byte[], hence its numOccurrences is 0
 * <li>Leaf: a node with no children and where numOccurrences is >= 1.  It's token represents the
 * last bytes in the input byte[]s.
 * <li>Nub: a combination of a branch and leaf.  Its token represents the last bytes of input
 * byte[]s and has numOccurrences >= 1, but it also has child nodes which represent input byte[]s
 * that add bytes to this nodes input byte[].
 * <br/><br/>
 * Example inputs (numInputs=7):
 * 0: AAA
 * 1: AAA
 * 2: AAB
 * 3: AAB
 * 4: AAB
 * 5: AABQQ
 * 6: AABQQ
 * <br/><br/>
 * Resulting TokenizerNodes:
 * AA <- branch, numOccurrences=0, tokenStartOffset=0, token.length=2
 * A  <- leaf, numOccurrences=2, tokenStartOffset=2, token.length=1
 * B  <- nub, numOccurrences=3, tokenStartOffset=2, token.length=1
 * QQ <- leaf, numOccurrences=2, tokenStartOffset=3, token.length=2
 * <br/><br/>
 * numInputs == 7 == sum(numOccurrences) == 0 + 2 + 3 + 2
 */
@InterfaceAudience.Private
public class TokenizerNode{

  /*
   * Ref to data structure wrapper
   */
  protected Tokenizer builder;

  /****************************************************************** 
   * Tree content/structure used during tokenization 
   * ****************************************************************/

  /*
   * ref to parent trie node
   */
  protected TokenizerNode parent;

  /*
   * node depth in trie, irrespective of each node's token length
   */
  protected int nodeDepth;

  /*
   * start index of this token in original byte[]
   */
  protected int tokenStartOffset;

  /*
   * bytes for this trie node.  can be length 0 in root node
   */
  protected ByteRange token;

  /*
   * A count of occurrences in the input byte[]s, not the trie structure. 0 for branch nodes, 1+ for
   * nubs and leaves. If the same byte[] is added to the trie multiple times, this is the only thing
   * that changes in the tokenizer. As a result, duplicate byte[]s are very inexpensive to encode.
   */
  protected int numOccurrences;

  /*
   * The maximum fan-out of a byte[] trie is 256, so there are a maximum of 256
   * child nodes.
   */
  protected ArrayList<TokenizerNode> children;


  /*
   * Fields used later in the encoding process for sorting the nodes into the order they'll be
   * written to the output byte[].  With these fields, the TokenizerNode and therefore Tokenizer
   * are not generic data structures but instead are specific to HBase PrefixTree encoding. 
   */

  /*
   * unique id assigned to each TokenizerNode
   */
  protected long id;

  /*
   * set >=0 for nubs and leaves
   */
  protected int firstInsertionIndex = -1;

  /*
   * A positive value indicating how many bytes before the end of the block this node will start. If
   * the section is 55 bytes and negativeOffset is 9, then the node will start at 46.
   */
  protected int negativeIndex = 0;

  /*
   * The offset in the output array at which to start writing this node's token bytes.  Influenced
   * by the lengths of all tokens sorted before this one.
   */
  protected int outputArrayOffset = -1;


  /*********************** construct *****************************/

  public TokenizerNode(Tokenizer builder, TokenizerNode parent, int nodeDepth,
      int tokenStartOffset, int tokenOffset, int tokenLength) {
    this.token = new SimpleByteRange();
    reconstruct(builder, parent, nodeDepth, tokenStartOffset, tokenOffset, tokenLength);
    this.children = Lists.newArrayList();
  }

  /*
   * Sub-constructor for initializing all fields without allocating a new object.  Used by the
   * regular constructor.
   */
  public void reconstruct(Tokenizer builder, TokenizerNode parent, int nodeDepth,
      int tokenStartOffset, int tokenOffset, int tokenLength) {
    this.builder = builder;
    this.id = builder.nextNodeId();
    this.parent = parent;
    this.nodeDepth = nodeDepth;
    builder.submitMaxNodeDepthCandidate(nodeDepth);
    this.tokenStartOffset = tokenStartOffset;
    this.token.set(builder.tokens, tokenOffset, tokenLength);
    this.numOccurrences = 1;
  }

  /*
   * Clear the state of this node so that it looks like it was just allocated.
   */
  public void reset() {
    builder = null;
    parent = null;
    nodeDepth = 0;
    tokenStartOffset = 0;
    token.unset();
    numOccurrences = 0;
    children.clear();// branches & nubs

    // ids/offsets. used during writing to byte[]
    id = 0;
    firstInsertionIndex = -1;// set >=0 for nubs and leaves
    negativeIndex = 0;
    outputArrayOffset = -1;
  }


  /************************* building *********************************/

  /*
   * <li>Only public method used during the tokenization process
   * <li>Requires that the input ByteRange sort after the previous, and therefore after all previous
   * inputs
   * <li>Only looks at bytes of the input array that align with this node's token
   */
  public void addSorted(final ByteRange bytes) {// recursively build the tree

    /*
     * Recurse deeper into the existing trie structure
     */
    if (matchesToken(bytes) && CollectionUtils.notEmpty(children)) {
      TokenizerNode lastChild = CollectionUtils.getLast(children);
      if (lastChild.partiallyMatchesToken(bytes)) {
        lastChild.addSorted(bytes);
        return;
      }
    }

    /*
     * Recursion ended.  We must either
     * <li>1: increment numOccurrences if this input was equal to the previous
     * <li>2: convert this node from a leaf to a nub, and add a new child leaf
     * <li>3: split this node into a branch and leaf, and then add a second leaf
     */

    // add it as a child of this node
    int numIdenticalTokenBytes = numIdenticalBytes(bytes);// should be <= token.length
    int tailOffset = tokenStartOffset + numIdenticalTokenBytes;
    int tailLength = bytes.getLength() - tailOffset;

    if (numIdenticalTokenBytes == token.getLength()) {
      if (tailLength == 0) {// identical to this node (case 1)
        incrementNumOccurrences(1);
      } else {// identical to this node, but with a few extra tailing bytes. (leaf -> nub) (case 2)
        int childNodeDepth = nodeDepth + 1;
        int childTokenStartOffset = tokenStartOffset + numIdenticalTokenBytes;
        TokenizerNode newChildNode = builder.addNode(this, childNodeDepth, childTokenStartOffset,
          bytes, tailOffset);
        addChild(newChildNode);
      }
    } else {//numIdenticalBytes > 0, split into branch/leaf and then add second leaf (case 3)
      split(numIdenticalTokenBytes, bytes);
    }
  }


  protected void addChild(TokenizerNode node) {
    node.setParent(this);
    children.add(node);
  }


  /**
   * Called when we need to convert a leaf node into a branch with 2 leaves. Comments inside the
   * method assume we have token BAA starting at tokenStartOffset=0 and are adding BOO. The output
   * will be 3 nodes:<br/>
   * <li>1: B <- branch
   * <li>2: AA <- leaf
   * <li>3: OO <- leaf
   *
   * @param numTokenBytesToRetain => 1 (the B)
   * @param bytes => BOO
   */
  protected void split(int numTokenBytesToRetain, final ByteRange bytes) {
    int childNodeDepth = nodeDepth;
    int childTokenStartOffset = tokenStartOffset + numTokenBytesToRetain;

    //create leaf AA
    TokenizerNode firstChild = builder.addNode(this, childNodeDepth, childTokenStartOffset,
      token, numTokenBytesToRetain);
    firstChild.setNumOccurrences(numOccurrences);// do before clearing this node's numOccurrences
    token.setLength(numTokenBytesToRetain);//shorten current token from BAA to B
    numOccurrences = 0;//current node is now a branch

    moveChildrenToDifferentParent(firstChild);//point the new leaf (AA) to the new branch (B)
    addChild(firstChild);//add the new leaf (AA) to the branch's (B's) children

    //create leaf OO
    TokenizerNode secondChild = builder.addNode(this, childNodeDepth, childTokenStartOffset,
      bytes, tokenStartOffset + numTokenBytesToRetain);
    addChild(secondChild);//add the new leaf (00) to the branch's (B's) children

    // we inserted branch node B as a new level above/before the two children, so increment the
    // depths of the children below
    firstChild.incrementNodeDepthRecursively();
    secondChild.incrementNodeDepthRecursively();
  }


  protected void incrementNodeDepthRecursively() {
    ++nodeDepth;
    builder.submitMaxNodeDepthCandidate(nodeDepth);
    for (int i = 0; i < children.size(); ++i) {
      children.get(i).incrementNodeDepthRecursively();
    }
  }


  protected void moveChildrenToDifferentParent(TokenizerNode newParent) {
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      child.setParent(newParent);
      newParent.children.add(child);
    }
    children.clear();
  }


	/************************ byte[] utils *************************/

  protected boolean partiallyMatchesToken(ByteRange bytes) {
    return numIdenticalBytes(bytes) > 0;
  }

  protected boolean matchesToken(ByteRange bytes) {
    return numIdenticalBytes(bytes) == getTokenLength();
  }

  protected int numIdenticalBytes(ByteRange bytes) {
    return ByteRangeUtils.numEqualPrefixBytes(token, bytes, tokenStartOffset);
  }


	/***************** moving nodes around ************************/

  public void appendNodesToExternalList(List<TokenizerNode> appendTo, boolean includeNonLeaves,
      boolean includeLeaves) {
    if (includeNonLeaves && !isLeaf() || includeLeaves && isLeaf()) {
      appendTo.add(this);
    }
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      child.appendNodesToExternalList(appendTo, includeNonLeaves, includeLeaves);
    }
  }

  public int setInsertionIndexes(int nextIndex) {
    int newNextIndex = nextIndex;
    if (hasOccurrences()) {
      setFirstInsertionIndex(nextIndex);
      newNextIndex += numOccurrences;
    }
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      newNextIndex = child.setInsertionIndexes(newNextIndex);
    }
    return newNextIndex;
  }

  public void appendOutputArrayOffsets(List<Integer> offsets) {
    if (hasOccurrences()) {
      offsets.add(outputArrayOffset);
    }
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      child.appendOutputArrayOffsets(offsets);
    }
  }


  /***************** searching *********************************/

  /*
   * Do a trie style search through the tokenizer.  One option for looking up families or qualifiers
   * during encoding, but currently unused in favor of tracking this information as they are added.
   *
   * Keeping code pending further performance testing.
   */
  public void getNode(TokenizerRowSearchResult resultHolder, byte[] key, int keyOffset,
      int keyLength) {
    int thisNodeDepthPlusLength = tokenStartOffset + token.getLength();

    // quick check if the key is shorter than this node (may not work for binary search)
    if (CollectionUtils.isEmpty(children)) {
      if (thisNodeDepthPlusLength < keyLength) {// ran out of bytes
        resultHolder.set(TokenizerRowSearchPosition.NO_MATCH, null);
        return;
      }
    }

    // all token bytes must match
    for (int i = 0; i < token.getLength(); ++i) {
      if (key[tokenStartOffset + keyOffset + i] != token.get(i)) {
        // TODO return whether it's before or after so we can binary search
        resultHolder.set(TokenizerRowSearchPosition.NO_MATCH, null);
        return;
      }
    }

    if (thisNodeDepthPlusLength == keyLength && numOccurrences > 0) {
      resultHolder.set(TokenizerRowSearchPosition.MATCH, this);// MATCH
      return;
    }

    if (CollectionUtils.notEmpty(children)) {
      // TODO binary search the children
      for (int i = 0; i < children.size(); ++i) {
        TokenizerNode child = children.get(i);
        child.getNode(resultHolder, key, keyOffset, keyLength);
        if (resultHolder.isMatch()) {
          return;
        } else if (resultHolder.getDifference() == TokenizerRowSearchPosition.BEFORE) {
          // passed it, so it doesn't exist
          resultHolder.set(TokenizerRowSearchPosition.NO_MATCH, null);
          return;
        }
        // key is still AFTER the current node, so continue searching
      }
    }

    // checked all children (or there were no children), and didn't find it
    resultHolder.set(TokenizerRowSearchPosition.NO_MATCH, null);
    return;
  }


  /****************** writing back to byte[]'s *************************/

  public byte[] getNewByteArray() {
    byte[] arrayToFill = new byte[tokenStartOffset + token.getLength()];
    fillInBytes(arrayToFill);
    return arrayToFill;
  }

  public void fillInBytes(byte[] arrayToFill) {
    for (int i = 0; i < token.getLength(); ++i) {
      arrayToFill[tokenStartOffset + i] = token.get(i);
    }
    if (parent != null) {
      parent.fillInBytes(arrayToFill);
    }
  }


  /************************** printing ***********************/

  @Override
  public String toString() {
    String s = "";
    if (parent == null) {
      s += "R ";
    } else {
      s += getBnlIndicator(false) + " " + Bytes.toString(parent.getNewByteArray());
    }
    s += "[" + Bytes.toString(token.deepCopyToNewArray()) + "]";
    if (numOccurrences > 0) {
      s += "x" + numOccurrences;
    }
    return s;
  }

  public String getPaddedTokenAndOccurrenceString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getBnlIndicator(true));
    sb.append(Strings.padFront(numOccurrences + "", ' ', 3));
    sb.append(Strings.padFront(nodeDepth + "", ' ', 3));
    if (outputArrayOffset >= 0) {
      sb.append(Strings.padFront(outputArrayOffset + "", ' ', 3));
    }
    sb.append("  ");
    for (int i = 0; i < tokenStartOffset; ++i) {
      sb.append(" ");
    }
    sb.append(Bytes.toString(token.deepCopyToNewArray()).replaceAll(" ", "_"));
    return sb.toString();
  }

  public String getBnlIndicator(boolean indent) {
    if (indent) {
      if (isNub()) {
        return " N ";
      }
      return isBranch() ? "B  " : "  L";
    }
    if (isNub()) {
      return "N";
    }
    return isBranch() ? "B" : "L";
  }


	/********************** count different node types ********************/

  public int getNumBranchNodesIncludingThisNode() {
    if (isLeaf()) {
      return 0;
    }
    int totalFromThisPlusChildren = isBranch() ? 1 : 0;
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      totalFromThisPlusChildren += child.getNumBranchNodesIncludingThisNode();
    }
    return totalFromThisPlusChildren;
  }

  public int getNumNubNodesIncludingThisNode() {
    if (isLeaf()) {
      return 0;
    }
    int totalFromThisPlusChildren = isNub() ? 1 : 0;
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      totalFromThisPlusChildren += child.getNumNubNodesIncludingThisNode();
    }
    return totalFromThisPlusChildren;
  }

  public int getNumLeafNodesIncludingThisNode() {
    if (isLeaf()) {
      return 1;
    }
    int totalFromChildren = 0;
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      totalFromChildren += child.getNumLeafNodesIncludingThisNode();
    }
    return totalFromChildren;
  }


  /*********************** simple read-only methods *******************************/

  public int getNodeDepth() {
    return nodeDepth;
  }

  public int getTokenLength() {
    return token.getLength();
  }

  public boolean hasOccurrences() {
    return numOccurrences > 0;
  }

  public boolean isRoot() {
    return this.parent == null;
  }

  public int getNumChildren() {
    return CollectionUtils.nullSafeSize(children);
  }

  public TokenizerNode getLastChild() {
    if (CollectionUtils.isEmpty(children)) {
      return null;
    }
    return CollectionUtils.getLast(children);
  }

  public boolean isLeaf() {
    return CollectionUtils.isEmpty(children) && hasOccurrences();
  }

  public boolean isBranch() {
    return CollectionUtils.notEmpty(children) && !hasOccurrences();
  }

  public boolean isNub() {
    return CollectionUtils.notEmpty(children) && hasOccurrences();
  }


  /********************** simple mutation methods *************************/

  /**
   * Each occurrence > 1 indicates a repeat of the previous entry.  This can be called directly by
   * an external class without going through the process of detecting a repeat if it is a known
   * repeat by some external mechanism.  PtEncoder uses this when adding cells to a row if it knows
   * the new cells are part of the current row.
   * @param d increment by this amount
   */
  public void incrementNumOccurrences(int d) {
    numOccurrences += d;
  }


  /************************* autogenerated get/set ******************/

  public int getTokenOffset() {
    return tokenStartOffset;
  }

  public TokenizerNode getParent() {
    return parent;
  }

  public ByteRange getToken() {
    return token;
  }

  public int getNumOccurrences() {
    return numOccurrences;
  }

  public void setParent(TokenizerNode parent) {
    this.parent = parent;
  }

  public void setNumOccurrences(int numOccurrences) {
    this.numOccurrences = numOccurrences;
  }

  public ArrayList<TokenizerNode> getChildren() {
    return children;
  }

  public long getId() {
    return id;
  }

  public int getFirstInsertionIndex() {
    return firstInsertionIndex;
  }

  public void setFirstInsertionIndex(int firstInsertionIndex) {
    this.firstInsertionIndex = firstInsertionIndex;
  }

  public int getNegativeIndex() {
    return negativeIndex;
  }

  public void setNegativeIndex(int negativeIndex) {
    this.negativeIndex = negativeIndex;
  }

  public int getOutputArrayOffset() {
    return outputArrayOffset;
  }

  public void setOutputArrayOffset(int outputArrayOffset) {
    this.outputArrayOffset = outputArrayOffset;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setBuilder(Tokenizer builder) {
    this.builder = builder;
  }

  public void setTokenOffset(int tokenOffset) {
    this.tokenStartOffset = tokenOffset;
  }

  public void setToken(ByteRange token) {
    this.token = token;
  }

}
