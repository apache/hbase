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

package org.apache.hadoop.hbase.codec.prefixtree.encode.row;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.encode.PrefixTreeEncoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.util.ByteRangeUtils;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.vint.UFIntTool;
import org.apache.hadoop.hbase.util.vint.UVIntTool;

/**
 * Serializes the fields comprising one node of the row trie, which can be a branch, nub, or leaf.
 * Please see the write() method for the order in which data is written.
 */
@InterfaceAudience.Private
public class RowNodeWriter{
  protected static final Log LOG = LogFactory.getLog(RowNodeWriter.class);

  /********************* fields ******************************/

  protected PrefixTreeEncoder prefixTreeEncoder;
  protected PrefixTreeBlockMeta blockMeta;
  protected TokenizerNode tokenizerNode;

  protected int tokenWidth;
  protected int fanOut;
  protected int numCells;

  protected int width;


  /*********************** construct *************************/

  public RowNodeWriter(PrefixTreeEncoder keyValueBuilder, TokenizerNode tokenizerNode) {
    reconstruct(keyValueBuilder, tokenizerNode);
  }

  public void reconstruct(PrefixTreeEncoder prefixTreeEncoder, TokenizerNode tokenizerNode) {
    this.prefixTreeEncoder = prefixTreeEncoder;
    reset(tokenizerNode);
  }

  public void reset(TokenizerNode node) {
    this.blockMeta = prefixTreeEncoder.getBlockMeta();// changes between blocks
    this.tokenizerNode = node;
    this.tokenWidth = 0;
    this.fanOut = 0;
    this.numCells = 0;
    this.width = 0;
    calculateOffsetsAndLengths();
  }


  /********************* methods ****************************/

  protected void calculateOffsetsAndLengths(){
    tokenWidth = tokenizerNode.getTokenLength();
    if(!tokenizerNode.isRoot()){
      --tokenWidth;//root has no parent
    }
    fanOut = CollectionUtils.nullSafeSize(tokenizerNode.getChildren());
    numCells = tokenizerNode.getNumOccurrences();
  }

  public int calculateWidth(){
    calculateWidthOverrideOffsetWidth(blockMeta.getNextNodeOffsetWidth());
    return width;
  }

  public int calculateWidthOverrideOffsetWidth(int offsetWidth){
    width = 0;
    width += UVIntTool.numBytes(tokenWidth);
    width += tokenWidth;

    width += UVIntTool.numBytes(fanOut);
    width += fanOut;

    width += UVIntTool.numBytes(numCells);

    if(tokenizerNode.hasOccurrences()){
      int fixedBytesPerCell = blockMeta.getFamilyOffsetWidth()
        + blockMeta.getQualifierOffsetWidth()
        + blockMeta.getTagsOffsetWidth()
        + blockMeta.getTimestampIndexWidth()
        + blockMeta.getMvccVersionIndexWidth()
        + blockMeta.getKeyValueTypeWidth()
        + blockMeta.getValueOffsetWidth()
        + blockMeta.getValueLengthWidth();
      width += numCells * fixedBytesPerCell;
    }

    if( ! tokenizerNode.isLeaf()){
      width += fanOut * offsetWidth;
    }

    return width;
  }


  /*********************** writing the compiled structure to the OutputStream ***************/

  public void write(OutputStream os) throws IOException{
    //info about this row trie node
    writeRowToken(os);
    writeFan(os);
    writeNumCells(os);

    //UFInt indexes and offsets for each cell in the row (if nub or leaf)
    writeFamilyNodeOffsets(os);
    writeQualifierNodeOffsets(os);
    writeTagNodeOffsets(os);
    writeTimestampIndexes(os);
    writeMvccVersionIndexes(os);
    writeCellTypes(os);
    writeValueOffsets(os);
    writeValueLengths(os);
    //offsets to the children of this row trie node (if branch or nub)
    writeNextRowTrieNodeOffsets(os);
  }


  /**
   * Row node token, fan, and numCells. Written once at the beginning of each row node. These 3
   * fields can reproduce all the row keys that compose the block.
   */

  /**
   * UVInt: tokenWidth
   * bytes: token
   */
  protected void writeRowToken(OutputStream os) throws IOException {
    UVIntTool.writeBytes(tokenWidth, os);
    int tokenStartIndex = tokenizerNode.isRoot() ? 0 : 1;
    ByteRangeUtils.write(os, tokenizerNode.getToken(), tokenStartIndex);
  }

  /**
   * UVInt: numFanBytes/fanOut
   * bytes: each fan byte
   */
  public void writeFan(OutputStream os) throws IOException {
    UVIntTool.writeBytes(fanOut, os);
    if (fanOut <= 0) {
      return;
    }
    ArrayList<TokenizerNode> children = tokenizerNode.getChildren();
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      os.write(child.getToken().get(0));// first byte of each child's token
    }
  }

  /**
   * UVInt: numCells, the number of cells in this row which will be 0 for branch nodes
   */
  protected void writeNumCells(OutputStream os) throws IOException {
    UVIntTool.writeBytes(numCells, os);
  }


  /**
   * The following methods write data for each cell in the row, mostly consisting of indexes or
   * offsets into the timestamp/column data structures that are written in the middle of the block.
   * We use {@link UFIntTool} to encode these indexes/offsets to allow random access during a binary
   * search of a particular column/timestamp combination.
   * <p/>
   * Branch nodes will not have any data in these sections.
   */

  protected void writeFamilyNodeOffsets(OutputStream os) throws IOException {
    if (blockMeta.getFamilyOffsetWidth() <= 0) {
      return;
    }
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = PrefixTreeEncoder.MULITPLE_FAMILIES_POSSIBLE ? tokenizerNode
          .getFirstInsertionIndex() + i : 0;
      int sortedIndex = prefixTreeEncoder.getFamilySorter().getSortedIndexForInsertionId(
        cellInsertionIndex);
      int indexedFamilyOffset = prefixTreeEncoder.getFamilyWriter().getOutputArrayOffset(
        sortedIndex);
      UFIntTool.writeBytes(blockMeta.getFamilyOffsetWidth(), indexedFamilyOffset, os);
    }
  }

  protected void writeQualifierNodeOffsets(OutputStream os) throws IOException {
    if (blockMeta.getQualifierOffsetWidth() <= 0) {
      return;
    }
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      int sortedIndex = prefixTreeEncoder.getQualifierSorter().getSortedIndexForInsertionId(
        cellInsertionIndex);
      int indexedQualifierOffset = prefixTreeEncoder.getQualifierWriter().getOutputArrayOffset(
        sortedIndex);
      UFIntTool.writeBytes(blockMeta.getQualifierOffsetWidth(), indexedQualifierOffset, os);
    }
  }

  protected void writeTagNodeOffsets(OutputStream os) throws IOException {
    if (blockMeta.getTagsOffsetWidth() <= 0) {
      return;
    }
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      int sortedIndex = prefixTreeEncoder.getTagSorter().getSortedIndexForInsertionId(
        cellInsertionIndex);
      int indexedTagOffset = prefixTreeEncoder.getTagWriter().getOutputArrayOffset(
        sortedIndex);
      UFIntTool.writeBytes(blockMeta.getTagsOffsetWidth(), indexedTagOffset, os);
    }
  }

  protected void writeTimestampIndexes(OutputStream os) throws IOException {
    if (blockMeta.getTimestampIndexWidth() <= 0) {
      return;
    }
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      long timestamp = prefixTreeEncoder.getTimestamps()[cellInsertionIndex];
      int timestampIndex = prefixTreeEncoder.getTimestampEncoder().getIndex(timestamp);
      UFIntTool.writeBytes(blockMeta.getTimestampIndexWidth(), timestampIndex, os);
    }
  }

  protected void writeMvccVersionIndexes(OutputStream os) throws IOException {
    if (blockMeta.getMvccVersionIndexWidth() <= 0) {
      return;
    }
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      long mvccVersion = prefixTreeEncoder.getMvccVersions()[cellInsertionIndex];
      int mvccVersionIndex = prefixTreeEncoder.getMvccVersionEncoder().getIndex(mvccVersion);
      UFIntTool.writeBytes(blockMeta.getMvccVersionIndexWidth(), mvccVersionIndex, os);
    }
  }

  protected void writeCellTypes(OutputStream os) throws IOException {
    if (blockMeta.isAllSameType()) {
      return;
    }
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      os.write(prefixTreeEncoder.getTypeBytes()[cellInsertionIndex]);
    }
  }

  protected void writeValueOffsets(OutputStream os) throws IOException {
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      long valueStartIndex = prefixTreeEncoder.getValueOffset(cellInsertionIndex);
      UFIntTool.writeBytes(blockMeta.getValueOffsetWidth(), valueStartIndex, os);
    }
  }

  protected void writeValueLengths(OutputStream os) throws IOException {
    for (int i = 0; i < numCells; ++i) {
      int cellInsertionIndex = tokenizerNode.getFirstInsertionIndex() + i;
      int valueLength = prefixTreeEncoder.getValueLength(cellInsertionIndex);
      UFIntTool.writeBytes(blockMeta.getValueLengthWidth(), valueLength, os);
    }
  }

  /**
   * If a branch or a nub, the last thing we append are the UFInt offsets to the child row nodes.
   */
  protected void writeNextRowTrieNodeOffsets(OutputStream os) throws IOException {
    ArrayList<TokenizerNode> children = tokenizerNode.getChildren();
    for (int i = 0; i < children.size(); ++i) {
      TokenizerNode child = children.get(i);
      int distanceToChild = tokenizerNode.getNegativeIndex() - child.getNegativeIndex();
      UFIntTool.writeBytes(blockMeta.getNextNodeOffsetWidth(), distanceToChild, os);
    }
  }
}
