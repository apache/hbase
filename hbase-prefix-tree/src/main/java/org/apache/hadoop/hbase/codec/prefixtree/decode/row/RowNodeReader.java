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

package org.apache.hadoop.hbase.codec.prefixtree.decode.row;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.vint.UFIntTool;
import org.apache.hadoop.hbase.util.vint.UVIntTool;

/**
 * Position one of these appropriately in the data block and you can call its methods to retrieve
 * information necessary to decode the cells in the row.
 */
@InterfaceAudience.Private
public class RowNodeReader {

  /************* fields ***********************************/

  protected ByteBuff block;
  protected int offset;
  protected int fanIndex;

  protected int numCells;

  protected int tokenOffset;
  protected int tokenLength;
  protected int fanOffset;
  protected int fanOut;

  protected int familyOffsetsOffset;
  protected int qualifierOffsetsOffset;
  protected int timestampIndexesOffset;
  protected int mvccVersionIndexesOffset;
  protected int operationTypesOffset;
  protected int valueOffsetsOffset;
  protected int valueLengthsOffset;
  protected int tagOffsetsOffset;
  protected int nextNodeOffsetsOffset;


  /******************* construct **************************/

  public void initOnBlock(PrefixTreeBlockMeta blockMeta, ByteBuff block, int offset) {
    this.block = block;

    this.offset = offset;
    resetFanIndex();

    this.tokenLength = UVIntTool.getInt(block, offset);
    this.tokenOffset = offset + UVIntTool.numBytes(tokenLength);

    this.fanOut = UVIntTool.getInt(block, tokenOffset + tokenLength);
    this.fanOffset = tokenOffset + tokenLength + UVIntTool.numBytes(fanOut);

    this.numCells = UVIntTool.getInt(block, fanOffset + fanOut);

    this.familyOffsetsOffset = fanOffset + fanOut + UVIntTool.numBytes(numCells);
    this.qualifierOffsetsOffset = familyOffsetsOffset + numCells * blockMeta.getFamilyOffsetWidth();
    this.tagOffsetsOffset = this.qualifierOffsetsOffset + numCells * blockMeta.getQualifierOffsetWidth();
    // TODO : This code may not be needed now..As we always consider tags to be present
    if(blockMeta.getTagsOffsetWidth() == 0) {
      // Make both of them same so that we know that there are no tags
      this.tagOffsetsOffset = this.qualifierOffsetsOffset;
      this.timestampIndexesOffset = qualifierOffsetsOffset + numCells * blockMeta.getQualifierOffsetWidth();
    } else {
      this.timestampIndexesOffset = tagOffsetsOffset + numCells * blockMeta.getTagsOffsetWidth();
    }
    this.mvccVersionIndexesOffset = timestampIndexesOffset + numCells
        * blockMeta.getTimestampIndexWidth();
    this.operationTypesOffset = mvccVersionIndexesOffset + numCells
        * blockMeta.getMvccVersionIndexWidth();
    this.valueOffsetsOffset = operationTypesOffset + numCells * blockMeta.getKeyValueTypeWidth();
    this.valueLengthsOffset = valueOffsetsOffset + numCells * blockMeta.getValueOffsetWidth();
    this.nextNodeOffsetsOffset = valueLengthsOffset + numCells * blockMeta.getValueLengthWidth();
  }


  /******************** methods ****************************/

  public boolean isLeaf() {
    return fanOut == 0;
  }

  public boolean isNub() {
    return fanOut > 0 && numCells > 0;
  }

  public boolean isBranch() {
    return fanOut > 0 && numCells == 0;
  }

  public boolean hasOccurrences() {
    return numCells > 0;
  }

  public int getTokenArrayOffset(){
    return tokenOffset;
  }

  public int getTokenLength() {
    return tokenLength;
  }

  public byte getFanByte(int i) {
    return block.get(fanOffset + i);
  }
  
  /**
   * for debugging
   */
  protected String getFanByteReadable(int i){
    return ByteBuff.toStringBinary(block, fanOffset + i, 1);
  }

  public int getFamilyOffset(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getFamilyOffsetWidth();
    int startIndex = familyOffsetsOffset + fIntWidth * index;
    return (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
  }

  public int getColumnOffset(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getQualifierOffsetWidth();
    int startIndex = qualifierOffsetsOffset + fIntWidth * index;
    return (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
  }

  public int getTagOffset(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getTagsOffsetWidth();
    int startIndex = tagOffsetsOffset + fIntWidth * index;
    return (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
  }

  public int getTimestampIndex(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getTimestampIndexWidth();
    int startIndex = timestampIndexesOffset + fIntWidth * index;
    return (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
  }

  public int getMvccVersionIndex(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getMvccVersionIndexWidth();
    int startIndex = mvccVersionIndexesOffset + fIntWidth * index;
    return (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
  }

  public int getType(int index, PrefixTreeBlockMeta blockMeta) {
    if (blockMeta.isAllSameType()) {
      return blockMeta.getAllTypes();
    }
    return block.get(operationTypesOffset + index);
  }

  public int getValueOffset(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getValueOffsetWidth();
    int startIndex = valueOffsetsOffset + fIntWidth * index;
    int offset = (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
    return offset;
  }

  public int getValueLength(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getValueLengthWidth();
    int startIndex = valueLengthsOffset + fIntWidth * index;
    int length = (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
    return length;
  }

  public int getNextNodeOffset(int index, PrefixTreeBlockMeta blockMeta) {
    int fIntWidth = blockMeta.getNextNodeOffsetWidth();
    int startIndex = nextNodeOffsetsOffset + fIntWidth * index;
    return (int) UFIntTool.fromBytes(block, startIndex, fIntWidth);
  }

  public String getBranchNubLeafIndicator() {
    if (isNub()) {
      return "N";
    }
    return isBranch() ? "B" : "L";
  }

  public boolean hasChildren() {
    return fanOut > 0;
  }

  public int getLastFanIndex() {
    return fanOut - 1;
  }

  public int getLastCellIndex() {
    return numCells - 1;
  }

  public int getNumCells() {
    return numCells;
  }

  public int getFanOut() {
    return fanOut;
  }

  public byte[] getToken() {
    byte[] newToken = new byte[tokenLength];
    block.get(tokenOffset, newToken, 0, tokenLength);
    return newToken;
  }

  public int getOffset() {
    return offset;
  }

  public int whichFanNode(byte searchForByte) {
    if( ! hasFan()){
      throw new IllegalStateException("This row node has no fan, so can't search it");
    }
    int fanIndexInBlock = ByteBuff.unsignedBinarySearch(block, fanOffset, fanOffset + fanOut,
      searchForByte);
    if (fanIndexInBlock >= 0) {// found it, but need to adjust for position of fan in overall block
      return fanIndexInBlock - fanOffset;
    }
    return fanIndexInBlock + fanOffset;// didn't find it, so compensate in reverse
  }

  public void resetFanIndex() {
    fanIndex = -1;// just the way the logic currently works
  }

  public int getFanIndex() {
    return fanIndex;
  }

  public void setFanIndex(int fanIndex) {
    this.fanIndex = fanIndex;
  }

  public boolean hasFan(){
    return fanOut > 0;
  }

  public boolean hasPreviousFanNodes() {
    return fanOut > 0 && fanIndex > 0;
  }

  public boolean hasMoreFanNodes() {
    return fanIndex < getLastFanIndex();
  }

  public boolean isOnLastFanNode() {
    return !hasMoreFanNodes();
  }


  /*************** standard methods **************************/

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("fan:" + ByteBuff.toStringBinary(block, fanOffset, fanOut));
    sb.append(",token:" + ByteBuff.toStringBinary(block, tokenOffset, tokenLength));
    sb.append(",numCells:" + numCells);
    sb.append(",fanIndex:"+fanIndex);
    if(fanIndex>=0){
      sb.append("("+getFanByteReadable(fanIndex)+")");
    }
    return sb.toString();
  }
}
