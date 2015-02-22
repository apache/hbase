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

package org.apache.hadoop.hbase.codec.prefixtree.decode;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.decode.column.ColumnReader;
import org.apache.hadoop.hbase.codec.prefixtree.decode.row.RowNodeReader;
import org.apache.hadoop.hbase.codec.prefixtree.decode.timestamp.MvccVersionDecoder;
import org.apache.hadoop.hbase.codec.prefixtree.decode.timestamp.TimestampDecoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.ColumnNodeType;

/**
 * Extends PtCell and manipulates its protected fields.  Could alternatively contain a PtCell and
 * call get/set methods.
 *
 * This is an "Array" scanner to distinguish from a future "ByteBuffer" scanner.  This
 * implementation requires that the bytes be in a normal java byte[] for performance.  The
 * alternative ByteBuffer implementation would allow for accessing data in an off-heap ByteBuffer
 * without copying the whole buffer on-heap.
 */
@InterfaceAudience.Private
public class PrefixTreeArrayScanner extends PrefixTreeCell implements CellScanner {

  /***************** fields ********************************/

  protected PrefixTreeBlockMeta blockMeta;

  protected boolean beforeFirst;
  protected boolean afterLast;

  protected RowNodeReader[] rowNodes;
  protected int rowNodeStackIndex;

  protected RowNodeReader currentRowNode;
  protected ColumnReader familyReader;
  protected ColumnReader qualifierReader;
  protected ColumnReader tagsReader;
  protected TimestampDecoder timestampDecoder;
  protected MvccVersionDecoder mvccVersionDecoder;

  protected boolean nubCellsRemain;
  protected int currentCellIndex;


  /*********************** construct ******************************/

  // pass in blockMeta so we can initialize buffers big enough for all cells in the block
  public PrefixTreeArrayScanner(PrefixTreeBlockMeta blockMeta, int rowTreeDepth,
      int rowBufferLength, int qualifierBufferLength, int tagsBufferLength) {
    this.rowNodes = new RowNodeReader[rowTreeDepth];
    for (int i = 0; i < rowNodes.length; ++i) {
      rowNodes[i] = new RowNodeReader();
    }
    this.rowBuffer = new byte[rowBufferLength];
    this.familyBuffer = new byte[PrefixTreeBlockMeta.MAX_FAMILY_LENGTH];
    this.familyReader = new ColumnReader(familyBuffer, ColumnNodeType.FAMILY);
    this.qualifierBuffer = new byte[qualifierBufferLength];
    this.tagsBuffer = new byte[tagsBufferLength];
    this.qualifierReader = new ColumnReader(qualifierBuffer, ColumnNodeType.QUALIFIER);
    this.tagsReader = new ColumnReader(tagsBuffer, ColumnNodeType.TAGS);
    this.timestampDecoder = new TimestampDecoder();
    this.mvccVersionDecoder = new MvccVersionDecoder();
  }


  /**************** init helpers ***************************************/

  /**
   * Call when first accessing a block.
   * @return entirely new scanner if false
   */
  public boolean areBuffersBigEnough() {
    if (rowNodes.length < blockMeta.getRowTreeDepth()) {
      return false;
    }
    if (rowBuffer.length < blockMeta.getMaxRowLength()) {
      return false;
    }
    if (qualifierBuffer.length < blockMeta.getMaxQualifierLength()) {
      return false;
    }
    if(tagsBuffer.length < blockMeta.getMaxTagsLength()) {
      return false;
    }
    return true;
  }

  public void initOnBlock(PrefixTreeBlockMeta blockMeta, byte[] block,
      boolean includeMvccVersion) {
    this.block = block;
    this.blockMeta = blockMeta;
    this.familyOffset = familyBuffer.length;
    this.familyReader.initOnBlock(blockMeta, block);
    this.qualifierOffset = qualifierBuffer.length;
    this.qualifierReader.initOnBlock(blockMeta, block);
    this.tagsOffset = tagsBuffer.length;
    this.tagsReader.initOnBlock(blockMeta, block);
    this.timestampDecoder.initOnBlock(blockMeta, block);
    this.mvccVersionDecoder.initOnBlock(blockMeta, block);
    this.includeMvccVersion = includeMvccVersion;
    resetToBeforeFirstEntry();
  }

  // Does this have to be in the CellScanner Interface?  TODO
  public void resetToBeforeFirstEntry() {
    beforeFirst = true;
    afterLast = false;
    rowNodeStackIndex = -1;
    currentRowNode = null;
    rowLength = 0;
    familyOffset = familyBuffer.length;
    familyLength = 0;
    qualifierOffset = blockMeta.getMaxQualifierLength();
    qualifierLength = 0;
    nubCellsRemain = false;
    currentCellIndex = -1;
    timestamp = -1L;
    type = DEFAULT_TYPE;
    absoluteValueOffset = 0;//use 0 vs -1 so the cell is valid when value hasn't been initialized
    valueLength = 0;// had it at -1, but that causes null Cell to add up to the wrong length
    tagsOffset = blockMeta.getMaxTagsLength();
    tagsLength = 0;
  }

  /**
   * Call this before putting the scanner back into a pool so it doesn't hold the last used block
   * in memory.
   */
  public void releaseBlockReference(){
    block = null;
  }


  /********************** CellScanner **********************/

  @Override
  public Cell current() {
    if(isOutOfBounds()){
      return null;
    }
    return (Cell)this;
  }

  /******************* Object methods ************************/

  @Override
  public boolean equals(Object obj) {
    //trivial override to confirm intent (findbugs)
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Override PrefixTreeCell.toString() with a check to see if the current cell is valid.
   */
  @Override
  public String toString() {
    Cell currentCell = current();
    if(currentCell==null){
      return "null";
    }
    return ((PrefixTreeCell)currentCell).getKeyValueString();
  }


  /******************* advance ***************************/

  public boolean positionAtFirstCell() {
    reInitFirstNode();
    return advance();
  }

  @Override
  public boolean advance() {
    if (afterLast) {
      return false;
    }
    if (!hasOccurrences()) {
      resetToBeforeFirstEntry();
    }
    if (beforeFirst || isLastCellInRow()) {
      nextRow();
      if (afterLast) {
        return false;
      }
    } else {
      ++currentCellIndex;
    }

    populateNonRowFields(currentCellIndex);
    return true;
  }


  public boolean nextRow() {
    nextRowInternal();
    if (afterLast) {
      return false;
    }
    populateNonRowFields(currentCellIndex);
    return true;
  }


  /**
   * This method is safe to call when the scanner is not on a fully valid row node, as in the case
   * of a row token miss in the Searcher
   * @return true if we are positioned on a valid row, false if past end of block
   */
  protected boolean nextRowInternal() {
    if (afterLast) {
      return false;
    }
    if (beforeFirst) {
      initFirstNode();
      if (currentRowNode.hasOccurrences()) {
        if (currentRowNode.isNub()) {
          nubCellsRemain = true;
        }
        currentCellIndex = 0;
        return true;
      }
    }
    if (currentRowNode.isLeaf()) {
      discardCurrentRowNode(true);
    }
    while (!afterLast) {
      if (nubCellsRemain) {
        nubCellsRemain = false;
      }
      if (currentRowNode.hasMoreFanNodes()) {
        followNextFan();
        if (currentRowNode.hasOccurrences()) {
          // found some values
          currentCellIndex = 0;
          return true;
        }
      } else {
        discardCurrentRowNode(true);
      }
    }
    return false;// went past the end
  }


  /**************** secondary traversal methods ******************************/

  protected void reInitFirstNode() {
    resetToBeforeFirstEntry();
    initFirstNode();
  }

  protected void initFirstNode() {
    int offsetIntoUnderlyingStructure = blockMeta.getAbsoluteRowOffset();
    rowNodeStackIndex = 0;
    currentRowNode = rowNodes[0];
    currentRowNode.initOnBlock(blockMeta, block, offsetIntoUnderlyingStructure);
    appendCurrentTokenToRowBuffer();
    beforeFirst = false;
  }

  protected void followFirstFan() {
    followFan(0);
  }

  protected void followPreviousFan() {
    int nextFanPosition = currentRowNode.getFanIndex() - 1;
    followFan(nextFanPosition);
  }

  protected void followCurrentFan() {
    int currentFanPosition = currentRowNode.getFanIndex();
    followFan(currentFanPosition);
  }

  protected void followNextFan() {
    int nextFanPosition = currentRowNode.getFanIndex() + 1;
    followFan(nextFanPosition);
  }

  protected void followLastFan() {
    followFan(currentRowNode.getLastFanIndex());
  }

  protected void followFan(int fanIndex) {
    currentRowNode.setFanIndex(fanIndex);
    appendToRowBuffer(currentRowNode.getFanByte(fanIndex));

    int nextOffsetIntoUnderlyingStructure = currentRowNode.getOffset()
        + currentRowNode.getNextNodeOffset(fanIndex, blockMeta);
    ++rowNodeStackIndex;

    currentRowNode = rowNodes[rowNodeStackIndex];
    currentRowNode.initOnBlock(blockMeta, block, nextOffsetIntoUnderlyingStructure);

    //TODO getToken is spewing garbage
    appendCurrentTokenToRowBuffer();
    if (currentRowNode.isNub()) {
      nubCellsRemain = true;
    }
    currentCellIndex = 0;
  }

  /**
   * @param forwards which marker to set if we overflow
   */
  protected void discardCurrentRowNode(boolean forwards) {
    RowNodeReader rowNodeBeingPopped = currentRowNode;
    --rowNodeStackIndex;// pop it off the stack
    if (rowNodeStackIndex < 0) {
      currentRowNode = null;
      if (forwards) {
        markAfterLast();
      } else {
        markBeforeFirst();
      }
      return;
    }
    popFromRowBuffer(rowNodeBeingPopped);
    currentRowNode = rowNodes[rowNodeStackIndex];
  }

  protected void markBeforeFirst() {
    beforeFirst = true;
    afterLast = false;
    currentRowNode = null;
  }

  protected void markAfterLast() {
    beforeFirst = false;
    afterLast = true;
    currentRowNode = null;
  }


  /***************** helper methods **************************/

  protected void appendCurrentTokenToRowBuffer() {
    System.arraycopy(block, currentRowNode.getTokenArrayOffset(), rowBuffer, rowLength,
      currentRowNode.getTokenLength());
    rowLength += currentRowNode.getTokenLength();
  }

  protected void appendToRowBuffer(byte b) {
    rowBuffer[rowLength] = b;
    ++rowLength;
  }

  protected void popFromRowBuffer(RowNodeReader rowNodeBeingPopped) {
    rowLength -= rowNodeBeingPopped.getTokenLength();
    --rowLength; // pop the parent's fan byte
  }

  protected boolean hasOccurrences() {
    return currentRowNode != null && currentRowNode.hasOccurrences();
  }

  protected boolean isBranch() {
    return currentRowNode != null && !currentRowNode.hasOccurrences()
        && currentRowNode.hasChildren();
  }

  protected boolean isNub() {
    return currentRowNode != null && currentRowNode.hasOccurrences()
        && currentRowNode.hasChildren();
  }

  protected boolean isLeaf() {
    return currentRowNode != null && currentRowNode.hasOccurrences()
        && !currentRowNode.hasChildren();
  }

  //TODO expose this in a PrefixTreeScanner interface
  public boolean isBeforeFirst(){
    return beforeFirst;
  }

  public boolean isAfterLast(){
    return afterLast;
  }

  protected boolean isOutOfBounds(){
    return beforeFirst || afterLast;
  }

  protected boolean isFirstCellInRow() {
    return currentCellIndex == 0;
  }

  protected boolean isLastCellInRow() {
    return currentCellIndex == currentRowNode.getLastCellIndex();
  }


  /********************* fill in family/qualifier/ts/type/value ************/

  protected int populateNonRowFieldsAndCompareTo(int cellNum, Cell key) {
    populateNonRowFields(cellNum);
    return CellComparator.compare(this, key, true);
  }

  protected void populateFirstNonRowFields() {
    populateNonRowFields(0);
  }

  protected void populatePreviousNonRowFields() {
    populateNonRowFields(currentCellIndex - 1);
  }

  protected void populateLastNonRowFields() {
    populateNonRowFields(currentRowNode.getLastCellIndex());
  }

  protected void populateNonRowFields(int cellIndex) {
    currentCellIndex = cellIndex;
    populateFamily();
    populateQualifier();
    // Read tags only if there are tags in the meta
    if(blockMeta.getNumTagsBytes() != 0) {
      populateTag();
    }
    populateTimestamp();
    populateMvccVersion();
    populateType();
    populateValueOffsets();
  }

  protected void populateFamily() {
    int familyTreeIndex = currentRowNode.getFamilyOffset(currentCellIndex, blockMeta);
    familyOffset = familyReader.populateBuffer(familyTreeIndex).getColumnOffset();
    familyLength = familyReader.getColumnLength();
  }

  protected void populateQualifier() {
    int qualifierTreeIndex = currentRowNode.getColumnOffset(currentCellIndex, blockMeta);
    qualifierOffset = qualifierReader.populateBuffer(qualifierTreeIndex).getColumnOffset();
    qualifierLength = qualifierReader.getColumnLength();
  }

  protected void populateTag() {
    int tagTreeIndex = currentRowNode.getTagOffset(currentCellIndex, blockMeta);
    tagsOffset = tagsReader.populateBuffer(tagTreeIndex).getColumnOffset();
    tagsLength = tagsReader.getColumnLength();
  }

  protected void populateTimestamp() {
    if (blockMeta.isAllSameTimestamp()) {
      timestamp = blockMeta.getMinTimestamp();
    } else {
      int timestampIndex = currentRowNode.getTimestampIndex(currentCellIndex, blockMeta);
      timestamp = timestampDecoder.getLong(timestampIndex);
    }
  }

  protected void populateMvccVersion() {
    if (blockMeta.isAllSameMvccVersion()) {
      mvccVersion = blockMeta.getMinMvccVersion();
    } else {
      int mvccVersionIndex = currentRowNode.getMvccVersionIndex(currentCellIndex,
        blockMeta);
      mvccVersion = mvccVersionDecoder.getMvccVersion(mvccVersionIndex);
    }
  }

  protected void populateType() {
    int typeInt;
    if (blockMeta.isAllSameType()) {
      typeInt = blockMeta.getAllTypes();
    } else {
      typeInt = currentRowNode.getType(currentCellIndex, blockMeta);
    }
    type = PrefixTreeCell.TYPES[typeInt];
  }

  protected void populateValueOffsets() {
    int offsetIntoValueSection = currentRowNode.getValueOffset(currentCellIndex, blockMeta);
    absoluteValueOffset = blockMeta.getAbsoluteValueOffset() + offsetIntoValueSection;
    valueLength = currentRowNode.getValueLength(currentCellIndex, blockMeta);
  }

  /**************** getters ***************************/

  public byte[] getTreeBytes() {
    return block;
  }

  public PrefixTreeBlockMeta getBlockMeta() {
    return blockMeta;
  }

  public int getMaxRowTreeStackNodes() {
    return rowNodes.length;
  }

  public int getRowBufferLength() {
    return rowBuffer.length;
  }

  public int getQualifierBufferLength() {
    return qualifierBuffer.length;
  }

  public int getTagBufferLength() {
    return tagsBuffer.length;
  }
}
