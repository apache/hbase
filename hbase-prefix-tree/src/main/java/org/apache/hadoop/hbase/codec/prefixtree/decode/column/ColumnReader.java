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

package org.apache.hadoop.hbase.codec.prefixtree.decode.column;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.ColumnNodeType;
import org.apache.hadoop.hbase.nio.ByteBuff;

/**
 * Position one of these appropriately in the data block and you can call its methods to retrieve
 * the family or qualifier at the current position.
 */
@InterfaceAudience.Private
public class ColumnReader {

  /****************** fields *************************/

  protected PrefixTreeBlockMeta blockMeta;

  protected byte[] columnBuffer;
  protected int columnOffset;
  protected int columnLength;
  protected ColumnNodeType nodeType;  

  protected ColumnNodeReader columnNodeReader;


  /******************** construct *******************/

  public ColumnReader(byte[] columnBuffer, ColumnNodeType nodeType) {
    this.columnBuffer = columnBuffer;
    this.nodeType = nodeType;
    this.columnNodeReader = new ColumnNodeReader(columnBuffer, nodeType);
  }

  public void initOnBlock(PrefixTreeBlockMeta blockMeta, ByteBuff block) {
    this.blockMeta = blockMeta;
    clearColumnBuffer();
    columnNodeReader.initOnBlock(blockMeta, block);
  }


  /********************* methods *******************/

  public ColumnReader populateBuffer(int offsetIntoColumnData) {
    clearColumnBuffer();
    int nextRelativeOffset = offsetIntoColumnData;
    while (true) {
      int absoluteOffset = 0;
      if (nodeType == ColumnNodeType.FAMILY) {
        absoluteOffset = blockMeta.getAbsoluteFamilyOffset() + nextRelativeOffset;
      } else if (nodeType == ColumnNodeType.QUALIFIER) {
        absoluteOffset = blockMeta.getAbsoluteQualifierOffset() + nextRelativeOffset;
      } else {
        absoluteOffset = blockMeta.getAbsoluteTagsOffset() + nextRelativeOffset;
      }
      columnNodeReader.positionAt(absoluteOffset);
      columnOffset -= columnNodeReader.getTokenLength();
      columnLength += columnNodeReader.getTokenLength();
      columnNodeReader.prependTokenToBuffer(columnOffset);
      if (columnNodeReader.isRoot()) {
        return this;
      }
      nextRelativeOffset = columnNodeReader.getParentStartPosition();
    }
  }

  public byte[] copyBufferToNewArray() {// for testing
    byte[] out = new byte[columnLength];
    System.arraycopy(columnBuffer, columnOffset, out, 0, out.length);
    return out;
  }

  public int getColumnLength() {
    return columnLength;
  }

  public void clearColumnBuffer() {
    columnOffset = columnBuffer.length;
    columnLength = 0;
  }


  /****************************** get/set *************************************/

  public int getColumnOffset() {
    return columnOffset;
  }

}

