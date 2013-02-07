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

package org.apache.hbase.codec.prefixtree.decode.column;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hbase.util.vint.UFIntTool;
import org.apache.hbase.util.vint.UVIntTool;

@InterfaceAudience.Private
public class ColumnNodeReader {

  /**************** fields ************************/

  protected PrefixTreeBlockMeta blockMeta;
  protected byte[] block;

  protected byte[] columnBuffer;
  protected boolean familyVsQualifier;

  protected int offsetIntoBlock;

  protected int tokenOffsetIntoBlock;
  protected int tokenLength;
  protected int parentStartPosition;


  /************** construct *************************/

  public ColumnNodeReader(byte[] columnBuffer, boolean familyVsQualifier) {
    this.columnBuffer = columnBuffer;
    this.familyVsQualifier = familyVsQualifier;
  }

  public void initOnBlock(PrefixTreeBlockMeta blockMeta, byte[] block) {
    this.blockMeta = blockMeta;
    this.block = block;
  }


  /************* methods *****************************/

  public void positionAt(int offsetIntoBlock) {
    this.offsetIntoBlock = offsetIntoBlock;
    tokenLength = UVIntTool.getInt(block, offsetIntoBlock);
    tokenOffsetIntoBlock = offsetIntoBlock + UVIntTool.numBytes(tokenLength);
    int parentStartPositionIndex = tokenOffsetIntoBlock + tokenLength;
    int offsetWidth;
    if (familyVsQualifier) {
      offsetWidth = blockMeta.getFamilyOffsetWidth();
    } else {
      offsetWidth = blockMeta.getQualifierOffsetWidth();
    }
    parentStartPosition = (int) UFIntTool.fromBytes(block, parentStartPositionIndex, offsetWidth);
  }

  public void prependTokenToBuffer(int bufferStartIndex) {
    System.arraycopy(block, tokenOffsetIntoBlock, columnBuffer, bufferStartIndex, tokenLength);
  }

  public boolean isRoot() {
    if (familyVsQualifier) {
      return offsetIntoBlock == blockMeta.getAbsoluteFamilyOffset();
    } else {
      return offsetIntoBlock == blockMeta.getAbsoluteQualifierOffset();
    }
  }


  /************** standard methods *********************/

  @Override
  public String toString() {
    return super.toString() + "[" + offsetIntoBlock + "]";
  }


  /****************** get/set ****************************/

  public int getTokenLength() {
    return tokenLength;
  }

  public int getParentStartPosition() {
    return parentStartPosition;
  }

}
