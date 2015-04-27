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

package org.apache.hadoop.hbase.codec.prefixtree.encode.column;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.ColumnNodeType;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.vint.UFIntTool;
import org.apache.hadoop.hbase.util.vint.UVIntTool;

/**
 * <p>
 * Column nodes can be either family nodes or qualifier nodes, as both sections encode similarly.
 * The family and qualifier sections of the data block are made of 1 or more of these nodes.
 * </p>
 * Each node is composed of 3 sections:<br>
 * <ul>
 * <li>tokenLength: UVInt (normally 1 byte) indicating the number of token bytes
 * <li>token[]: the actual token bytes
 * <li>parentStartPosition: the offset of the next node from the start of the family or qualifier
 * section
 * </ul>
 */
@InterfaceAudience.Private
public class ColumnNodeWriter{

  /************* fields ****************************/

  protected TokenizerNode builderNode;
  protected PrefixTreeBlockMeta blockMeta;

  protected int tokenLength;
  protected byte[] token;
  protected int parentStartPosition;
  protected ColumnNodeType nodeType;


  /*************** construct **************************/

  public ColumnNodeWriter(PrefixTreeBlockMeta blockMeta, TokenizerNode builderNode,
      ColumnNodeType nodeType) {
    this.blockMeta = blockMeta;
    this.builderNode = builderNode;
    this.nodeType = nodeType;
    calculateTokenLength();
  }


  /************* methods *******************************/

  public boolean isRoot() {
    return parentStartPosition == 0;
  }

  private void calculateTokenLength() {
    tokenLength = builderNode.getTokenLength();
    token = new byte[tokenLength];
  }

  /**
   * This method is called before blockMeta.qualifierOffsetWidth is known, so we pass in a
   * placeholder.
   * @param offsetWidthPlaceholder the placeholder
   * @return node width
   */
  public int getWidthUsingPlaceholderForOffsetWidth(int offsetWidthPlaceholder) {
    int width = 0;
    width += UVIntTool.numBytes(tokenLength);
    width += token.length;
    width += offsetWidthPlaceholder;
    return width;
  }

  public void writeBytes(OutputStream os) throws IOException {
    int parentOffsetWidth;
    if (this.nodeType == ColumnNodeType.FAMILY) {
      parentOffsetWidth = blockMeta.getFamilyOffsetWidth();
    } else if (this.nodeType == ColumnNodeType.QUALIFIER) {
      parentOffsetWidth = blockMeta.getQualifierOffsetWidth();
    } else {
      parentOffsetWidth = blockMeta.getTagsOffsetWidth();
    }
    UVIntTool.writeBytes(tokenLength, os);
    os.write(token);
    UFIntTool.writeBytes(parentOffsetWidth, parentStartPosition, os);
  }

  public void setTokenBytes(ByteRange source) {
    source.deepCopySubRangeTo(0, tokenLength, token, 0);
  }


  /****************** standard methods ************************/

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Strings.padFront(builderNode.getOutputArrayOffset() + "", ' ', 3) + ",");
    sb.append("[");
    sb.append(Bytes.toString(token));
    sb.append("]->");
    sb.append(parentStartPosition);
    return sb.toString();
  }


  /************************** get/set ***********************/

  public void setParentStartPosition(int parentStartPosition) {
    this.parentStartPosition = parentStartPosition;
  }

}
