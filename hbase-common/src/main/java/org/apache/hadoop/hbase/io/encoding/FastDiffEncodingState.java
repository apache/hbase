/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class FastDiffEncodingState extends DiffEncodingState {

  private int lastMaterializedValueStreamOffset = -1;
  private int currentCellValueStreamOffset = -1;
  private byte[] shippedValueBuffer;
  private int shippedValueAbsoluteOffset;

  boolean matchingPreviousValue(ExtendedCell cell) {
    if (shippedValueBuffer != null) {
      return PrivateCellUtil.matchingValue(cell, shippedValueBuffer, shippedValueAbsoluteOffset,
        getPreviousValueLength());
    }
    return prevCell != null && PrivateCellUtil.matchingValue(cell, prevCell,
        cell.getValueLength(), getPreviousValueLength());
  }

  void beginCellEncoding() {
    currentCellValueStreamOffset = -1;
  }

  void setCurrentCellValueStreamOffset(int valueStreamOffset) {
    currentCellValueStreamOffset = valueStreamOffset;
  }

  @Override
  protected void setPreviousCell(ExtendedCell cell) {
    super.setPreviousCell(cell);
    if (currentCellValueStreamOffset >= 0) {
      lastMaterializedValueStreamOffset = currentCellValueStreamOffset;
    }
    currentCellValueStreamOffset = -1;
    shippedValueBuffer = null;
  }

  @Override
  public void beforeShipped(byte[] encodedBlockBuffer, int streamBaseOffset,
      int encodedBlockLength) {
    if (shippedValueBuffer != null || prevCell == null
        || lastMaterializedValueStreamOffset < 0) {
      return;
    }

    if (encodedBlockBuffer == null || streamBaseOffset < 0 || streamBaseOffset > encodedBlockLength
        || encodedBlockLength > encodedBlockBuffer.length) {
      throw new IllegalArgumentException("Invalid encoded block buffer range");
    }
    int valueLength = getPreviousValueLength();
    int encodedStreamLength = encodedBlockLength - streamBaseOffset;
    if (lastMaterializedValueStreamOffset > encodedStreamLength
        || valueLength > encodedStreamLength - lastMaterializedValueStreamOffset) {
      throw new IllegalArgumentException("Invalid encoded value range");
    }

    super.beforeShipped(encodedBlockBuffer, streamBaseOffset, encodedBlockLength);
    shippedValueBuffer = encodedBlockBuffer;
    shippedValueAbsoluteOffset =
      streamBaseOffset + lastMaterializedValueStreamOffset;
  }
}
