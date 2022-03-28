/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RowIndexEncoderV1 {
  private static final Logger LOG = LoggerFactory.getLogger(RowIndexEncoderV1.class);

  /** The Cell previously appended. */
  private Cell lastCell = null;

  private DataOutputStream out;
  private NoneEncoder encoder;
  private int startOffset = -1;
  private ByteArrayOutputStream rowsOffsetBAOS = new ByteArrayOutputStream(64 * 4);
  private final HFileBlockEncodingContext context;

  public RowIndexEncoderV1(DataOutputStream out, HFileBlockDefaultEncodingContext encodingCtx) {
    this.out = out;
    this.encoder = new NoneEncoder(out, encodingCtx);
    this.context = encodingCtx;
  }

  public void write(Cell cell) throws IOException {
    // checkRow uses comparator to check we are writing in order.
    int extraBytesForRowIndex = 0;

    if (!checkRow(cell)) {
      if (startOffset < 0) {
        startOffset = out.size();
      }
      rowsOffsetBAOS.writeInt(out.size() - startOffset);
      // added for the int written in the previous line
      extraBytesForRowIndex = Bytes.SIZEOF_INT;
    }
    lastCell = cell;
    int size = encoder.write(cell);
    context.getEncodingState().postCellEncode(size, size + extraBytesForRowIndex);
  }

  protected boolean checkRow(final Cell cell) throws IOException {
    boolean isDuplicateRow = false;
    if (cell == null) {
      throw new IOException("Key cannot be null or empty");
    }
    if (lastCell != null) {
      int keyComp = this.context.getHFileContext().getCellComparator().compareRows(lastCell, cell);
      if (keyComp > 0) {
        throw new IOException("Added a key not lexically larger than"
            + " previous. Current cell = " + cell + ", lastCell = " + lastCell);
      } else if (keyComp == 0) {
        isDuplicateRow = true;
      }
    }
    return isDuplicateRow;
  }

  public void flush() throws IOException {
    int onDiskDataSize = 0;
    if (startOffset >= 0) {
      onDiskDataSize = out.size() - startOffset;
    }
    out.writeInt(rowsOffsetBAOS.size() / 4);
    if (rowsOffsetBAOS.size() > 0) {
      out.write(rowsOffsetBAOS.getBuffer(), 0, rowsOffsetBAOS.size());
    }
    out.writeInt(onDiskDataSize);
    if (LOG.isTraceEnabled()) {
      LOG.trace("RowNumber: " + rowsOffsetBAOS.size() / 4
          + ", onDiskDataSize: " + onDiskDataSize + ", totalOnDiskSize: "
          + (out.size() - startOffset));
    }
  }

  void beforeShipped() {
    if (this.lastCell != null) {
      this.lastCell = KeyValueUtil.toNewKeyCell(this.lastCell);
    }
  }
}
