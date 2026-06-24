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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A hash key for ROWCOL bloom. This assumes the cells to be serialized in the Keyvalue
 * serialization format with Empty column family. Note that the byte representing the family length
 * is considered to be 0
 */
@InterfaceAudience.Private
public class RowColBloomHashKey extends CellHashKey {
  private final int rowLength;
  private final int qualLength;
  private final int totalLength;

  public RowColBloomHashKey(Cell cell) {
    super(cell);
    rowLength = cell.getRowLength();
    // We don't consider the family length for ROWCOL bloom. So subtract the famLen from the
    // length calculation. Timestamp and type are of no relevance here
    qualLength = cell.getQualifierLength();
    // ROWCOL Bloom byte layout:
    // <2B RK length> <RK> <1B CF length> <CQ> <8B TS> <1B TYPE>
    totalLength = KeyValue.ROW_LENGTH_SIZE + rowLength + KeyValue.FAMILY_LENGTH_SIZE + qualLength
      + KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public byte get(int offset) {
    return (byte) assembleCrossingLE(offset, Bytes.SIZEOF_BYTE);
  }

  @Override
  public int length() {
    return totalLength;
  }

  @Override
  public int getIntLE(int offset) {
    // Handle fast path that can return the row key as int directly
    // Compute rowkey section range.
    final int rowEnd = KeyValue.ROW_LENGTH_SIZE + rowLength;
    if (offset >= KeyValue.ROW_LENGTH_SIZE && offset + Bytes.SIZEOF_INT <= rowEnd) {
      return LittleEndianBytes.getRowAsInt(t, offset - KeyValue.ROW_LENGTH_SIZE);
    }

    // Compute qualifier section range.
    final int qualStart = rowEnd + KeyValue.FAMILY_LENGTH_SIZE;
    final int qualEnd = qualStart + qualLength;
    if (offset >= qualStart && offset + Bytes.SIZEOF_INT <= qualEnd) {
      return LittleEndianBytes.getQualifierAsInt(t, offset - qualStart);
    }

    // Compute timestamp section range.
    final int tsEnd = qualEnd + KeyValue.TIMESTAMP_SIZE;
    if (offset >= qualEnd && offset + Bytes.SIZEOF_INT <= tsEnd) {
      return LittleEndianBytes.toInt(LATEST_TS, offset - qualEnd);
    }

    return (int) assembleCrossingLE(offset, Bytes.SIZEOF_INT);
  }

  private long assembleCrossingLE(int offset, int wordBytes) {
    final int rowEnd = KeyValue.ROW_LENGTH_SIZE + rowLength;
    final int qualStart = rowEnd + KeyValue.FAMILY_LENGTH_SIZE;
    final int qualEnd = qualStart + qualLength;
    final int tsEnd = qualEnd + KeyValue.TIMESTAMP_SIZE;

    long result = 0L;
    int pos = offset;
    int remaining = wordBytes;

    while (remaining > 0) {
      // 1) row length field [0,2)
      if (pos < KeyValue.ROW_LENGTH_SIZE) {
        if (pos == 0 && remaining >= KeyValue.ROW_LENGTH_SIZE) {
          result |= (rowLength >>> 8) & 0xFF;
          result |= (rowLength & 0xFF) << 8;
          pos += 2;
          remaining -= 2;
        } else if (pos == 0) {
          result |= (rowLength >>> 8) & 0xFF;
          pos += 1;
          remaining -= 1;
        } else {
          result |= rowLength & 0xFF;
          pos += 1;
          remaining -= 1;
        }
        continue;
      }

      // 2) row bytes [2, rowEnd)
      if (pos < rowEnd) {
        final int take = Math.min(rowEnd - pos, remaining);
        final int rOffset = pos - KeyValue.ROW_LENGTH_SIZE;
        for (int i = 0; i < take; i++) {
          final int shift = (wordBytes - remaining) * 8;
          final byte b = PrivateCellUtil.getRowByte(t, rOffset + i);
          result |= ((long) b & 0xFF) << shift;
          remaining -= 1;
        }
        pos += take;
        continue;
      }

      // 3) family length byte (always 0)
      if (pos == rowEnd) {
        pos += 1;
        remaining -= 1;
        continue;
      }

      // 4) qualifier bytes [qualStart, qualEnd)
      if (pos < qualEnd) {
        final int take = Math.min(qualEnd - pos, remaining);
        final int qOffset = pos - qualStart;
        for (int i = 0; i < take; i++) {
          final int shift = (wordBytes - remaining) * 8;
          final int b = PrivateCellUtil.getQualifierByte(t, qOffset + i) & 0xFF;
          result |= ((long) b) << shift;
          remaining -= 1;
        }
        pos += take;
        continue;
      }

      // 5) timestamp bytes [qualEnd, tsEnd) -> LATEST_TS
      if (pos < tsEnd) {
        final int take = Math.min(tsEnd - pos, remaining);
        final int tsOff = pos - qualEnd;
        for (int i = 0; i < take; i++) {
          final int shift = (wordBytes - remaining) * 8;
          final int b = LATEST_TS[tsOff + i] & 0xFF;
          result |= ((long) b) << shift;
          remaining -= 1;
        }
        pos += take;
        continue;
      }

      // 6) type byte at typePos -> MAX_TYPE
      final int shift = (wordBytes - remaining) * 8;
      result |= ((long) MAX_TYPE & 0xFF) << shift;
      pos += 1;
      remaining -= 1;
    }

    return result;
  }
}
