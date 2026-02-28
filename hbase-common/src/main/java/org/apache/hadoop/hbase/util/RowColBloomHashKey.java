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
  // last 8 bytes (LATEST_TS[1..7] + MAX_TYPE) as LE long
  private static final long LAST_8_BYTES = -1L;
  private static final long LATEST_TS_LE = LittleEndianBytes.toLong(LATEST_TS, 0);
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
    // ROWCOL Bloom byte layout:
    // <2B RK length> <RK> <1B CF length> <CQ> <8B TS> <1B TYPE>
    if (offset < Bytes.SIZEOF_SHORT) {
      // assign locally
      int rowlen = rowLength;
      byte b = (byte) rowlen;
      if (offset == 0) {
        rowlen >>= 8;
        b = (byte) rowlen;
      }
      return b;
    }
    int refLen = Bytes.SIZEOF_SHORT + rowLength;
    if (offset < refLen) {
      return PrivateCellUtil.getRowByte(t, offset - Bytes.SIZEOF_SHORT);
    }
    if (offset == refLen) {
      // The fam length should return 0 assuming there is no column family.
      // Because for ROWCOL blooms family is not considered
      return 0;
    }
    refLen += qualLength + Bytes.SIZEOF_BYTE;
    // skip the family len because actual cells may have family also
    if (offset < refLen) {
      return PrivateCellUtil.getQualifierByte(t,
        offset - (Bytes.SIZEOF_SHORT + rowLength + Bytes.SIZEOF_BYTE));
    }
    // TODO : check if ts and type can be removed
    refLen += KeyValue.TIMESTAMP_SIZE;
    if (offset < refLen) {
      return LATEST_TS[offset - (Bytes.SIZEOF_SHORT + rowLength + qualLength + Bytes.SIZEOF_BYTE)];
    }
    return MAX_TYPE;
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

  @Override
  public long getLongLE(int offset) {
    // Handle fast path that can return the row key as long directly
    // Compute rowkey section range.
    final int rowEnd = KeyValue.ROW_LENGTH_SIZE + rowLength;
    if (offset >= KeyValue.ROW_LENGTH_SIZE && offset + Bytes.SIZEOF_LONG <= rowEnd) {
      return LittleEndianBytes.getRowAsLong(t, offset - KeyValue.ROW_LENGTH_SIZE);
    }

    // Compute qualifier section range.
    final int qualStart = rowEnd + KeyValue.FAMILY_LENGTH_SIZE;
    final int qualEnd = qualStart + qualLength;
    if (offset >= qualStart && offset + Bytes.SIZEOF_LONG <= qualEnd) {
      return LittleEndianBytes.getQualifierAsLong(t, offset - qualStart);
    }

    // Compute timestamp section range.
    if (offset == qualEnd) {
      return LATEST_TS_LE;
    }

    // Optimization: when the offset points to the last 8 bytes,
    // we can return the precomputed trailing long value directly.
    if (offset + Bytes.SIZEOF_LONG == totalLength) {
      return LAST_8_BYTES;
    }

    return assembleCrossingLE(offset, Bytes.SIZEOF_LONG);
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
        if (pos == 0) {
          result |= (rowLength >>> 8) & 0xFF;
          result |= (rowLength & 0xFF) << 8;
          pos += 2;
          remaining -= 2;
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
