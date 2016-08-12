/**
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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * An hash key for ROWCOL bloom. This assumes the cells to be serialized in the Keyvalue
 * serialization format with Empty column family. Note that the byte representing the family length
 * is considered to be 0
 */
@InterfaceAudience.Private
public class RowColBloomHashKey extends CellHashKey {

  private final int rowLength;
  private final int qualLength;

  public RowColBloomHashKey(Cell cell) {
    super(cell);
    rowLength = cell.getRowLength();
    // We don't consider the family length for ROWCOL bloom. So subtract the famLen from the
    // length calculation. Timestamp and type are of no relevance here
    qualLength = cell.getQualifierLength();
  }

  @Override
  public byte get(int offset) {
    // Always assume that this cell has keyvalue serialized key structure.
    // rk len + row key + 0 byte for family length + qual + ts + type
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
      return CellUtil.getRowByte(t, offset - Bytes.SIZEOF_SHORT);
    }
    if (offset == refLen) {
      // The fam length should return 0 assuming there is no column family.
      // Because for ROWCOL blooms family is not considered
      return 0;
    }
    refLen += qualLength + Bytes.SIZEOF_BYTE;
    // skip the family len because actual cells may have family also
    if (offset < refLen) {
      return CellUtil.getQualifierByte(t,
        offset - (Bytes.SIZEOF_SHORT + rowLength + Bytes.SIZEOF_BYTE));
    }
    // TODO : check if ts and type can be removed
    refLen += KeyValue.TIMESTAMP_SIZE;
    if (offset < refLen) {
      return LATEST_TS[offset - (Bytes.SIZEOF_SHORT + rowLength + qualLength + Bytes.SIZEOF_BYTE)];
    }
    return MAX_TYPE;
  }
}
