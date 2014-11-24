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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This wraps the key portion of a Cell. Key includes rowkey, family, qualifier, timestamp and type
 */
@InterfaceAudience.Private
public class CellKey {

  private byte[] rowArray;
  private int rowOffset;
  private int rowLength;
  private byte[] familyArray;
  private int familyOffset;
  private int familyLength;
  private byte[] qualifierArray;
  private int qualifierOffset;
  private int qualifierLength;
  private long ts;
  private byte type;

  public CellKey(byte[] rowArray, int rowOffset, int rowLength, byte[] familyArray,
      int familyOffset, int familyLength, byte[] qualifierArray, int qualifierOffset,
      int qualifierLength, long ts, byte type) {
    this.rowArray = rowArray;
    this.rowOffset = rowOffset;
    this.rowLength = rowLength;
    this.familyArray = familyArray;
    this.familyOffset = familyOffset;
    this.familyLength = familyLength;
    this.qualifierArray = qualifierArray;
    this.qualifierOffset = qualifierOffset;
    this.qualifierLength = qualifierLength;
    this.ts = ts;
    this.type = type;
  }

  @Override
  public String toString() {
    String row = Bytes.toStringBinary(rowArray, rowOffset, rowLength);
    String family = (familyLength == 0) ? "" : Bytes.toStringBinary(familyArray, familyOffset,
        familyLength);
    String qualifier = (qualifierLength == 0) ? "" : Bytes.toStringBinary(qualifierArray,
        qualifierOffset, qualifierLength);
    return row + "/" + family +
        (family != null && family.length() > 0 ? ":" : "") + qualifier
        + "/" + KeyValue.humanReadableTimestamp(ts) + "/" + Type.codeToType(type);
  }
}
