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

package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IterableUtils;
import org.apache.hadoop.hbase.util.Strings;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class KeyValueTestUtil {

  public static KeyValue create(
      String row,
      String family,
      String qualifier,
      long timestamp,
      String value)
  {
    return create(row, family, qualifier, timestamp, KeyValue.Type.Put, value);
  }

  public static KeyValue create(
      String row,
      String family,
      String qualifier,
      long timestamp,
      KeyValue.Type type,
      String value)
  {
      return new KeyValue(
          Bytes.toBytes(row),
          Bytes.toBytes(family),
          Bytes.toBytes(qualifier),
          timestamp,
          type,
          Bytes.toBytes(value)
      );
  }

  public static ByteBuffer toByteBufferAndRewind(final Iterable<? extends KeyValue> kvs,
      boolean includeMemstoreTS) {
    int totalBytes = KeyValueUtil.totalLengthWithMvccVersion(kvs, includeMemstoreTS);
    ByteBuffer bb = ByteBuffer.allocate(totalBytes);
    for (KeyValue kv : IterableUtils.nullSafe(kvs)) {
      KeyValueUtil.appendToByteBuffer(bb, kv, includeMemstoreTS);
    }
    bb.rewind();
    return bb;
  }

  /**
   * Checks whether KeyValues from kvCollection2 are contained in kvCollection1.
   * 
   * The comparison is made without distinguishing MVCC version of the KeyValues
   * 
   * @param kvCollection1
   * @param kvCollection2
   * @return true if KeyValues from kvCollection2 are contained in kvCollection1
   */
  public static boolean containsIgnoreMvccVersion(Collection<? extends Cell> kvCollection1,
      Collection<? extends Cell> kvCollection2) {
    for (Cell kv1 : kvCollection1) {
      boolean found = false;
      for (Cell kv2 : kvCollection2) {
        if (CellComparator.equalsIgnoreMvccVersion(kv1, kv2)) found = true;
      }
      if (!found) return false;
    }
    return true;
  }
  
  public static List<KeyValue> rewindThenToList(final ByteBuffer bb,
      final boolean includesMemstoreTS, final boolean useTags) {
    bb.rewind();
    List<KeyValue> kvs = Lists.newArrayList();
    KeyValue kv = null;
    while (true) {
      kv = KeyValueUtil.nextShallowCopy(bb, includesMemstoreTS, useTags);
      if (kv == null) {
        break;
      }
      kvs.add(kv);
    }
    return kvs;
  }


  /********************* toString ************************************/

  public static String toStringWithPadding(final Collection<? extends KeyValue> kvs,
      final boolean includeMeta) {
    int maxRowStringLength = 0;
    int maxFamilyStringLength = 0;
    int maxQualifierStringLength = 0;
    int maxTimestampLength = 0;
    for (KeyValue kv : kvs) {
      maxRowStringLength = Math.max(maxRowStringLength, getRowString(kv).length());
      maxFamilyStringLength = Math.max(maxFamilyStringLength, getFamilyString(kv).length());
      maxQualifierStringLength = Math.max(maxQualifierStringLength, getQualifierString(kv)
        .length());
      maxTimestampLength = Math.max(maxTimestampLength, Long.valueOf(kv.getTimestamp()).toString()
        .length());
    }
    StringBuilder sb = new StringBuilder();
    for (KeyValue kv : kvs) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      String row = toStringWithPadding(kv, maxRowStringLength, maxFamilyStringLength,
        maxQualifierStringLength, maxTimestampLength, includeMeta);
      sb.append(row);
    }
    return sb.toString();
  }

  protected static String toStringWithPadding(final KeyValue kv, final int maxRowLength,
      int maxFamilyLength, int maxQualifierLength, int maxTimestampLength, boolean includeMeta) {
    String leadingLengths = "";
    String familyLength = kv.getFamilyLength() + " ";
    if (includeMeta) {
      leadingLengths += Strings.padFront(kv.getKeyLength() + "", '0', 4);
      leadingLengths += " ";
      leadingLengths += Strings.padFront(kv.getValueLength() + "", '0', 4);
      leadingLengths += " ";
      leadingLengths += Strings.padFront(kv.getRowLength() + "", '0', 2);
      leadingLengths += " ";
    }
    int spacesAfterRow = maxRowLength - getRowString(kv).length() + 2;
    int spacesAfterFamily = maxFamilyLength - getFamilyString(kv).length() + 2;
    int spacesAfterQualifier = maxQualifierLength - getQualifierString(kv).length() + 1;
    int spacesAfterTimestamp = maxTimestampLength
        - Long.valueOf(kv.getTimestamp()).toString().length() + 1;
    return leadingLengths + getRowString(kv) + Strings.repeat(' ', spacesAfterRow)
        + familyLength + getFamilyString(kv) + Strings.repeat(' ', spacesAfterFamily)
        + getQualifierString(kv) + Strings.repeat(' ', spacesAfterQualifier)
        + getTimestampString(kv) + Strings.repeat(' ', spacesAfterTimestamp)
        + getTypeString(kv) + " " + getValueString(kv);
  }

  protected static String getRowString(final KeyValue kv) {
    return Bytes.toStringBinary(kv.getRow());
  }

  protected static String getFamilyString(final KeyValue kv) {
    return Bytes.toStringBinary(kv.getFamily());
  }

  protected static String getQualifierString(final KeyValue kv) {
    return Bytes.toStringBinary(kv.getQualifier());
  }

  protected static String getTimestampString(final KeyValue kv) {
    return kv.getTimestamp() + "";
  }

  protected static String getTypeString(final KeyValue kv) {
    return KeyValue.Type.codeToType(kv.getType()).toString();
  }

  protected static String getValueString(final KeyValue kv) {
    return Bytes.toStringBinary(kv.getValue());
  }

}
