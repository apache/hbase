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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.primitives.Longs;

/**
 * Compare two HBase cells.  Do not use this method comparing <code>-ROOT-</code> or
 * <code>hbase:meta</code> cells.  Cells from these tables need a specialized comparator, one that
 * takes account of the special formatting of the row where we have commas to delimit table from
 * regionname, from row.  See KeyValue for how it has a special comparator to do hbase:meta cells
 * and yet another for -ROOT-.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="UNKNOWN",
    justification="Findbugs doesn't like the way we are negating the result of a compare in below")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CellComparator implements Comparator<Cell>, Serializable{
  private static final long serialVersionUID = -8760041766259623329L;

  @Override
  public int compare(Cell a, Cell b) {
    return compareStatic(a, b);
  }

  public static int compareStatic(Cell a, Cell b) {
    return compareStatic(a, b, false);
  }
  
  public static int compareStatic(Cell a, Cell b, boolean onlyKey) {
    //row
    int c = Bytes.compareTo(
        a.getRowArray(), a.getRowOffset(), a.getRowLength(),
        b.getRowArray(), b.getRowOffset(), b.getRowLength());
    if (c != 0) return c;

    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    if (a.getFamilyLength() == 0 && a.getTypeByte() == Type.Minimum.getCode()) {
      // a is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (b.getFamilyLength() == 0 && b.getTypeByte() == Type.Minimum.getCode()) {
      return -1;
    }

    //family
    c = Bytes.compareTo(
      a.getFamilyArray(), a.getFamilyOffset(), a.getFamilyLength(),
      b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
    if (c != 0) return c;

    //qualifier
    c = Bytes.compareTo(
        a.getQualifierArray(), a.getQualifierOffset(), a.getQualifierLength(),
        b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
    if (c != 0) return c;

    //timestamp: later sorts first
    c = Longs.compare(b.getTimestamp(), a.getTimestamp());
    if (c != 0) return c;

    //type
    c = (0xff & b.getTypeByte()) - (0xff & a.getTypeByte());
    if (c != 0) return c;

    if (onlyKey) return c;

    //mvccVersion: later sorts first
    return Longs.compare(b.getMvccVersion(), a.getMvccVersion());
  }


  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b){
    return equalsRow(a, b)
        && equalsFamily(a, b)
        && equalsQualifier(a, b)
        && equalsTimestamp(a, b)
        && equalsType(a, b);
  }

  public static boolean equalsRow(Cell a, Cell b){
    return Bytes.equals(
      a.getRowArray(), a.getRowOffset(), a.getRowLength(),
      b.getRowArray(), b.getRowOffset(), b.getRowLength());
  }

  public static boolean equalsFamily(Cell a, Cell b){
    return Bytes.equals(
      a.getFamilyArray(), a.getFamilyOffset(), a.getFamilyLength(),
      b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
  }

  public static boolean equalsQualifier(Cell a, Cell b){
    return Bytes.equals(
      a.getQualifierArray(), a.getQualifierOffset(), a.getQualifierLength(),
      b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
  }

  public static boolean equalsTimestamp(Cell a, Cell b){
    return a.getTimestamp() == b.getTimestamp();
  }

  public static boolean equalsType(Cell a, Cell b){
    return a.getTypeByte() == b.getTypeByte();
  }


  /********************* hashCode ************************/

  /**
   * Returns a hash code that is always the same for two Cells having a matching equals(..) result.
   */
  public static int hashCode(Cell cell){
    if (cell == null) {// return 0 for empty Cell
      return 0;
    }
    int hash = calculateHashForKeyValue(cell);
    hash = 31 * hash + (int)cell.getMvccVersion();
    return hash;
  }

  /**
   * Returns a hash code that is always the same for two Cells having a matching
   * equals(..) result. Note : Ignore mvcc while calculating the hashcode
   * 
   * @param cell
   * @return hashCode
   */
  public static int hashCodeIgnoreMvcc(Cell cell) {
    if (cell == null) {// return 0 for empty Cell
      return 0;
    }
    int hash = calculateHashForKeyValue(cell);
    return hash;
  }

  private static int calculateHashForKeyValue(Cell cell) {
    //pre-calculate the 3 hashes made of byte ranges
    int rowHash = Bytes.hashCode(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    int familyHash =
      Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    int qualifierHash = Bytes.hashCode(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength());

    //combine the 6 sub-hashes
    int hash = 31 * rowHash + familyHash;
    hash = 31 * hash + qualifierHash;
    hash = 31 * hash + (int)cell.getTimestamp();
    hash = 31 * hash + cell.getTypeByte();
    return hash;
  }


  /******************** lengths *************************/

  public static boolean areKeyLengthsEqual(Cell a, Cell b) {
    return a.getRowLength() == b.getRowLength()
        && a.getFamilyLength() == b.getFamilyLength()
        && a.getQualifierLength() == b.getQualifierLength();
  }

  public static boolean areRowLengthsEqual(Cell a, Cell b) {
    return a.getRowLength() == b.getRowLength();
  }


  /***************** special cases ****************************/

  /**
   * special case for KeyValue.equals
   */
  private static int compareStaticIgnoreMvccVersion(Cell a, Cell b) {
    //row
    int c = Bytes.compareTo(
        a.getRowArray(), a.getRowOffset(), a.getRowLength(),
        b.getRowArray(), b.getRowOffset(), b.getRowLength());
    if (c != 0) return c;

    //family
    c = Bytes.compareTo(
      a.getFamilyArray(), a.getFamilyOffset(), a.getFamilyLength(),
      b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
    if (c != 0) return c;

    //qualifier
    c = Bytes.compareTo(
        a.getQualifierArray(), a.getQualifierOffset(), a.getQualifierLength(),
        b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
    if (c != 0) return c;

    //timestamp: later sorts first
    c = Longs.compare(b.getTimestamp(), a.getTimestamp());
    if (c != 0) return c;

    //type
    c = (0xff & b.getTypeByte()) - (0xff & a.getTypeByte());
    return c;
  }

  /**
   * special case for KeyValue.equals
   */
  public static boolean equalsIgnoreMvccVersion(Cell a, Cell b){
    return 0 == compareStaticIgnoreMvccVersion(a, b);
  }

}
