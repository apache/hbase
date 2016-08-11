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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;


/**
 * The unit of storage in HBase consisting of the following fields:
 * <br>
 * <pre>
 * 1) row
 * 2) column family
 * 3) column qualifier
 * 4) timestamp
 * 5) type
 * 6) MVCC version
 * 7) value
 * </pre>
 * <p>
 * Uniqueness is determined by the combination of row, column family, column qualifier,
 * timestamp, and type.
 * </p>
 * <p>
 * The natural comparator will perform a bitwise comparison on row, column family, and column
 * qualifier. Less intuitively, it will then treat the greater timestamp as the lesser value with
 * the goal of sorting newer cells first.
 * </p>
 * <p>
 * Cell implements Comparable&lt;Cell&gt; which is only meaningful when
 * comparing to other keys in the
 * same table. It uses CellComparator which does not work on the -ROOT- and hbase:meta tables.
 * </p>
 * <p>
 * In the future, we may consider adding a boolean isOnHeap() method and a getValueBuffer() method
 * that can be used to pass a value directly from an off-heap ByteBuffer to the network without
 * copying into an on-heap byte[].
 * </p>
 * <p>
 * Historic note: the original Cell implementation (KeyValue) requires that all fields be encoded as
 * consecutive bytes in the same byte[], whereas this interface allows fields to reside in separate
 * byte[]'s.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Cell {

  //1) Row

  /**
   * Contiguous raw bytes that may start at any index in the containing array. Max length is
   * Short.MAX_VALUE which is 32,767 bytes.
   * @return The array containing the row bytes.
   */
  byte[] getRowArray();

  /**
   * @return Array index of first row byte
   */
  int getRowOffset();

  /**
   * @return Number of row bytes. Must be &lt; rowArray.length - offset.
   */
  short getRowLength();


  //2) Family

  /**
   * Contiguous bytes composed of legal HDFS filename characters which may start at any index in the
   * containing array. Max length is Byte.MAX_VALUE, which is 127 bytes.
   * @return the array containing the family bytes.
   */
  byte[] getFamilyArray();

  /**
   * @return Array index of first family byte
   */
  int getFamilyOffset();

  /**
   * @return Number of family bytes.  Must be &lt; familyArray.length - offset.
   */
  byte getFamilyLength();


  //3) Qualifier

  /**
   * Contiguous raw bytes that may start at any index in the containing array.
   * @return The array containing the qualifier bytes.
   */
  byte[] getQualifierArray();

  /**
   * @return Array index of first qualifier byte
   */
  int getQualifierOffset();

  /**
   * @return Number of qualifier bytes.  Must be &lt; qualifierArray.length - offset.
   */
  int getQualifierLength();


  //4) Timestamp

  /**
   * @return Long value representing time at which this cell was "Put" into the row.  Typically
   * represents the time of insertion, but can be any value from 0 to Long.MAX_VALUE.
   */
  long getTimestamp();


  //5) Type

  /**
   * @return The byte representation of the KeyValue.TYPE of this cell: one of Put, Delete, etc
   */
  byte getTypeByte();


  //6) SequenceId

  /**
   * A region-specific unique monotonically increasing sequence ID given to each Cell. It always
   * exists for cells in the memstore but is not retained forever. It will be kept for
   * {@link HConstants#KEEP_SEQID_PERIOD} days, but generally becomes irrelevant after the cell's
   * row is no longer involved in any operations that require strict consistency.
   * @return seqId (always &gt; 0 if exists), or 0 if it no longer exists
   */
  long getSequenceId();

  //7) Value

  /**
   * Contiguous raw bytes that may start at any index in the containing array. Max length is
   * Integer.MAX_VALUE which is 2,147,483,647 bytes.
   * @return The array containing the value bytes.
   */
  byte[] getValueArray();

  /**
   * @return Array index of first value byte
   */
  int getValueOffset();

  /**
   * @return Number of value bytes.  Must be &lt; valueArray.length - offset.
   */
  int getValueLength();

  /**
   * @return the tags byte array
   */
  byte[] getTagsArray();

  /**
   * @return the first offset where the tags start in the Cell
   */
  int getTagsOffset();

  /**
   * @return the total length of the tags in the Cell.
   */
  int getTagsLength();
}
