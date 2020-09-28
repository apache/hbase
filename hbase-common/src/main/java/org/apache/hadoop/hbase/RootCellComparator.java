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

import java.util.Comparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A {@link CellComparatorImpl} for <code>hbase:meta</code> catalog table
 * {@link KeyValue}s.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RootCellComparator extends MetaCellComparator {

  /**
   * A {@link MetaCellComparator} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   */
  public static final RootCellComparator ROOT_COMPARATOR = new RootCellComparator();

  @Override
  public int compareRows(byte[] left, int loffset, int llength, byte[] right, int roffset,
      int rlength) {
    // Rows look like this: .META.,ROW_FROM_META,RID
    //        LOG.info("ROOT " + Bytes.toString(left, loffset, llength) +
    //          "---" + Bytes.toString(right, roffset, rlength));
    final int metalength = TableName.META_TABLE_NAME.getName().length + 1; // '.META.' length
    int lmetaOffsetPlusDelimiter = loffset + metalength;
    int leftFarDelimiter = Bytes
      .searchDelimiterIndexInReverse(left, lmetaOffsetPlusDelimiter, llength - metalength,
        HConstants.DELIMITER);
    int rmetaOffsetPlusDelimiter = roffset + metalength;
    int rightFarDelimiter = Bytes
      .searchDelimiterIndexInReverse(right, rmetaOffsetPlusDelimiter, rlength - metalength,
        HConstants.DELIMITER);
    if (leftFarDelimiter < 0 && rightFarDelimiter >= 0) {
      // Nothing between .META. and regionid.  Its first key.
      return -1;
    } else if (rightFarDelimiter < 0 && leftFarDelimiter >= 0) {
      return 1;
    } else if (leftFarDelimiter < 0 && rightFarDelimiter < 0) {
      return 0;
    }
    int result = MetaCellComparator.META_COMPARATOR
      .compareRows(left, lmetaOffsetPlusDelimiter, leftFarDelimiter - lmetaOffsetPlusDelimiter,
        right, rmetaOffsetPlusDelimiter, rightFarDelimiter - rmetaOffsetPlusDelimiter);
    if (result != 0) {
      return result;
    }
    // Compare last part of row, the rowid.
    leftFarDelimiter++;
    rightFarDelimiter++;
    result = Bytes.compareTo(left, leftFarDelimiter, llength - (leftFarDelimiter - loffset), right,
      rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
    return result;
  }

  @Override
  public Comparator getSimpleComparator() {
    return this;
  }

}
