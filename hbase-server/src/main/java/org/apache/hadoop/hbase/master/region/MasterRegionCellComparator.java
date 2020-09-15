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
package org.apache.hadoop.hbase.master.region;

import java.util.Comparator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Cell comparator implementation for master local region.
 * <p/>
 * In general, for catalog family, we need to use {@link MetaCellComparator} while for other
 * families, we need to use {@link CellComparatorImpl}.
 * <p/>
 * The trick here is to check the row key format, if it is start with 'hbase:meta', we will use
 * {@link MetaCellComparator}, otherwise we will use {@link CellComparatorImpl}.
 */
@InterfaceAudience.Private
public class MasterRegionCellComparator extends CellComparatorImpl {

  /**
   * A {@link MasterRegionCellComparator} for {@link MasterRegion} {@link Cell}s.
   */
  public static final MasterRegionCellComparator MASTER_REGION_COMPARATOR =
    new MasterRegionCellComparator();

  private static final byte[] CATALOG_ROW_PREFIX = TableName.META_TABLE_NAME.getName();

  private boolean isCatalogRow(Cell c) {
    return PrivateCellUtil.rowsStartWith(c, CATALOG_ROW_PREFIX);
  }

  private boolean isCatalogRow(byte[] row, int off, int len) {
    if (len < CATALOG_ROW_PREFIX.length) {
      return false;
    }
    return Bytes.equals(row, off, CATALOG_ROW_PREFIX.length, CATALOG_ROW_PREFIX, 0,
      CATALOG_ROW_PREFIX.length);
  }

  @Override
  public int compare(Cell a, Cell b, boolean ignoreSequenceid) {
    if (isCatalogRow(a) || isCatalogRow(b)) {
      return MetaCellComparator.META_COMPARATOR.compare(a, b, ignoreSequenceid);
    } else {
      return super.compare(a, b, ignoreSequenceid);
    }
  }

  @Override
  public int compareRows(Cell left, Cell right) {
    if (isCatalogRow(left) || isCatalogRow(right)) {
      return MetaCellComparator.META_COMPARATOR.compareRows(left, right);
    } else {
      return super.compareRows(left, right);
    }
  }

  @Override
  public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
    if (isCatalogRow(left) || isCatalogRow(right, roffset, rlength)) {
      return MetaCellComparator.META_COMPARATOR.compareRows(left, right, roffset, rlength);
    } else {
      return super.compareRows(left, right, roffset, rlength);
    }
  }

  @Override
  public Comparator getSimpleComparator() {
    return this;
  }
}
