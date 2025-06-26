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
package org.apache.hadoop.hbase.filter;

import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;

import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * optimize scan range through filter to reduce unnecessary reading of data.
 */
@InterfaceAudience.Private
public final class ScanRangeOptimizer {

  private ScanRangeOptimizer() {}

  public static void optimize(Scan scan) {
    if (!scan.optimizationEnabled()) {
      return;
    }
    if (scan.hasFilter()) {
      byte[] largestPossibleStartRow = findLargestPossibleStartRow(scan.getFilter());
      byte[] smallestPossibleStopRow = findSmallestPossibleStopRow(scan.getFilter());
      if (largestPossibleStartRow != null) {
        if (scan.getStartRow() == null) {
          // always include the startrow. if the startrow is unwanted, it will be filtered
          // out by filters.
          scan.withStartRow(largestPossibleStartRow, /* inclusive= */true);
        } else {
          if (Bytes.compareTo(largestPossibleStartRow, scan.getStartRow()) != 0) {
            scan.withStartRow(largestPossibleStartRow, /* inclusive= */true);
          }
        }
      }
      if (smallestPossibleStopRow != null) {
        if (scan.getStopRow() != null) {
          // always include the stoprow. if the stoprow is unwanted, it will be filtered
          // out by filters.
          scan.withStopRow(smallestPossibleStopRow, /* inclusive= */true);
        } else {
          if (Bytes.compareTo(smallestPossibleStopRow, scan.getStopRow()) != 0) {
            scan.withStopRow(smallestPossibleStopRow, /* inclusive= */true);
          }
        }
      }
    }
  }

  private static byte[] findLargestPossibleStartRow(Filter filter) {
    if (filter instanceof PrefixFilter) {
      return findLargestPossibleStartRowInPrefixFilter((PrefixFilter) filter);
    } else if (filter instanceof RowFilter) {
      return findLargestPossibleStartRowInRowFilter((RowFilter) filter);
    } else if (filter instanceof FilterListWithAND) {
      return findLargestPossibleStartRowInFilterList((FilterListWithAND) filter, Bytes::max);
    } else if (filter instanceof FilterListWithOR) {
      return findLargestPossibleStartRowInFilterList((FilterListWithOR) filter, Bytes::min);
    }
    return null;
  }

  private static byte[] findLargestPossibleStartRowInPrefixFilter(PrefixFilter filter) {
    return filter.getPrefix();
  }

  private static byte[] findLargestPossibleStartRowInRowFilter(RowFilter filter) {
    switch (filter.getCompareOperator()) {
      case GREATER:
      case GREATER_OR_EQUAL:
      case EQUAL:
        return filter.getComparator().getValue();
      default:
        return null;
    }
  }

  private static byte[] findLargestPossibleStartRowInFilterList(
    FilterListBase filterList, Aggregator<byte[]> aggregator) {
    byte[] ret = null;
    byte[] tmp = null;
    for (Filter filter : filterList.getFilters()) {
      if (filter instanceof PrefixFilter) {
        tmp = findLargestPossibleStartRowInPrefixFilter((PrefixFilter) filter);
      } else if (filter instanceof RowFilter) {
        tmp = findLargestPossibleStartRowInRowFilter((RowFilter) filter);
      } else if (filter instanceof FilterListBase) {
        tmp = findLargestPossibleStartRowInFilterList((FilterListBase) filter, aggregator);
      }
      if (tmp == null) {
        continue;
      }
      if (ret == null) {
        ret = tmp;
      } else {
        ret = aggregator.aggregate(ret, tmp);
      }
    }
    return ret;
  }

  private static byte[] findSmallestPossibleStopRow(Filter filter) {
    if (filter instanceof PrefixFilter) {
      return findSmallestPossibleStopRowInPrefixFilter((PrefixFilter) filter);
    } else if (filter instanceof RowFilter) {
      return findSmallestPossibleStopRowInRowFilter((RowFilter) filter);
    } else if (filter instanceof FilterListWithAND) {
      return findSmallestPossibleStopRowInFilterList((FilterListWithAND) filter, Bytes::min);
    } else if (filter instanceof FilterListWithOR) {
      return findSmallestPossibleStopRowInFilterList((FilterListWithOR) filter, Bytes::max);
    }
    return null;
  }

  private static byte[] findSmallestPossibleStopRowInPrefixFilter(PrefixFilter filter) {
    if (filter == null) {
      return null;
    } else {
      return ClientUtil.calculateTheClosestNextRowKeyForPrefix(filter.getPrefix());
    }
  }

  private static byte[] findSmallestPossibleStopRowInRowFilter(RowFilter filter) {
    switch (filter.getCompareOperator()) {
      case LESS:
      case LESS_OR_EQUAL:
      case EQUAL:
        return filter.getComparator().getValue();
      default:
        return null;
    }
  }

  private static byte[] findSmallestPossibleStopRowInFilterList(
    FilterListBase filterList, Aggregator<byte[]> aggregator) {
    byte[] ret = null;
    byte[] tmp = null;
    for (Filter filter : filterList.getFilters()) {
      if (filter instanceof PrefixFilter) {
        tmp = findSmallestPossibleStopRowInPrefixFilter((PrefixFilter) filter);
      } else if (filter instanceof RowFilter) {
        tmp = findSmallestPossibleStopRowInRowFilter((RowFilter) filter);
      } else if (filter instanceof FilterListBase) {
        tmp = findSmallestPossibleStopRowInFilterList((FilterListBase) filter, aggregator);
      }
      if (tmp == null) {
        continue;
      }
      if (ret == null) {
        ret = tmp;
      } else {
        ret = aggregator.aggregate(ret, tmp);
      }
    }
    return ret;
  }

  private interface Aggregator<T> {
    T aggregate(T first, T second);
  }
}
