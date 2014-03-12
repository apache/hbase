/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.loadtest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class DataGenerator {
  private static final Log LOG = LogFactory.getLog(HBaseUtils.class);

  static Random random_ = new Random();
  /* one byte fill pattern */
  public static final String fill1B_ = "-";
  /* 64 byte fill pattern */
  public static final String fill64B_ =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789  ";
  /* alternate 64 byte fill pattern */
  public static final String fill64BAlt_ =
      "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789+-";
  /* 1K fill pattern */
  public static final String fill1K_ =
      fill64BAlt_ + fill64BAlt_ + fill64BAlt_ + fill64BAlt_ + fill64BAlt_
          + fill64BAlt_ + fill64BAlt_ + fill64BAlt_ + fill64BAlt_ + fill64BAlt_
          + fill64BAlt_ + fill64BAlt_ + fill64BAlt_ + fill64BAlt_ + fill64BAlt_
          + fill64BAlt_;

  static public String paddedNumber(long key) {
    // left-pad key with zeroes to 10 decimal places.
    String paddedKey = String.format("%010d", key);
    // flip the key to randomize
    return (new StringBuffer(paddedKey)).reverse().toString();
  }

  public static byte[] getDataInSize(long key, int dataSize) {
    StringBuilder sb = new StringBuilder();

    // write the key first
    int sizeLeft = dataSize;
    String keyAsString = DataGenerator.paddedNumber(key);
    sb.append(keyAsString);
    sizeLeft -= keyAsString.length();

    for (int i = 0; i < sizeLeft / 1024; ++i) {
      sb.append(fill1K_);
    }
    sizeLeft = sizeLeft % 1024;
    for (int i = 0; i < sizeLeft / 64; ++i) {
      sb.append(fill64B_);
    }
    sizeLeft = sizeLeft % 64;
    for (int i = 0; i < dataSize % 64; ++i) {
      sb.append(fill1B_);
    }

    return sb.toString().getBytes();
  }

  public static int getValueLength(int minDataSize, int maxDataSize, long rowKey) {
    return Math.abs(minDataSize
        + hash((int) rowKey, (maxDataSize - minDataSize + 1)));
  }

  public static int getNumberOfColumns(long minColumnsPerKey,
      long maxColumnsPerKey, long rowKey) {
    return Math.abs((int) minColumnsPerKey
        + hash((int) rowKey, (int) (maxColumnsPerKey - minColumnsPerKey + 1)));
  }

  public static int hash(int key, int mod) {
    return key % mod;
  }

  public static byte[] getDataInSize(long row, int column, int timestamp,
      int minDataSize, int maxDataSize) {
    int dataSize = getValueLength(minDataSize, maxDataSize, row);
    return getDataInSize(row * column * timestamp, dataSize);
  }

  public static TreeSet<KeyValue> getSortedResultSet(long rowID,
      ColumnFamilyProperties familyProperty) {
    TreeSet<KeyValue> kvSet =
        new TreeSet<KeyValue>(new KeyValue.KVComparator());
    // byte[] row = DataGenerator.paddedKey(rowKey).getBytes();
    byte[] row = RegionSplitter.getHBaseKeyFromRowID(rowID);
    int numColumns =
        getNumberOfColumns(familyProperty.minColsPerKey,
            familyProperty.maxColsPerKey, rowID);
    byte[] family = Bytes.toBytes(familyProperty.familyName);
    for (int colIndex = 0; colIndex <= numColumns; ++colIndex) {
      byte[] column = (DataGenerator.paddedNumber(colIndex)).getBytes();
      for (int timestamp = familyProperty.startTimestamp; timestamp <= familyProperty.endTimestamp; timestamp++) {
        byte[] value =
            getDataInSize(rowID, colIndex, timestamp,
                familyProperty.minColDataSize, familyProperty.maxColDataSize);
        KeyValue kv = new KeyValue(row, family, column, timestamp, value);
        kvSet.add(kv);
      }
    }
    return kvSet;
  }

  /**
   * Returns a set containing keys for the passed row based on the information
   * in familyProperties. This is a slightly redundant form of the above
   * function but is required for efficiency.
   * 
   * @param rowID
   * @param familyProperties
   * @return
   */
  public static Put getPut(long rowID, ColumnFamilyProperties[] familyProperties) {
    // Put put = new Put(DataGenerator.paddedKey(rowKeyID).getBytes());
    byte[] row = RegionSplitter.getHBaseKeyFromRowID(rowID);
    Put put = new Put(row);
    for (ColumnFamilyProperties familyProperty : familyProperties) {
      int numColumns =
          getNumberOfColumns(familyProperty.minColsPerKey,
              familyProperty.maxColsPerKey, rowID);
      byte[] family = Bytes.toBytes(familyProperty.familyName);
      for (int colIndex = 0; colIndex <= numColumns; ++colIndex) {
        byte[] column = (DataGenerator.paddedNumber(colIndex)).getBytes();
        for (int timestamp = familyProperty.startTimestamp; timestamp <= familyProperty.endTimestamp; timestamp++) {
          byte[] value =
              getDataInSize(rowID, colIndex, timestamp,
                  familyProperty.minColDataSize, familyProperty.maxColDataSize);
          put.add(family, column, timestamp, value);
        }
      }
    }
    return put;
  }

  public static TreeSet<KeyValue> filterAndVersioningForSingleRowFamily(
      TreeSet<KeyValue> kvSet, Filter filter, int maxVersions) {
    int currentVersions = 0;
    byte[] prevColumn = null;
    TreeSet<KeyValue> filteredSet =
        new TreeSet<KeyValue>(new KeyValue.KVComparator());
    for (KeyValue kv : kvSet) {
      if (filter == null
          || filter.filterKeyValue(kv, null).equals(Filter.ReturnCode.INCLUDE)) {
        byte[] column = kv.getQualifier();
        if (Bytes.equals(prevColumn, column)) {
          currentVersions++;
        } else {
          prevColumn = column;
          currentVersions = 1;
        }
        if (currentVersions <= maxVersions) {
          filteredSet.add(kv);
        }
      }
    }
    return filteredSet;
  }

  public static TimestampsFilter getTimestampFilter(long rowKey,
      ColumnFamilyProperties familyProperty) {
    double timestampSelectionFrequency = 0.01;
    List<Long> timestamps = new ArrayList<Long>();
    for (long timestamp = familyProperty.startTimestamp; timestamp <= familyProperty.endTimestamp; timestamp++) {
      if (timestampSelectionFrequency >= Math.random()) {
        timestamps.add(timestamp);
      }
    }
    TimestampsFilter timestampsFilter = new TimestampsFilter(timestamps);
    return timestampsFilter;
  }

  public static Filter getColumnPrefixFilter(long rowKey,
      ColumnFamilyProperties familyProperty) {
    int randomNumber = (int) (Math.random() * 20);
    byte[] prefix = Bytes.toBytes(randomNumber);
    return new ColumnPrefixFilter(prefix);
  }

  public static Filter getFilter(long rowKey,
      ColumnFamilyProperties familyProperty) {
    if (familyProperty.filterType == null
        || familyProperty.filterType.equalsIgnoreCase("None")) {
      return null;
    } else if (familyProperty.filterType.equalsIgnoreCase("Timestamps")) {
      return getTimestampFilter(rowKey, familyProperty);
    } else if (familyProperty.filterType.equalsIgnoreCase("ColumnPrefix")) {
      return getColumnPrefixFilter(rowKey, familyProperty);
    } else {
      LOG.info("FilterType " + familyProperty.filterType + " not recognized!"
          + "Currently supported filter types are 'Timestamps' and 'None'");
      return null;
    }
  }
}
