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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionAsTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Similar to {@link HConstants} but for tests. Also provides some simple static utility functions
 * to generate test data.
 */
public class HTestConst {

  private HTestConst() {
  }

  public static final String DEFAULT_TABLE_STR = "MyTestTable";
  public static final byte[] DEFAULT_TABLE_BYTES = Bytes.toBytes(DEFAULT_TABLE_STR);
  public static final TableName DEFAULT_TABLE = TableName.valueOf(DEFAULT_TABLE_BYTES);

  public static final String DEFAULT_CF_STR = "MyDefaultCF";
  public static final byte[] DEFAULT_CF_BYTES = Bytes.toBytes(DEFAULT_CF_STR);

  public static final Set<String> DEFAULT_CF_STR_SET =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(new String[] { DEFAULT_CF_STR })));

  public static final String DEFAULT_ROW_STR = "MyTestRow";
  public static final byte[] DEFAULT_ROW_BYTES = Bytes.toBytes(DEFAULT_ROW_STR);

  public static final String DEFAULT_QUALIFIER_STR = "MyColumnQualifier";
  public static final byte[] DEFAULT_QUALIFIER_BYTES = Bytes.toBytes(DEFAULT_QUALIFIER_STR);

  public static String DEFAULT_VALUE_STR = "MyTestValue";
  public static byte[] DEFAULT_VALUE_BYTES = Bytes.toBytes(DEFAULT_VALUE_STR);

  private static final char FIRST_CHAR = 'a';
  private static final char LAST_CHAR = 'z';
  private static final byte[] START_KEY_BYTES = { FIRST_CHAR, FIRST_CHAR, FIRST_CHAR };

  /**
   * Generate the given number of unique byte sequences by appending numeric suffixes (ASCII
   * representations of decimal numbers).
   */
  public static byte[][] makeNAscii(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      byte[] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

  /**
   * Add content to region <code>r</code> on the passed column <code>column</code>. Adds data of the
   * from 'aaa', 'aab', etc where key and value are the same.
   * @return count of what we added.
   */
  public static long addContent(final Region r, final byte[] columnFamily, final byte[] column)
    throws IOException {
    byte[] startKey = r.getRegionInfo().getStartKey();
    byte[] endKey = r.getRegionInfo().getEndKey();
    byte[] startKeyBytes = startKey;
    if (startKeyBytes == null || startKeyBytes.length == 0) {
      startKeyBytes = START_KEY_BYTES;
    }
    return addContent(new RegionAsTable(r), Bytes.toString(columnFamily), Bytes.toString(column),
      startKeyBytes, endKey, -1);
  }

  public static long addContent(final Region r, final byte[] columnFamily) throws IOException {
    return addContent(r, columnFamily, null);
  }

  /**
   * Add content to region <code>r</code> on the passed column <code>column</code>. Adds data of the
   * from 'aaa', 'aab', etc where key and value are the same.
   * @return count of what we added.
   */
  public static long addContent(Table updater, String columnFamily) throws IOException {
    return addContent(updater, columnFamily, START_KEY_BYTES, null);
  }

  public static long addContent(Table updater, String family, String column) throws IOException {
    return addContent(updater, family, column, START_KEY_BYTES, null);
  }

  /**
   * Add content to region <code>r</code> on the passed column <code>column</code>. Adds data of the
   * from 'aaa', 'aab', etc where key and value are the same.
   * @return count of what we added.
   */
  public static long addContent(Table updater, String columnFamily, byte[] startKeyBytes,
    byte[] endKey) throws IOException {
    return addContent(updater, columnFamily, null, startKeyBytes, endKey, -1);
  }

  public static long addContent(Table updater, String family, String column, byte[] startKeyBytes,
    byte[] endKey) throws IOException {
    return addContent(updater, family, column, startKeyBytes, endKey, -1);
  }

  /**
   * Add content to region <code>r</code> on the passed column <code>column</code>. Adds data of the
   * from 'aaa', 'aab', etc where key and value are the same.
   * @return count of what we added.
   */
  public static long addContent(Table updater, String columnFamily, String column,
    byte[] startKeyBytes, byte[] endKey, long ts) throws IOException {
    long count = 0;
    // Add rows of three characters. The first character starts with the
    // 'a' character and runs up to 'z'. Per first character, we run the
    // second character over same range. And same for the third so rows
    // (and values) look like this: 'aaa', 'aab', 'aac', etc.
    char secondCharStart = (char) startKeyBytes[1];
    char thirdCharStart = (char) startKeyBytes[2];
    EXIT: for (char c = (char) startKeyBytes[0]; c <= LAST_CHAR; c++) {
      for (char d = secondCharStart; d <= LAST_CHAR; d++) {
        for (char e = thirdCharStart; e <= LAST_CHAR; e++) {
          byte[] t = new byte[] { (byte) c, (byte) d, (byte) e };
          if (endKey != null && endKey.length > 0 && Bytes.compareTo(endKey, t) <= 0) {
            break EXIT;
          }
          Put put;
          if (ts != -1) {
            put = new Put(t, ts);
          } else {
            put = new Put(t);
          }
          StringBuilder sb = new StringBuilder();
          if (column != null && column.contains(":")) {
            sb.append(column);
          } else {
            if (columnFamily != null) {
              sb.append(columnFamily);
              if (!columnFamily.endsWith(":")) {
                sb.append(":");
              }
              if (column != null) {
                sb.append(column);
              }
            }
          }
          byte[][] split = CellUtil.parseColumn(Bytes.toBytes(sb.toString()));
          if (split.length == 1) {
            byte[] qualifier = new byte[0];
            put.addColumn(split[0], qualifier, t);
          } else {
            put.addColumn(split[0], split[1], t);
          }
          put.setDurability(Durability.SKIP_WAL);
          updater.put(put);
          count++;
        }
        // Set start character back to FIRST_CHAR after we've done first loop.
        thirdCharStart = FIRST_CHAR;
      }
      secondCharStart = FIRST_CHAR;
    }
    return count;
  }
}
