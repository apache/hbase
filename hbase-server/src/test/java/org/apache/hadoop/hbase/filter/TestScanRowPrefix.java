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
package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test if Scan.setRowPrefixFilter works as intended.
 */
@Category({FilterTests.class, MediumTests.class})
public class TestScanRowPrefix extends FilterTestingCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScanRowPrefix.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestScanRowPrefix.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testPrefixScanning() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    createTable(tableName,"F");
    Table table = openTable(tableName);

    /**
     * Note that about half of these tests were relevant for an different implementation approach
     * of setRowPrefixFilter. These test cases have been retained to ensure that also the
     * edge cases found there are still covered.
     */

    final byte[][] rowIds = {
        {(byte) 0x11},                                                      //  0
        {(byte) 0x12},                                                      //  1
        {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFE},               //  2
        {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF},               //  3
        {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF, (byte) 0x00},  //  4
        {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF, (byte) 0x01},  //  5
        {(byte) 0x12, (byte) 0x24},                                         //  6
        {(byte) 0x12, (byte) 0x24, (byte) 0x00},                            //  7
        {(byte) 0x12, (byte) 0x24, (byte) 0x00, (byte) 0x00},               //  8
        {(byte) 0x12, (byte) 0x25},                                         //  9
        {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},  // 10
    };
    for (byte[] rowId: rowIds) {
      Put p = new Put(rowId);
      // Use the rowId as the column qualifier
      p.addColumn("F".getBytes(), rowId, "Dummy value".getBytes());
      table.put(p);
    }

    byte[] prefix0 = {};
    List<byte[]> expected0 = new ArrayList<>(16);
    expected0.addAll(Arrays.asList(rowIds)); // Expect all rows

    byte[] prefix1 = {(byte) 0x12, (byte) 0x23};
    List<byte[]> expected1 = new ArrayList<>(16);
    expected1.add(rowIds[2]);
    expected1.add(rowIds[3]);
    expected1.add(rowIds[4]);
    expected1.add(rowIds[5]);

    byte[] prefix2 = {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF};
    List<byte[]> expected2 = new ArrayList<>();
    expected2.add(rowIds[3]);
    expected2.add(rowIds[4]);
    expected2.add(rowIds[5]);

    byte[] prefix3 = {(byte) 0x12, (byte) 0x24};
    List<byte[]> expected3 = new ArrayList<>();
    expected3.add(rowIds[6]);
    expected3.add(rowIds[7]);
    expected3.add(rowIds[8]);

    byte[] prefix4 = {(byte) 0xFF, (byte) 0xFF};
    List<byte[]> expected4 = new ArrayList<>();
    expected4.add(rowIds[10]);

    // ========
    // PREFIX 0
    Scan scan = new Scan();
    scan.setRowPrefixFilter(prefix0);
    verifyScanResult(table, scan, expected0, "Scan empty prefix failed");

    // ========
    // PREFIX 1
    scan = new Scan();
    scan.setRowPrefixFilter(prefix1);
    verifyScanResult(table, scan, expected1, "Scan normal prefix failed");

    scan.setRowPrefixFilter(null);
    verifyScanResult(table, scan, expected0, "Scan after prefix reset failed");

    scan = new Scan();
    scan.setFilter(new ColumnPrefixFilter(prefix1));
    verifyScanResult(table, scan, expected1, "Double check on column prefix failed");

    // ========
    // PREFIX 2
    scan = new Scan();
    scan.setRowPrefixFilter(prefix2);
    verifyScanResult(table, scan, expected2, "Scan edge 0xFF prefix failed");

    scan.setRowPrefixFilter(null);
    verifyScanResult(table, scan, expected0, "Scan after prefix reset failed");

    scan = new Scan();
    scan.setFilter(new ColumnPrefixFilter(prefix2));
    verifyScanResult(table, scan, expected2, "Double check on column prefix failed");

    // ========
    // PREFIX 3
    scan = new Scan();
    scan.setRowPrefixFilter(prefix3);
    verifyScanResult(table, scan, expected3, "Scan normal with 0x00 ends failed");

    scan.setRowPrefixFilter(null);
    verifyScanResult(table, scan, expected0, "Scan after prefix reset failed");

    scan = new Scan();
    scan.setFilter(new ColumnPrefixFilter(prefix3));
    verifyScanResult(table, scan, expected3, "Double check on column prefix failed");

    // ========
    // PREFIX 4
    scan = new Scan();
    scan.setRowPrefixFilter(prefix4);
    verifyScanResult(table, scan, expected4, "Scan end prefix failed");

    scan.setRowPrefixFilter(null);
    verifyScanResult(table, scan, expected0, "Scan after prefix reset failed");

    scan = new Scan();
    scan.setFilter(new ColumnPrefixFilter(prefix4));
    verifyScanResult(table, scan, expected4, "Double check on column prefix failed");

    // ========
    // COMBINED
    // Prefix + Filter
    scan = new Scan();
    scan.setRowPrefixFilter(prefix1);
    verifyScanResult(table, scan, expected1, "Prefix filter failed");

    scan.setFilter(new ColumnPrefixFilter(prefix2));
    verifyScanResult(table, scan, expected2, "Combined Prefix + Filter failed");

    scan.setRowPrefixFilter(null);
    verifyScanResult(table, scan, expected2, "Combined Prefix + Filter; removing Prefix failed");

    scan.setFilter(null);
    verifyScanResult(table, scan, expected0, "Scan after Filter reset failed");

    // ========
    // Reversed: Filter + Prefix
    scan = new Scan();
    scan.setFilter(new ColumnPrefixFilter(prefix2));
    verifyScanResult(table, scan, expected2, "Test filter failed");

    scan.setRowPrefixFilter(prefix1);
    verifyScanResult(table, scan, expected2, "Combined Filter + Prefix failed");

    scan.setFilter(null);
    verifyScanResult(table, scan, expected1, "Combined Filter + Prefix ; removing Filter failed");

    scan.setRowPrefixFilter(null);
    verifyScanResult(table, scan, expected0, "Scan after prefix reset failed");
  }

  private void verifyScanResult(Table table, Scan scan, List<byte[]> expectedKeys, String message) {
    List<byte[]> actualKeys = new ArrayList<>();
    try {
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        actualKeys.add(result.getRow());
      }

      String fullMessage = message;
      if (LOG.isDebugEnabled()) {
        fullMessage = message + "\n" + tableOfTwoListsOfByteArrays(
                "Expected", expectedKeys,
                "Actual  ", actualKeys);
      }

      Assert.assertArrayEquals(
              fullMessage,
              expectedKeys.toArray(),
              actualKeys.toArray());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private String printMultiple(char letter, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(letter);
    }
    return sb.toString();
  }

  private String tableOfTwoListsOfByteArrays(
          String label1, List<byte[]> listOfBytes1,
          String label2, List<byte[]> listOfBytes2) {
    int margin1 = calculateWidth(label1, listOfBytes1);
    int margin2 = calculateWidth(label2, listOfBytes2);

    StringBuilder sb = new StringBuilder(512);
    String separator = '+' + printMultiple('-', margin1 + margin2 + 5) + '+' + '\n';
    sb.append(separator);
    sb.append(printLine(label1, margin1, label2, margin2)).append('\n');
    sb.append(separator);
    int maxLength = Math.max(listOfBytes1.size(), listOfBytes2.size());
    for (int offset = 0; offset < maxLength; offset++) {
      String value1 = getStringFromList(listOfBytes1, offset);
      String value2 = getStringFromList(listOfBytes2, offset);
      sb.append(printLine(value1, margin1, value2, margin2)).append('\n');
    }
    sb.append(separator).append('\n');
    return sb.toString();
  }

  private String printLine(String leftValue, int leftWidth1, String rightValue, int rightWidth) {
    return "| " +
           leftValue  + printMultiple(' ', leftWidth1 - leftValue.length() ) +
           " | " +
           rightValue + printMultiple(' ', rightWidth - rightValue.length()) +
           " |";
  }

  private int calculateWidth(String label1, List<byte[]> listOfBytes1) {
    int longestList1 = label1.length();
    for (byte[] value : listOfBytes1) {
      longestList1 = Math.max(value.length * 2, longestList1);
    }
    return longestList1 + 5;
  }

  private String getStringFromList(List<byte[]> listOfBytes, int offset) {
    String value1;
    if (listOfBytes.size() > offset) {
      value1 = Hex.encodeHexString(listOfBytes.get(offset));
    } else {
      value1 = "<missing>";
    }
    return value1;
  }

}
