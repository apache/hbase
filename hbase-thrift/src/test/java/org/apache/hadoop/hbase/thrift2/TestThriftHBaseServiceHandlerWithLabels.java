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
package org.apache.hadoop.hbase.thrift2;

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TAuthorization;
import org.apache.hadoop.hbase.thrift2.generated.TCellVisibility;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ClientTests.class, MediumTests.class})
public class TestThriftHBaseServiceHandlerWithLabels {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftHBaseServiceHandlerWithLabels.class);

  private static final Logger LOG = LoggerFactory
    .getLogger(TestThriftHBaseServiceHandlerWithLabels.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  // Static names for tables, columns, rows, and values
  private static byte[] tableAname = Bytes.toBytes("tableA");
  private static byte[] familyAname = Bytes.toBytes("familyA");
  private static byte[] familyBname = Bytes.toBytes("familyB");
  private static byte[] qualifierAname = Bytes.toBytes("qualifierA");
  private static byte[] qualifierBname = Bytes.toBytes("qualifierB");
  private static byte[] valueAname = Bytes.toBytes("valueA");
  private static byte[] valueBname = Bytes.toBytes("valueB");
  private static HColumnDescriptor[] families = new HColumnDescriptor[] {
      new HColumnDescriptor(familyAname).setMaxVersions(3),
      new HColumnDescriptor(familyBname).setMaxVersions(2) };

  private final static String TOPSECRET = "topsecret";
  private final static String PUBLIC = "public";
  private final static String PRIVATE = "private";
  private final static String CONFIDENTIAL = "confidential";
  private final static String SECRET = "secret";
  private static User SUPERUSER;

  private static Configuration conf;

  public void assertTColumnValuesEqual(List<TColumnValue> columnValuesA,
    List<TColumnValue> columnValuesB) {
    assertEquals(columnValuesA.size(), columnValuesB.size());
    Comparator<TColumnValue> comparator = new Comparator<TColumnValue>() {
      @Override
      public int compare(TColumnValue o1, TColumnValue o2) {
        return Bytes.compareTo(Bytes.add(o1.getFamily(), o1.getQualifier()),
            Bytes.add(o2.getFamily(), o2.getQualifier()));
      }
    };
    Collections.sort(columnValuesA, comparator);
    Collections.sort(columnValuesB, comparator);

    for (int i = 0; i < columnValuesA.size(); i++) {
      TColumnValue a = columnValuesA.get(i);
      TColumnValue b = columnValuesB.get(i);
      assertArrayEquals(a.getFamily(), b.getFamily());
      assertArrayEquals(a.getQualifier(), b.getQualifier());
      assertArrayEquals(a.getValue(), b.getValue());
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {

    SUPERUSER = User.createUserForTesting(conf, "admin",
        new String[] { "supergroup" });
    conf = UTIL.getConfiguration();
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS,
        SimpleScanLabelGenerator.class, ScanLabelGenerator.class);
    conf.set("hbase.superuser", SUPERUSER.getShortName());
    VisibilityTestUtil.enableVisiblityLabels(conf);
    UTIL.startMiniCluster(1);
    // Wait for the labels table to become available
    UTIL.waitTableEnabled(VisibilityConstants.LABELS_TABLE_NAME.getName(), 50000);
    createLabels();
    Admin admin = UTIL.getAdmin();
    HTableDescriptor tableDescriptor = new HTableDescriptor(
        TableName.valueOf(tableAname));
    for (HColumnDescriptor family : families) {
      tableDescriptor.addFamily(family);
    }
    admin.createTable(tableDescriptor);
    admin.close();
    setAuths();
  }

  private static void createLabels() throws IOException, InterruptedException {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, CONFIDENTIAL, PRIVATE, PUBLIC, TOPSECRET };
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.addLabels(conn, labels);
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  private static void setAuths() throws IOException {
    String[] labels = { SECRET, CONFIDENTIAL, PRIVATE, PUBLIC, TOPSECRET };
    try {
      VisibilityClient.setAuths(UTIL.getConnection(), labels, User.getCurrent().getShortName());
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {

  }

  private ThriftHBaseServiceHandler createHandler() throws IOException {
    return new ThriftHBaseServiceHandler(conf, UserProvider.instantiate(conf));
  }

  @Test
  public void testScanWithVisibilityLabels() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname),
        wrap(qualifierAname), wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(Bytes.toBytes("testScan" + i)), columnValues);
      if (i == 5) {
        put.setCellVisibility(new TCellVisibility().setExpression(PUBLIC));
      } else {
        put.setCellVisibility(new TCellVisibility().setExpression("(" + SECRET
            + "|" + CONFIDENTIAL + ")" + "&" + "!" + TOPSECRET));
      }
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testScan"));
    scan.setStopRow(Bytes.toBytes("testScan\uffff"));

    TAuthorization tauth = new TAuthorization();
    List<String> labels = new ArrayList<>(2);
    labels.add(SECRET);
    labels.add(PRIVATE);
    tauth.setLabels(labels);
    scan.setAuthorizations(tauth);
    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(9, results.size());
    Assert.assertFalse(Bytes.equals(results.get(5).getRow(), Bytes.toBytes("testScan" + 5)));
    for (int i = 0; i < 9; i++) {
      if (i < 5) {
        assertArrayEquals(Bytes.toBytes("testScan" + i), results.get(i).getRow());
      } else if (i == 5) {
        continue;
      } else {
        assertArrayEquals(Bytes.toBytes("testScan" + (i + 1)), results.get(i)
            .getRow());
      }
    }

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 9);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 9);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testGetScannerResultsWithAuthorizations() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname),
        wrap(qualifierAname), wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < 20; i++) {
      TPut put = new TPut(
          wrap(Bytes.toBytes("testGetScannerResults" + pad(i, (byte) 2))), columnValues);
      if (i == 3) {
        put.setCellVisibility(new TCellVisibility().setExpression(PUBLIC));
      } else {
        put.setCellVisibility(new TCellVisibility().setExpression("(" + SECRET
            + "|" + CONFIDENTIAL + ")" + "&" + "!" + TOPSECRET));
      }
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testGetScannerResults"));

    // get 5 rows and check the returned results
    scan.setStopRow(Bytes.toBytes("testGetScannerResults05"));
    TAuthorization tauth = new TAuthorization();
    List<String> labels = new ArrayList<>(2);
    labels.add(SECRET);
    labels.add(PRIVATE);
    tauth.setLabels(labels);
    scan.setAuthorizations(tauth);
    List<TResult> results = handler.getScannerResults(table, scan, 5);
    assertEquals(4, results.size());
    for (int i = 0; i < 4; i++) {
      if (i < 3) {
        assertArrayEquals(
            Bytes.toBytes("testGetScannerResults" + pad(i, (byte) 2)), results.get(i).getRow());
      } else if (i == 3) {
        continue;
      } else {
        assertArrayEquals(
            Bytes.toBytes("testGetScannerResults" + pad(i + 1, (byte) 2)), results.get(i).getRow());
      }
    }
  }

  @Test
  public void testGetsWithLabels() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testPutGet");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname),
        wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);
    put.setCellVisibility(new TCellVisibility().setExpression("(" + SECRET + "|"
        + CONFIDENTIAL + ")" + "&" + "!" + TOPSECRET));
    handler.put(table, put);
    TGet get = new TGet(wrap(rowName));
    TAuthorization tauth = new TAuthorization();
    List<String> labels = new ArrayList<>(2);
    labels.add(SECRET);
    labels.add(PRIVATE);
    tauth.setLabels(labels);
    get.setAuthorizations(tauth);
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    assertTColumnValuesEqual(columnValues, returnedColumnValues);
  }

  @Test
  public void testIncrementWithTags() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testIncrementWithTags");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    put.setCellVisibility(new TCellVisibility().setExpression(PRIVATE));
    handler.put(table, put);

    List<TColumnIncrement> incrementColumns = new ArrayList<>(1);
    incrementColumns.add(new TColumnIncrement(wrap(familyAname),
        wrap(qualifierAname)));
    TIncrement increment = new TIncrement(wrap(rowName), incrementColumns);
    increment.setCellVisibility(new TCellVisibility().setExpression(SECRET));
    handler.increment(table, increment);

    TGet get = new TGet(wrap(rowName));
    TAuthorization tauth = new TAuthorization();
    List<String> labels = new ArrayList<>(1);
    labels.add(SECRET);
    tauth.setLabels(labels);
    get.setAuthorizations(tauth);
    TResult result = handler.get(table, get);

    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    TColumnValue columnValue = result.getColumnValues().get(0);
    assertArrayEquals(Bytes.toBytes(2L), columnValue.getValue());
  }

  @Test
  public void testIncrementWithTagsWithNotMatchLabels() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testIncrementWithTagsWithNotMatchLabels");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    put.setCellVisibility(new TCellVisibility().setExpression(PRIVATE));
    handler.put(table, put);

    List<TColumnIncrement> incrementColumns = new ArrayList<>(1);
    incrementColumns.add(new TColumnIncrement(wrap(familyAname),
        wrap(qualifierAname)));
    TIncrement increment = new TIncrement(wrap(rowName), incrementColumns);
    increment.setCellVisibility(new TCellVisibility().setExpression(SECRET));
    handler.increment(table, increment);

    TGet get = new TGet(wrap(rowName));
    TAuthorization tauth = new TAuthorization();
    List<String> labels = new ArrayList<>(1);
    labels.add(PUBLIC);
    tauth.setLabels(labels);
    get.setAuthorizations(tauth);
    TResult result = handler.get(table, get);
    assertNull(result.getRow());
  }

  @Test
  public void testAppend() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testAppend");
    ByteBuffer table = wrap(tableAname);
    byte[] v1 = Bytes.toBytes(1L);
    byte[] v2 = Bytes.toBytes(5L);
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    put.setCellVisibility(new TCellVisibility().setExpression(PRIVATE));
    handler.put(table, put);

    List<TColumnValue> appendColumns = new ArrayList<>(1);
    appendColumns.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(v2)));
    TAppend append = new TAppend(wrap(rowName), appendColumns);
    append.setCellVisibility(new TCellVisibility().setExpression(SECRET));
    handler.append(table, append);

    TGet get = new TGet(wrap(rowName));
    TAuthorization tauth = new TAuthorization();
    List<String> labels = new ArrayList<>(1);
    labels.add(SECRET);
    tauth.setLabels(labels);
    get.setAuthorizations(tauth);
    TResult result = handler.get(table, get);

    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    TColumnValue columnValue = result.getColumnValues().get(0);
    assertArrayEquals(Bytes.add(v1, v2), columnValue.getValue());
  }

  /**
   * Padding numbers to make comparison of sort order easier in a for loop
   *
   * @param n
   *          The number to pad.
   * @param pad
   *          The length to pad up to.
   * @return The padded number as a string.
   */
  private String pad(int n, byte pad) {
    String res = Integer.toString(n);
    while (res.length() < pad) {
      res = "0" + res;
    }
    return res;
  }
}
