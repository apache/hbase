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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestDefaultScanLabelGeneratorStack {

  public static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROW_1 = Bytes.toBytes("row1");
  private final static byte[] CF = Bytes.toBytes("f");
  private final static byte[] Q1 = Bytes.toBytes("q1");
  private final static byte[] Q2 = Bytes.toBytes("q2");
  private final static byte[] Q3 = Bytes.toBytes("q3");
  private final static byte[] value1 = Bytes.toBytes("value1");
  private final static byte[] value2 = Bytes.toBytes("value2");
  private final static byte[] value3 = Bytes.toBytes("value3");
  public static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER;
  public static User TESTUSER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    VisibilityTestUtil.enableVisiblityLabels(conf);
    // Not setting any SLG class. This means to use the default behavior.
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(1);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    TESTUSER = User.createUserForTesting(conf, "test", new String[] { });

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);

    // Set up for the test
    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        try {
          VisibilityClient.addLabels(conf, new String[] { SECRET, CONFIDENTIAL });
          VisibilityClient.setAuths(conf, new String[] { CONFIDENTIAL }, TESTUSER.getShortName());
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    });
  }

  @Test
  public void testDefaultScanLabelGeneratorStack() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());

    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        HTable table = TEST_UTIL.createTable(tableName, CF);
        try {
          Put put = new Put(ROW_1);
          put.add(CF, Q1, HConstants.LATEST_TIMESTAMP, value1);
          put.setCellVisibility(new CellVisibility(SECRET));
          table.put(put);
          put = new Put(ROW_1);
          put.add(CF, Q2, HConstants.LATEST_TIMESTAMP, value2);
          put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          table.put(put);
          put = new Put(ROW_1);
          put.add(CF, Q3, HConstants.LATEST_TIMESTAMP, value3);
          table.put(put);
          return null;
        } finally {
          table.close();
        }
      }
    });

    TESTUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        HTable table = new HTable(conf, tableName);
        try {
          // Test scan with no auth attribute
          Scan s = new Scan();
          ResultScanner scanner = table.getScanner(s);
          Result[] next = scanner.next(1);

          assertTrue(next.length == 1);
          CellScanner cellScanner = next[0].cellScanner();
          cellScanner.advance();
          Cell current = cellScanner.current();
          // test user can see value2 (CONFIDENTIAL) and value3 (no label)
          assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
              current.getRowLength(), ROW_1, 0, ROW_1.length));
          assertTrue(Bytes.equals(current.getQualifier(), Q2));
          assertTrue(Bytes.equals(current.getValue(), value2));
          cellScanner.advance();
          current = cellScanner.current();
          // test user can see value2 (CONFIDENTIAL) and value3 (no label)
          assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
              current.getRowLength(), ROW_1, 0, ROW_1.length));
          assertTrue(Bytes.equals(current.getQualifier(), Q3));
          assertTrue(Bytes.equals(current.getValue(), value3));

          // Test scan with correct auth attribute for test user
          Scan s1 = new Scan();
          // test user is entitled to 'CONFIDENTIAL'.
          // If we set both labels in the scan, 'SECRET' will be dropped by the SLGs.
          s1.setAuthorizations(new Authorizations(new String[] { SECRET, CONFIDENTIAL }));
          ResultScanner scanner1 = table.getScanner(s1);
          Result[] next1 = scanner1.next(1);

          assertTrue(next1.length == 1);
          CellScanner cellScanner1 = next1[0].cellScanner();
          cellScanner1.advance();
          Cell current1 = cellScanner1.current();
          // test user can see value2 (CONFIDENTIAL) and value3 (no label)
          assertTrue(Bytes.equals(current1.getRowArray(), current1.getRowOffset(),
            current1.getRowLength(), ROW_1, 0, ROW_1.length));
          assertTrue(Bytes.equals(current1.getQualifier(), Q2));
          assertTrue(Bytes.equals(current1.getValue(), value2));
          cellScanner1.advance();
          current1 = cellScanner1.current();
          // test user can see value2 (CONFIDENTIAL) and value3 (no label)
          assertTrue(Bytes.equals(current1.getRowArray(), current1.getRowOffset(),
            current1.getRowLength(), ROW_1, 0, ROW_1.length));
          assertTrue(Bytes.equals(current1.getQualifier(), Q3));
          assertTrue(Bytes.equals(current1.getValue(), value3));

          // Test scan with incorrect auth attribute for test user
          Scan s2 = new Scan();
          // test user is entitled to 'CONFIDENTIAL'.
          // If we set 'SECRET', it will be dropped by the SLGs.
          s2.setAuthorizations(new Authorizations(new String[] { SECRET }));
          ResultScanner scanner2 = table.getScanner(s2);
          Result next2 = scanner2.next();
          CellScanner cellScanner2 = next2.cellScanner();
          cellScanner2.advance();
          Cell current2 = cellScanner2.current();
          // This scan will only see value3 (no label)
          assertTrue(Bytes.equals(current2.getRowArray(), current2.getRowOffset(),
            current2.getRowLength(), ROW_1, 0, ROW_1.length));
          assertTrue(Bytes.equals(current2.getQualifier(), Q3));
          assertTrue(Bytes.equals(current2.getValue(), value3));

          assertFalse(cellScanner2.advance());


          return null;
        } finally {
          table.close();
        }
      }
    });

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
