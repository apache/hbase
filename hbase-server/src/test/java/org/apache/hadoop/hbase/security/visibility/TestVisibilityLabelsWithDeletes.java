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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests visibility labels with deletes
 */
@Category({SecurityTests.class, MediumTests.class})
public class TestVisibilityLabelsWithDeletes {
  private static final String TOPSECRET = "TOPSECRET";
  private static final String PUBLIC = "PUBLIC";
  private static final String PRIVATE = "PRIVATE";
  private static final String CONFIDENTIAL = "CONFIDENTIAL";
  private static final String SECRET = "SECRET";
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private final static byte[] fam = Bytes.toBytes("info");
  private final static byte[] qual = Bytes.toBytes("qual");
  private final static byte[] qual1 = Bytes.toBytes("qual1");
  private final static byte[] qual2 = Bytes.toBytes("qual2");
  private final static byte[] value = Bytes.toBytes("value");
  private final static byte[] value1 = Bytes.toBytes("value1");
  public static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    VisibilityTestUtil.enableVisiblityLabels(conf);
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
        ScanLabelGenerator.class);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(2);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumns() throws Throwable {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());

    try (Table table = createTableAndWriteDataWithLabels(tableName,
        SECRET + "&" + TOPSECRET, SECRET)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(TOPSECRET + "&" + SECRET));
            d.addColumns(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamily() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = createTableAndWriteDataWithLabels(tableName, SECRET,
        CONFIDENTIAL + "|" + TOPSECRET);) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row2);
            d.setCellVisibility(new CellVisibility(TOPSECRET + "|" + CONFIDENTIAL));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamilyVersion() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    long[] ts = new long[] { 123l, 125l };
    try (Table table = createTableAndWriteDataWithLabels(tableName, ts,
        CONFIDENTIAL + "|" + TOPSECRET, SECRET)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(TOPSECRET + "|" + CONFIDENTIAL));
            d.addFamilyVersion(fam, 123l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnExactVersion() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    long[] ts = new long[] { 123l, 125l };
    try (Table table = createTableAndWriteDataWithLabels(tableName, ts,
        CONFIDENTIAL + "|" + TOPSECRET, SECRET);) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(TOPSECRET + "|" + CONFIDENTIAL));
            d.addColumn(fam, qual, 123l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsWithMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" +
                SECRET + "&" + TOPSECRET+")"));
            d.addColumns(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d1 = new Delete(row1);
            d1.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d1.addColumns(fam, qual);

            table.delete(d1);

            Delete d2 = new Delete(row1);
            d2.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d2.addColumns(fam, qual);
            table.delete(d2);

            Delete d3 = new Delete(row1);
            d3.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + SECRET + "&" + TOPSECRET + ")"));
            d3.addColumns(fam, qual);
            table.delete(d3);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void
    testVisibilityLabelsWithDeleteColumnsWithNoMatchVisExpWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addColumns(fam, qual);
            table.delete(d);

            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addColumns(fam, qual);
            table.delete(d);

            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + SECRET + "&" + TOPSECRET + ")"));
            d.addColumns(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamilyWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d1 = new Delete(row1);
            d1.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d1.addFamily(fam);
            table.delete(d1);

            Delete d2 = new Delete(row1);
            d2.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d2.addFamily(fam);
            table.delete(d2);

            Delete d3 = new Delete(row1);
            d3.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + SECRET + "&" + TOPSECRET + ")"));
            d3.addFamily(fam);
            table.delete(d3);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnsWithoutAndWithVisibilityLabels() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(row1);
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      Delete d = new Delete(row1);
      // without visibility
      d.addColumns(fam, qual, HConstants.LATEST_TIMESTAMP);
      table.delete(d);
      PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 1);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
      d = new Delete(row1);
      // with visibility
      d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      d.addColumns(fam, qual, HConstants.LATEST_TIMESTAMP);
      table.delete(d);
      scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
    }
  }

  @Test
  public void testDeleteColumnsWithAndWithoutVisibilityLabels() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(row1);
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      Delete d = new Delete(row1);
      // with visibility
      d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      d.addColumns(fam, qual, HConstants.LATEST_TIMESTAMP);
      table.delete(d);
      PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
      d = new Delete(row1);
      // without visibility
      d.addColumns(fam, qual, HConstants.LATEST_TIMESTAMP);
      table.delete(d);
      scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
    }
  }

  @Test
  public void testDeleteFamiliesWithoutAndWithVisibilityLabels() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(row1);
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      Delete d = new Delete(row1);
      // without visibility
      d.addFamily(fam);
      table.delete(d);
      PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 1);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
      d = new Delete(row1);
      // with visibility
      d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      d.addFamily(fam);
      table.delete(d);
      scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
    }
  }

  @Test
  public void testDeleteFamiliesWithAndWithoutVisibilityLabels() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(row1);
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      Delete d = new Delete(row1);
      d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      // with visibility
      d.addFamily(fam);
      table.delete(d);
      PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
      d = new Delete(row1);
      // without visibility
      d.addFamily(fam);
      table.delete(d);
      scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
    }
  }

  @Test
  public void testDeletesWithoutAndWithVisibilityLabels() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(row1);
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      Delete d = new Delete(row1);
      // without visibility
      d.addColumn(fam, qual);
      table.delete(d);
      PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            // The delete would not be able to apply it because of visibility mismatch
            Result[] next = scanner.next(3);
            assertEquals(next.length, 1);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
      d = new Delete(row1);
      // with visibility
      d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      d.addColumn(fam, qual);
      table.delete(d);
      scanAction = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
              Table table = connection.getTable(tableName)) {
            Scan s = new Scan();
            ResultScanner scanner = table.getScanner(s);
            Result[] next = scanner.next(3);
            // this will alone match
            assertEquals(next.length, 0);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(scanAction);
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamilyWithPutsReAppearing() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(next.length, 1);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, value1);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertEquals(next.length, 1);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      scanner = table.getScanner(s);
      Result[] next1 = scanner.next(3);
      assertEquals(next1.length, 0);
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsWithPutsReAppearing() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addColumns(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(next.length, 1);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, value1);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addColumns(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertEquals(next.length, 1);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      scanner = table.getScanner(s);
      Result[] next1 = scanner.next(3);
      assertEquals(next1.length, 0);
    }
  }

  @Test
  public void testVisibilityCombinations() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 123l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 124l, value1);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addColumns(fam, qual, 126l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }

          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addColumn(fam, qual, 123l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL, SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(next.length, 0);
    }
  }
  @Test
  public void testVisibilityLabelsWithDeleteColumnWithSpecificVersionWithPutsReAppearing()
      throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);

    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put1 = new Put(Bytes.toBytes("row1"));
      put1.addColumn(fam, qual, 123l, value);
      put1.setCellVisibility(new CellVisibility(CONFIDENTIAL));

      Put put2 = new Put(Bytes.toBytes("row1"));
      put2.addColumn(fam, qual, 123l, value1);
      put2.setCellVisibility(new CellVisibility(SECRET));
      table.put(createList(put1, put2));

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL, SECRET));

      ResultScanner scanner = table.getScanner(s);
      assertEquals(scanner.next(3).length, 1);
      scanner.close();

      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addColumn(fam, qual, 123l);
            table.delete(d);
          }

          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addColumn(fam, qual, 123l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL));
      scanner = table.getScanner(s);
      assertEquals(scanner.next(3).length, 0);
      scanner.close();
    }
  }

  @Test
  public void
    testVisibilityLabelsWithDeleteFamilyWithNoMatchingVisExpWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d1 = new Delete(row1);
          d1.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          d1.addFamily(fam);

          Delete d2 = new Delete(row1);
          d2.setCellVisibility(new CellVisibility(SECRET));
          d2.addFamily(fam);

          Delete d3 = new Delete(row1);
          d3.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
              + SECRET + "&" + TOPSECRET + ")"));
          d3.addFamily(fam);

          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            table.delete(createList(d1, d2, d3));
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      scanner.close();
    }
  }

  @Test
  public void testDeleteFamilyAndDeleteColumnsWithAndWithoutVisibilityExp() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d1 = new Delete(row1);
          d1.addFamily(fam);

          Delete d2 = new Delete(row1);
          d2.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
          d2.addColumns(fam, qual);
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            table.delete(createList(d1, d2));
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      scanner.close();
    }
  }

  private Table doPuts(TableName tableName) throws IOException, InterruptedIOException,
      RetriesExhaustedWithDetailsException, InterruptedException {
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);

    List<Put> puts = new ArrayList<>(5);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 123l, value);
    put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 124l, value);
    put.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
    + TOPSECRET + "&" + SECRET+")"));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 125l, value);
    put.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 126l, value);
    put.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
        + TOPSECRET + "&" + SECRET+")"));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 127l, value);
    put.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
        + TOPSECRET + "&" + SECRET+")"));
    puts.add(put);

    TEST_UTIL.getAdmin().flush(tableName);
    put = new Put(Bytes.toBytes("row2"));
    put.addColumn(fam, qual, 127l, value);
    put.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET
        + "&" + SECRET + ")"));
    puts.add(put);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(puts);
    return table;
  }

  private Table doPutsWithDiffCols(TableName tableName) throws IOException,
      InterruptedIOException, RetriesExhaustedWithDetailsException, InterruptedException {
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);

    List<Put> puts = new ArrayList<>(5);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 123l, value);
    put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 124l, value);
    put.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
    + TOPSECRET + "&" + SECRET+")"));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 125l, value);
    put.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual1, 126l, value);
    put.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual2, 127l, value);
    put.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
        + TOPSECRET + "&" + SECRET+")"));
    puts.add(put);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(puts);
    return table;
  }

  private Table doPutsWithoutVisibility(TableName tableName) throws IOException,
      InterruptedIOException, RetriesExhaustedWithDetailsException, InterruptedException {
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    List<Put> puts = new ArrayList<>(5);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 123l, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 124l, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 125l, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 126l, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 127l, value);
    puts.add(put);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(puts);

    TEST_UTIL.getAdmin().flush(tableName);

    put = new Put(Bytes.toBytes("row2"));
    put.addColumn(fam, qual, 127l, value);
    table.put(put);

    return table;
  }


  @Test
  public void testDeleteColumnWithSpecificTimeStampUsingMultipleVersionsUnMatchingVisExpression()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" +
                SECRET + "&" + TOPSECRET+")"));
            d.addColumn(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnWithLatestTimeStampUsingMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test (timeout=180000)
  public void testDeleteColumnWithLatestTimeStampWhenNoVersionMatches() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 128l, value);
      put.setCellVisibility(new CellVisibility(TOPSECRET));
      table.put(put);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET ));
            d.addColumn(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 128l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 129l, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 129l);
    }
  }
  @Test
  public void testDeleteColumnWithLatestTimeStampUsingMultipleVersionsAfterCompaction()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      TEST_UTIL.getAdmin().flush(tableName);
      Put put = new Put(Bytes.toBytes("row3"));
      put.addColumn(fam, qual, 127l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL + "&" + PRIVATE));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      TEST_UTIL.getAdmin().majorCompact(tableName);
      // Sleep to ensure compaction happens. Need to do it in a better way
      Thread.sleep(5000);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 3);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilyLatestTimeStampWithMulipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnswithMultipleColumnsWithMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPutsWithDiffCols(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d = new Delete(row1);
          d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
          d.addColumns(fam, qual, 125l);
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertTrue(Bytes.equals(current.getQualifierArray(), current.getQualifierOffset(),
          current.getQualifierLength(), qual1, 0, qual1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      assertTrue(Bytes.equals(current.getQualifierArray(), current.getQualifierOffset(),
          current.getQualifierLength(), qual2, 0, qual2.length));
    }
  }

  @Test
  public void testDeleteColumnsWithDiffColsAndTags() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 125l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 126l, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d1 = new Delete(row1);
          d1.setCellVisibility(new CellVisibility(SECRET));
          d1.addColumns(fam, qual, 126l);

          Delete d2 = new Delete(row1);
          d2.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          d2.addColumns(fam, qual1, 125l);

          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            table.delete(createList(d1, d2));
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(next.length, 1);
    }
  }
  @Test
  public void testDeleteColumnsWithDiffColsAndTags1() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 125l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 126l, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d1 = new Delete(row1);
          d1.setCellVisibility(new CellVisibility(SECRET));
          d1.addColumns(fam, qual, 126l);

          Delete d2 = new Delete(row1);
          d2.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          d2.addColumns(fam, qual1, 126l);

          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            table.delete(createList(d1, d2));
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(next.length, 1);
    }
  }
  @Test
  public void testDeleteFamilyWithoutCellVisibilityWithMulipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPutsWithoutVisibility(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      // All cells wrt row1 should be deleted as we are not passing the Cell Visibility
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilyLatestTimeStampWithMulipleVersionsWithoutCellVisibilityInPuts()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPutsWithoutVisibility(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilySpecificTimeStampWithMulipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + SECRET + "&" + TOPSECRET + ")"));
            d.addFamily(fam, 126l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(6);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testScanAfterCompaction() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" +
                SECRET + "&" + TOPSECRET+")"));
            d.addFamily(fam, 126l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Put put = new Put(Bytes.toBytes("row3"));
      put.addColumn(fam, qual, 127l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL + "&" + PRIVATE));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      TEST_UTIL.getAdmin().compact(tableName);
      Thread.sleep(5000);
      // Sleep to ensure compaction happens. Need to do it in a better way
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 3);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilySpecificTimeStampWithMulipleVersionsDoneTwice() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addFamily(fam, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addFamily(fam, 127l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      assertEquals(current.getTimestamp(), 127l);
    }
  }

  @Test
  public void testMultipleDeleteFamilyVersionWithDiffLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { CONFIDENTIAL, PRIVATE, SECRET },
              SUPERUSER.getShortName());
        } catch (Throwable e) {
        }
        return null;
      }
    };
    VisibilityLabelsResponse response = SUPERUSER.runAs(action);
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName);) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addFamilyVersion(fam, 123l);
            table.delete(d);
            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamilyVersion(fam, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(5);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
    }
  }

  @Test (timeout=180000)
  public void testSpecificDeletesFollowedByDeleteFamily() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)){
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 126l);
            table.delete(d);
            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamilyVersion(fam, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(5);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(5);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
    }
  }

  @Test(timeout = 180000)
  public void testSpecificDeletesFollowedByDeleteFamily1() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { CONFIDENTIAL,
            PRIVATE, SECRET },
              SUPERUSER.getShortName());
        } catch (Throwable e) {
        }
        return null;
      }
    };
    VisibilityLabelsResponse response = SUPERUSER.runAs(action);
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)){
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual);
            table.delete(d);

            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamilyVersion(fam, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(5);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(5);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
    }
  }

  @Test
  public void testDeleteColumnSpecificTimeStampWithMulipleVersionsDoneTwice() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addColumn(fam, qual, 127l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      assertEquals(current.getTimestamp(), 127l);
    }
  }

  @Test
  public void testDeleteColumnSpecificTimeStampWithMulipleVersionsDoneTwice1() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")" +
                "|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 127l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual, 127l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      assertEquals(current.getTimestamp(), 127l);
    }
  }
  @Test
  public void testDeleteColumnSpecificTimeStampWithMulipleVersionsDoneTwice2() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());

    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addColumn(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addColumn(fam, qual, 127l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      assertEquals(current.getTimestamp(), 127l);
    }
  }
  @Test
  public void testDeleteColumnAndDeleteFamilylSpecificTimeStampWithMulipleVersion()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addFamily(fam, 124l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      assertEquals(current.getTimestamp(), 127l);
    }
  }

  private void setAuths() throws IOException, InterruptedException {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { CONFIDENTIAL,
            PRIVATE, SECRET,
              TOPSECRET }, SUPERUSER.getShortName());
        } catch (Throwable e) {
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test
  public void testDiffDeleteTypesForTheSameCellUsingMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)){
      // Do not flush here.
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + PRIVATE + "&" + CONFIDENTIAL + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addColumns(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 127l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility("(" + CONFIDENTIAL + "&" + PRIVATE + ")|("
                + TOPSECRET + "&" + SECRET+")"));
            d.addColumn(fam, qual, 127l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 126l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 125l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnLatestWithNoCellVisibility() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = doPuts(tableName)){
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addColumn(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      scanAll(next);
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addColumns(fam, qual, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      scanAll(next);

      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addFamily(fam, 125l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      scanAll(next);

      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addFamily(fam);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      scanAll(next);

      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addColumns(fam, qual);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      scanAll(next);

      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addFamilyVersion(fam, 126l);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      scanAll(next);
    }
  }

  private void scanAll(Result[] next) throws IOException {
    CellScanner cellScanner = next[0].cellScanner();
    cellScanner.advance();
    Cell current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
    assertEquals(current.getTimestamp(), 127l);
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
    assertEquals(current.getTimestamp(), 126l);
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
    assertEquals(current.getTimestamp(), 125l);
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
    assertEquals(current.getTimestamp(), 124l);
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
    assertEquals(current.getTimestamp(), 123l);
    cellScanner = next[1].cellScanner();
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
  }

  @Test
  public void testVisibilityExpressionWithNotEqualORCondition() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 123l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 124l, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL + "|" + PRIVATE));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addColumn(fam, qual, 124l);
            d.setCellVisibility(new CellVisibility(PRIVATE ));
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      TEST_UTIL.getAdmin().flush(tableName);
      Scan s = new Scan();
      s.setMaxVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 124l);
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      assertEquals(current.getTimestamp(), 123l);
    }
  }

  @Test
  public void testDeleteWithNoVisibilitiesForPutsAndDeletes() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    Put p = new Put (Bytes.toBytes("row1"));
    p.addColumn(fam, qual, value);
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(p);
    p = new Put (Bytes.toBytes("row1"));
    p.addColumn(fam, qual1, value);
    table.put(p);
    p = new Put (Bytes.toBytes("row2"));
    p.addColumn(fam, qual, value);
    table.put(p);
    p = new Put (Bytes.toBytes("row2"));
    p.addColumn(fam, qual1, value);
    table.put(p);
    Delete d = new Delete(Bytes.toBytes("row1"));
    table.delete(d);
    Get g = new Get(Bytes.toBytes("row1"));
    g.setMaxVersions();
    g.setAuthorizations(new Authorizations(SECRET, PRIVATE));
    Result result = table.get(g);
    assertEquals(0, result.rawCells().length);

    p = new Put (Bytes.toBytes("row1"));
    p.addColumn(fam, qual, value);
    table.put(p);
    result = table.get(g);
    assertEquals(1, result.rawCells().length);
  }

  @Test
  public void testDeleteWithFamilyDeletesOfSameTsButDifferentVisibilities() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    long t1 = 1234L;
    CellVisibility cellVisibility1 = new CellVisibility(SECRET);
    CellVisibility cellVisibility2 = new CellVisibility(PRIVATE);
    // Cell row1:info:qual:1234 with visibility SECRET
    Put p = new Put(row1);
    p.addColumn(fam, qual, t1, value);
    p.setCellVisibility(cellVisibility1);
    table.put(p);

    // Cell row1:info:qual1:1234 with visibility PRIVATE
    p = new Put(row1);
    p.addColumn(fam, qual1, t1, value);
    p.setCellVisibility(cellVisibility2);
    table.put(p);

    Delete d = new Delete(row1);
    d.addFamily(fam, t1);
    d.setCellVisibility(cellVisibility2);
    table.delete(d);
    d = new Delete(row1);
    d.addFamily(fam, t1);
    d.setCellVisibility(cellVisibility1);
    table.delete(d);

    Get g = new Get(row1);
    g.setMaxVersions();
    g.setAuthorizations(new Authorizations(SECRET, PRIVATE));
    Result result = table.get(g);
    assertEquals(0, result.rawCells().length);

    // Cell row2:info:qual:1234 with visibility SECRET
    p = new Put(row2);
    p.addColumn(fam, qual, t1, value);
    p.setCellVisibility(cellVisibility1);
    table.put(p);

    // Cell row2:info:qual1:1234 with visibility PRIVATE
    p = new Put(row2);
    p.addColumn(fam, qual1, t1, value);
    p.setCellVisibility(cellVisibility2);
    table.put(p);

    d = new Delete(row2);
    d.addFamilyVersion(fam, t1);
    d.setCellVisibility(cellVisibility2);
    table.delete(d);
    d = new Delete(row2);
    d.addFamilyVersion(fam, t1);
    d.setCellVisibility(cellVisibility1);
    table.delete(d);

    g = new Get(row2);
    g.setMaxVersions();
    g.setAuthorizations(new Authorizations(SECRET, PRIVATE));
    result = table.get(g);
    assertEquals(0, result.rawCells().length);
  }

  public static Table createTableAndWriteDataWithLabels(TableName tableName, String... labelExps)
      throws Exception {
    Table table = null;
    table = TEST_UTIL.createTable(tableName, fam);
    int i = 1;
    List<Put> puts = new ArrayList<>(labelExps.length);
    for (String labelExp : labelExps) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setCellVisibility(new CellVisibility(labelExp));
      puts.add(put);
      table.put(put);
      i++;
    }
    // table.put(puts);
    return table;
  }

  public static Table createTableAndWriteDataWithLabels(TableName tableName, long[] timestamp,
      String... labelExps) throws Exception {
    Table table = null;
    table = TEST_UTIL.createTable(tableName, fam);
    int i = 1;
    List<Put> puts = new ArrayList<>(labelExps.length);
    for (String labelExp : labelExps) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(fam, qual, timestamp[i - 1], value);
      put.setCellVisibility(new CellVisibility(labelExp));
      puts.add(put);
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      i++;
    }
    return table;
  }

  public static void addLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, TOPSECRET, CONFIDENTIAL, PUBLIC, PRIVATE };
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
  
  @SafeVarargs
  public static <T> List<T> createList(T... ts) {
    return new ArrayList<>(Arrays.asList(ts));
  }
}
