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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests visibility labels with deletes
 */
public abstract class VisibilityLabelsWithDeletesTestBase {

  protected static final String TOPSECRET = "TOPSECRET";
  protected static final String PUBLIC = "PUBLIC";
  protected static final String PRIVATE = "PRIVATE";
  protected static final String CONFIDENTIAL = "CONFIDENTIAL";
  protected static final String SECRET = "SECRET";
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static final byte[] row1 = Bytes.toBytes("row1");
  protected static final byte[] row2 = Bytes.toBytes("row2");
  protected final static byte[] fam = Bytes.toBytes("info");
  protected final static byte[] qual = Bytes.toBytes("qual");
  protected final static byte[] qual1 = Bytes.toBytes("qual1");
  protected final static byte[] qual2 = Bytes.toBytes("qual2");
  protected final static byte[] value = Bytes.toBytes("value");
  protected final static byte[] value1 = Bytes.toBytes("value1");
  protected static Configuration conf;

  @Rule
  public final TestName testName = new TestName();
  protected static User SUPERUSER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
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

  protected abstract Table createTable(byte[] fam) throws IOException;

  protected final void setAuths() throws IOException, InterruptedException {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
      new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
        @Override
        public VisibilityLabelsResponse run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(conf)) {
            return VisibilityClient.setAuths(conn,
              new String[] { CONFIDENTIAL, PRIVATE, SECRET, TOPSECRET }, SUPERUSER.getShortName());
          } catch (Throwable e) {
          }
          return null;
        }
      };
    SUPERUSER.runAs(action);
  }

  private Table createTableAndWriteDataWithLabels(String... labelExps) throws Exception {
    Table table = createTable(fam);
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

  private Table createTableAndWriteDataWithLabels(long[] timestamp, String... labelExps)
      throws Exception {
    Table table = createTable(fam);
    int i = 1;
    List<Put> puts = new ArrayList<>(labelExps.length);
    for (String labelExp : labelExps) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(fam, qual, timestamp[i - 1], value);
      put.setCellVisibility(new CellVisibility(labelExp));
      puts.add(put);
      table.put(put);
      TEST_UTIL.getAdmin().flush(table.getName());
      i++;
    }
    return table;
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumns() throws Throwable {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());

    try (Table table = createTableAndWriteDataWithLabels(SECRET + "&" + TOPSECRET, SECRET)) {
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
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamily() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = createTableAndWriteDataWithLabels(SECRET, CONFIDENTIAL + "|" + TOPSECRET)) {
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
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamilyVersion() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    long[] ts = new long[] { 123L, 125L };
    try (
      Table table = createTableAndWriteDataWithLabels(ts, CONFIDENTIAL + "|" + TOPSECRET, SECRET)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(TOPSECRET + "|" + CONFIDENTIAL));
            d.addFamilyVersion(fam, 123L);
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
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnExactVersion() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    long[] ts = new long[] { 123L, 125L };
    try (
      Table table = createTableAndWriteDataWithLabels(ts, CONFIDENTIAL + "|" + TOPSECRET, SECRET)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(TOPSECRET + "|" + CONFIDENTIAL));
            d.addColumn(fam, qual, 123L);
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
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }
}
