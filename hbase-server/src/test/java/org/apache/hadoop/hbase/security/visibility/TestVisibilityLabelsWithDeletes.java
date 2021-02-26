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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
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
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, LargeTests.class })
public class TestVisibilityLabelsWithDeletes extends VisibilityLabelsWithDeletesTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestVisibilityLabelsWithDeletes.class);

  @Override
  protected Table createTable(byte[] fam) throws IOException {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    TEST_UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam)).build());
    return TEST_UTIL.getConnection().getTable(tableName);
  }

  private TableName createTable() throws IOException {
    return createTable(-1);
  }

  private TableName createTable(int maxVersions) throws IOException {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    createTable(tableName, maxVersions);
    return tableName;
  }

  private void createTable(TableName tableName, int maxVersions) throws IOException {
    ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.newBuilder(fam);
    if (maxVersions > 0) {
      builder.setMaxVersions(maxVersions);
    }
    TEST_UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(builder.build()).build());
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsWithMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
            d.addColumns(fam, qual, 125L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
            d3.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsNoMatchVisExpWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamilyWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
            d3.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnsWithoutAndWithVisibilityLabels() throws Exception {
    TableName tableName = createTable();
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
            assertEquals(1, next.length);
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
            assertEquals(0, next.length);
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
    TableName tableName = createTable();
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
            assertEquals(0, next.length);
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
            assertEquals(0, next.length);
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
    TableName tableName = createTable();
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
            assertEquals(1, next.length);
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
            assertEquals(0, next.length);
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
    TableName tableName = createTable();
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
            assertEquals(0, next.length);
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
            assertEquals(0, next.length);
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
    TableName tableName = createTable();
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
            assertEquals(1, next.length);
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
            assertEquals(0, next.length);
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
    TableName tableName = createTable(5);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertEquals(1, next.length);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      scanner = table.getScanner(s);
      Result[] next1 = scanner.next(3);
      assertEquals(0, next1.length);
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnsWithPutsReAppearing() throws Exception {
    TableName tableName = createTable(5);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertEquals(1, next.length);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET));
      scanner = table.getScanner(s);
      Result[] next1 = scanner.next(3);
      assertEquals(0, next1.length);
    }
  }

  @Test
  public void testVisibilityCombinations() throws Exception {
    TableName tableName = createTable(5);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 123L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 124L, value1);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addColumns(fam, qual, 126L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }

          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addColumn(fam, qual, 123L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL, SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(0, next.length);
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteColumnWithSpecificVersionWithPutsReAppearing()
      throws Exception {
    TableName tableName = createTable(5);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put1 = new Put(Bytes.toBytes("row1"));
      put1.addColumn(fam, qual, 123L, value);
      put1.setCellVisibility(new CellVisibility(CONFIDENTIAL));

      Put put2 = new Put(Bytes.toBytes("row1"));
      put2.addColumn(fam, qual, 123L, value1);
      put2.setCellVisibility(new CellVisibility(SECRET));
      table.put(createList(put1, put2));

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL, SECRET));

      ResultScanner scanner = table.getScanner(s);
      assertEquals(1, scanner.next(3).length);
      scanner.close();

      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addColumn(fam, qual, 123L);
            table.delete(d);
          }

          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
            d.addColumn(fam, qual, 123L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(CONFIDENTIAL));
      scanner = table.getScanner(s);
      assertEquals(0, scanner.next(3).length);
      scanner.close();
    }
  }

  @Test
  public void testVisibilityLabelsWithDeleteFamilyNoMatchingVisExpWithMultipleVersionsNoTimestamp()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
          d3.setCellVisibility(new CellVisibility(
              "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      scanner.close();
    }
  }

  @Test
  public void testDeleteFamilyAndDeleteColumnsWithAndWithoutVisibilityExp() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      scanner.close();
    }
  }

  private Table doPuts(TableName tableName) throws IOException, InterruptedIOException,
      RetriesExhaustedWithDetailsException, InterruptedException {
    createTable(tableName, 5);

    List<Put> puts = new ArrayList<>(5);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 123L, value);
    put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 124L, value);
    put.setCellVisibility(new CellVisibility(
        "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 125L, value);
    put.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 126L, value);
    put.setCellVisibility(new CellVisibility(
        "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 127L, value);
    put.setCellVisibility(new CellVisibility(
        "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
    puts.add(put);

    TEST_UTIL.getAdmin().flush(tableName);
    put = new Put(Bytes.toBytes("row2"));
    put.addColumn(fam, qual, 127L, value);
    put.setCellVisibility(new CellVisibility(
        "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
    puts.add(put);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(puts);
    return table;
  }

  private Table doPutsWithDiffCols(TableName tableName) throws IOException, InterruptedIOException,
      RetriesExhaustedWithDetailsException, InterruptedException {
    createTable(tableName, 5);

    List<Put> puts = new ArrayList<>(5);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 123L, value);
    put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 124L, value);
    put.setCellVisibility(new CellVisibility(
        "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 125L, value);
    put.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual1, 126L, value);
    put.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual2, 127L, value);
    put.setCellVisibility(new CellVisibility(
        "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
    puts.add(put);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(puts);
    return table;
  }

  private Table doPutsWithoutVisibility(TableName tableName) throws IOException,
      InterruptedIOException, RetriesExhaustedWithDetailsException, InterruptedException {
    createTable(tableName, 5);
    List<Put> puts = new ArrayList<>(5);
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 123L, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 124L, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 125L, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 126L, value);
    puts.add(put);

    put = new Put(Bytes.toBytes("row1"));
    put.addColumn(fam, qual, 127L, value);
    puts.add(put);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(puts);

    TEST_UTIL.getAdmin().flush(tableName);

    put = new Put(Bytes.toBytes("row2"));
    put.addColumn(fam, qual, 127L, value);
    table.put(put);

    return table;
  }

  @Test
  public void testDeleteColumnWithSpecificTimeStampUsingMultipleVersionsUnMatchingVisExpression()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
            d.addColumn(fam, qual, 125L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnWithLatestTimeStampUsingMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnWithLatestTimeStampWhenNoVersionMatches() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 128L, value);
      put.setCellVisibility(new CellVisibility(TOPSECRET));
      table.put(put);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET));
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(128L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 129L, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);

      TEST_UTIL.getAdmin().flush(tableName);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(129L, current.getTimestamp());
    }
  }

  @Test
  public void testDeleteColumnWithLatestTimeStampUsingMultipleVersionsAfterCompaction()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
      put.addColumn(fam, qual, 127L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL + "&" + PRIVATE));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      TEST_UTIL.getAdmin().majorCompact(tableName);
      // Sleep to ensure compaction happens. Need to do it in a better way
      Thread.sleep(5000);
      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 3);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilyLatestTimeStampWithMulipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnswithMultipleColumnsWithMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPutsWithDiffCols(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d = new Delete(row1);
          d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
          d.addColumns(fam, qual, 125L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertTrue(Bytes.equals(current.getQualifierArray(), current.getQualifierOffset(),
        current.getQualifierLength(), qual1, 0, qual1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      assertTrue(Bytes.equals(current.getQualifierArray(), current.getQualifierOffset(),
        current.getQualifierLength(), qual2, 0, qual2.length));
    }
  }

  @Test
  public void testDeleteColumnsWithDiffColsAndTags() throws Exception {
    TableName tableName = createTable(5);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 125L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 126L, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d1 = new Delete(row1);
          d1.setCellVisibility(new CellVisibility(SECRET));
          d1.addColumns(fam, qual, 126L);

          Delete d2 = new Delete(row1);
          d2.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          d2.addColumns(fam, qual1, 125L);

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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
    }
  }

  @Test
  public void testDeleteColumnsWithDiffColsAndTags1() throws Exception {
    TableName tableName = createTable(5);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 125L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual1, 126L, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          Delete d1 = new Delete(row1);
          d1.setCellVisibility(new CellVisibility(SECRET));
          d1.addColumns(fam, qual, 126L);

          Delete d2 = new Delete(row1);
          d2.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          d2.addColumns(fam, qual1, 126L);

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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertEquals(1, next.length);
    }
  }

  @Test
  public void testDeleteFamilyWithoutCellVisibilityWithMulipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      // All cells wrt row1 should be deleted as we are not passing the Cell Visibility
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilyLatestTimeStampWithMulipleVersionsWithoutCellVisibilityInPuts()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilySpecificTimeStampWithMulipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
            d.addFamily(fam, 126L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(6);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testScanAfterCompaction() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + SECRET + "&" + TOPSECRET + ")"));
            d.addFamily(fam, 126L);
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
      put.addColumn(fam, qual, 127L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL + "&" + PRIVATE));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      TEST_UTIL.getAdmin().compact(tableName);
      Thread.sleep(5000);
      // Sleep to ensure compaction happens. Need to do it in a better way
      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 3);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteFamilySpecificTimeStampWithMulipleVersionsDoneTwice() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addFamily(fam, 125L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addFamily(fam, 127L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      assertEquals(127L, current.getTimestamp());
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
    SUPERUSER.runAs(action);
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(CONFIDENTIAL));
            d.addFamilyVersion(fam, 123L);
            table.delete(d);
            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamilyVersion(fam, 125L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(5);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
    }
  }

  @Test
  public void testSpecificDeletesFollowedByDeleteFamily() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 126L);
            table.delete(d);
            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamilyVersion(fam, 125L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(5);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(5);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
    }
  }

  @Test
  public void testSpecificDeletesFollowedByDeleteFamily1() throws Exception {
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
    SUPERUSER.runAs(action);
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual);
            table.delete(d);

            d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addFamilyVersion(fam, 125L);
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(5);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(5);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
    }
  }

  @Test
  public void testDeleteColumnSpecificTimeStampWithMulipleVersionsDoneTwice() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual, 125L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 127L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      assertEquals(127L, current.getTimestamp());
    }
  }

  @Test
  public void testDeleteColumnSpecificTimeStampWithMulipleVersionsDoneTwice1() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")" + "|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 127L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual, 127L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      assertEquals(127L, current.getTimestamp());
    }
  }

  @Test
  public void testDeleteColumnSpecificTimeStampWithMulipleVersionsDoneTwice2() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());

    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 125L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 127L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      assertEquals(127L, current.getTimestamp());
    }
  }

  @Test
  public void testDeleteColumnAndDeleteFamilylSpecificTimeStampWithMulipleVersion()
      throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    // Do not flush here.
    try (Table table = doPuts(tableName)) {
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(SECRET + "&" + TOPSECRET));
            d.addColumn(fam, qual, 125L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addFamily(fam, 124L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
      assertEquals(127L, current.getTimestamp());
    }
  }

  @Test
  public void testDiffDeleteTypesForTheSameCellUsingMultipleVersions() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      // Do not flush here.
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + PRIVATE + "&" + CONFIDENTIAL + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumns(fam, qual, 125L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);

      Scan s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 2);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(127L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));

      // Issue 2nd delete
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.setCellVisibility(new CellVisibility(
                "(" + CONFIDENTIAL + "&" + PRIVATE + ")|(" + TOPSECRET + "&" + SECRET + ")"));
            d.addColumn(fam, qual, 127L);
            table.delete(d);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(actiona);
      s = new Scan();
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      scanner = table.getScanner(s);
      next = scanner.next(3);
      assertTrue(next.length == 2);
      cellScanner = next[0].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(126L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(125L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row2, 0, row2.length));
    }
  }

  @Test
  public void testDeleteColumnLatestWithNoCellVisibility() throws Exception {
    setAuths();
    final TableName tableName = TableName.valueOf(testName.getMethodName());
    try (Table table = doPuts(tableName)) {
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addColumn(fam, qual, 125L);
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
      s.readVersions(5);
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
            d.addColumns(fam, qual, 125L);
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
      s.readVersions(5);
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
            d.addFamily(fam, 125L);
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
      s.readVersions(5);
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
      s.readVersions(5);
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
      s.readVersions(5);
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
            d.addFamilyVersion(fam, 126L);
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
      s.readVersions(5);
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
    assertEquals(127L, current.getTimestamp());
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
      row1, 0, row1.length));
    assertEquals(126L, current.getTimestamp());
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
      row1, 0, row1.length));
    assertEquals(125L, current.getTimestamp());
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
      row1, 0, row1.length));
    assertEquals(124L, current.getTimestamp());
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
      row1, 0, row1.length));
    assertEquals(123L, current.getTimestamp());
    cellScanner = next[1].cellScanner();
    cellScanner.advance();
    current = cellScanner.current();
    assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
      row2, 0, row2.length));
  }

  @Test
  public void testVisibilityExpressionWithNotEqualORCondition() throws Exception {
    setAuths();
    TableName tableName = createTable(5);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 123L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);
      put = new Put(Bytes.toBytes("row1"));
      put.addColumn(fam, qual, 124L, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL + "|" + PRIVATE));
      table.put(put);
      TEST_UTIL.getAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName)) {
            Delete d = new Delete(row1);
            d.addColumn(fam, qual, 124L);
            d.setCellVisibility(new CellVisibility(PRIVATE));
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
      s.readVersions(5);
      s.setAuthorizations(new Authorizations(SECRET, PRIVATE, CONFIDENTIAL, TOPSECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(124L, current.getTimestamp());
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(), current.getRowLength(),
        row1, 0, row1.length));
      assertEquals(123L, current.getTimestamp());
    }
  }

  @Test
  public void testDeleteWithNoVisibilitiesForPutsAndDeletes() throws Exception {
    TableName tableName = createTable(5);
    Put p = new Put(Bytes.toBytes("row1"));
    p.addColumn(fam, qual, value);
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.addColumn(fam, qual1, value);
    table.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.addColumn(fam, qual, value);
    table.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.addColumn(fam, qual1, value);
    table.put(p);
    Delete d = new Delete(Bytes.toBytes("row1"));
    table.delete(d);
    Get g = new Get(Bytes.toBytes("row1"));
    g.readAllVersions();
    g.setAuthorizations(new Authorizations(SECRET, PRIVATE));
    Result result = table.get(g);
    assertEquals(0, result.rawCells().length);

    p = new Put(Bytes.toBytes("row1"));
    p.addColumn(fam, qual, value);
    table.put(p);
    result = table.get(g);
    assertEquals(1, result.rawCells().length);
  }

  @Test
  public void testDeleteWithFamilyDeletesOfSameTsButDifferentVisibilities() throws Exception {
    TableName tableName = createTable(5);
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
    g.readAllVersions();
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
    g.readAllVersions();
    g.setAuthorizations(new Authorizations(SECRET, PRIVATE));
    result = table.get(g);
    assertEquals(0, result.rawCells().length);
  }

  @SafeVarargs
  public static <T> List<T> createList(T... ts) {
    return new ArrayList<>(Arrays.asList(ts));
  }

  private enum DeleteMark {
    ROW, FAMILY, FAMILY_VERSION, COLUMN, CELL
  }

  private static Delete addDeleteMark(Delete d, DeleteMark mark, long now) {
    switch (mark) {
      case ROW:
        break;
      case FAMILY:
        d.addFamily(fam);
        break;
      case FAMILY_VERSION:
        d.addFamilyVersion(fam, now);
        break;
      case COLUMN:
        d.addColumns(fam, qual);
        break;
      case CELL:
        d.addColumn(fam, qual);
        break;
      default:
        break;
    }
    return d;
  }

  @Test
  public void testDeleteCellWithoutVisibility() throws IOException, InterruptedException {
    for (DeleteMark mark : DeleteMark.values()) {
      testDeleteCellWithoutVisibility(mark);
    }
  }

  private void testDeleteCellWithoutVisibility(DeleteMark mark)
      throws IOException, InterruptedException {
    setAuths();
    TableName tableName = TableName.valueOf("testDeleteCellWithoutVisibility-" + mark.name());
    createTable(tableName, 5);
    long now = EnvironmentEdgeManager.currentTime();
    List<Put> puts = new ArrayList<>(1);
    Put put = new Put(row1);
    if (mark == DeleteMark.FAMILY_VERSION) {
      put.addColumn(fam, qual, now, value);
    } else {
      put.addColumn(fam, qual, value);
    }

    puts.add(put);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      table.put(puts);
      Result r = table.get(new Get(row1));
      assertEquals(1, r.size());
      assertEquals(Bytes.toString(value), Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));

      Delete d = addDeleteMark(new Delete(row1), mark, now);
      table.delete(d);
      r = table.get(new Get(row1));
      assertEquals(0, r.size());
    }
  }

  @Test
  public void testDeleteCellWithVisibility() throws IOException, InterruptedException {
    for (DeleteMark mark : DeleteMark.values()) {
      testDeleteCellWithVisibility(mark);
      testDeleteCellWithVisibilityV2(mark);
    }
  }

  private void testDeleteCellWithVisibility(DeleteMark mark)
      throws IOException, InterruptedException {
    setAuths();
    TableName tableName = TableName.valueOf("testDeleteCellWithVisibility-" + mark.name());
    createTable(tableName, 5);
    long now = EnvironmentEdgeManager.currentTime();
    List<Put> puts = new ArrayList<>(2);
    Put put = new Put(row1);
    if (mark == DeleteMark.FAMILY_VERSION) {
      put.addColumn(fam, qual, now, value);
    } else {
      put.addColumn(fam, qual, value);
    }
    puts.add(put);
    put = new Put(row1);
    if (mark == DeleteMark.FAMILY_VERSION) {
      put.addColumn(fam, qual, now, value1);
    } else {
      put.addColumn(fam, qual, value1);
    }
    put.setCellVisibility(new CellVisibility(PRIVATE));
    puts.add(put);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      table.put(puts);
      Result r = table.get(new Get(row1));
      assertEquals(0, r.size());
      r = table.get(new Get(row1).setAuthorizations(new Authorizations(PRIVATE)));
      assertEquals(1, r.size());
      assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));

      Delete d = addDeleteMark(new Delete(row1), mark, now);
      table.delete(d);

      r = table.get(new Get(row1));
      assertEquals(0, r.size());
      r = table.get(new Get(row1).setAuthorizations(new Authorizations(PRIVATE)));
      assertEquals(1, r.size());
      assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));

      d = addDeleteMark(new Delete(row1).setCellVisibility(new CellVisibility(PRIVATE)), mark, now);
      table.delete(d);

      r = table.get(new Get(row1));
      assertEquals(0, r.size());
      r = table.get(new Get(row1).setAuthorizations(new Authorizations(PRIVATE)));
      assertEquals(0, r.size());
    }
  }

  private void testDeleteCellWithVisibilityV2(DeleteMark mark)
      throws IOException, InterruptedException {
    setAuths();
    TableName tableName = TableName.valueOf("testDeleteCellWithVisibilityV2-" + mark.name());
    createTable(tableName, 5);
    long now = EnvironmentEdgeManager.currentTime();
    List<Put> puts = new ArrayList<>(2);
    Put put = new Put(row1);
    put.setCellVisibility(new CellVisibility(PRIVATE));
    if (mark == DeleteMark.FAMILY_VERSION) {
      put.addColumn(fam, qual, now, value);
    } else {
      put.addColumn(fam, qual, value);
    }
    puts.add(put);
    put = new Put(row1);
    if (mark == DeleteMark.FAMILY_VERSION) {
      put.addColumn(fam, qual, now, value1);
    } else {
      put.addColumn(fam, qual, value1);
    }
    puts.add(put);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      table.put(puts);
      Result r = table.get(new Get(row1));
      assertEquals(1, r.size());
      assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
      r = table.get(new Get(row1).setAuthorizations(new Authorizations(PRIVATE)));
      assertEquals(1, r.size());
      assertEquals(Bytes.toString(value1), Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));

      Delete d = addDeleteMark(new Delete(row1), mark, now);
      table.delete(d);

      r = table.get(new Get(row1));
      assertEquals(0, r.size());
      r = table.get(new Get(row1).setAuthorizations(new Authorizations(PRIVATE)));
      assertEquals(0, r.size());

      d = addDeleteMark(new Delete(row1).setCellVisibility(new CellVisibility(PRIVATE)), mark, now);
      table.delete(d);

      r = table.get(new Get(row1));
      assertEquals(0, r.size());
      r = table.get(new Get(row1).setAuthorizations(new Authorizations(PRIVATE)));
      assertEquals(0, r.size());
    }
  }
}
