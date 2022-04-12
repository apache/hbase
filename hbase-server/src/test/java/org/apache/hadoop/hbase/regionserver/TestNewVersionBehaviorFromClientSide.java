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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestNewVersionBehaviorFromClientSide {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNewVersionBehaviorFromClientSide.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] ROW = Bytes.toBytes("r1");
  private static final byte[] ROW2 = Bytes.toBytes("r2");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] value = Bytes.toBytes("value");
  private static final byte[] col1 = Bytes.toBytes("col1");
  private static final byte[] col2 = Bytes.toBytes("col2");
  private static final byte[] col3 = Bytes.toBytes("col3");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void setDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Table createTable() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setNewVersionBehavior(true);
    fam.setMaxVersions(3);
    table.addFamily(fam);
    TEST_UTIL.getHBaseAdmin().createTable(table);
    return TEST_UTIL.getConnection().getTable(tableName);
  }

  @Test
  public void testPutAndDeleteVersions() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));
      t.delete(new Delete(ROW).addColumns(FAMILY, col1, 2000000));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000000, value));
      TEST_UTIL.getAdmin().flush(t.getName());
      Result r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(1, r.size());
      assertEquals(1000000, r.rawCells()[0].getTimestamp());
    }
  }

  @Test
  public void testPutMasked() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));

      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000003));

      Result r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(2, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000002, r.rawCells()[1].getTimestamp());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(2, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000002, r.rawCells()[1].getTimestamp());
    }
  }

  @Test
  public void testPutMasked2() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000003));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));

      Result r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(3, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000003, r.rawCells()[1].getTimestamp());
      assertEquals(1000002, r.rawCells()[2].getTimestamp());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(3, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000003, r.rawCells()[1].getTimestamp());
      assertEquals(1000002, r.rawCells()[2].getTimestamp());
    }
  }

  @Test
  public void testPutMaskedAndUserMaxVersion() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));

      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000004));
      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000003));

      Result r = t.get(new Get(ROW).setMaxVersions(1));
      assertEquals(1, r.size());
      assertEquals(1000002, r.rawCells()[0].getTimestamp());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(1));
      assertEquals(1, r.size());
      assertEquals(1000002, r.rawCells()[0].getTimestamp());
    }
  }

  @Test
  public void testSameTs() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));

      Result r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(3, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000003, r.rawCells()[1].getTimestamp());
      assertEquals(1000002, r.rawCells()[2].getTimestamp());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(3, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000003, r.rawCells()[1].getTimestamp());
      assertEquals(1000002, r.rawCells()[2].getTimestamp());
    }
  }

  @Test
  public void testSameTsAndDelete() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));

      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000003));

      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));

      Result r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(3, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000002, r.rawCells()[1].getTimestamp());
      assertEquals(1000001, r.rawCells()[2].getTimestamp());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(3, r.size());
      assertEquals(1000004, r.rawCells()[0].getTimestamp());
      assertEquals(1000002, r.rawCells()[1].getTimestamp());
      assertEquals(1000001, r.rawCells()[2].getTimestamp());
    }
  }

  @Test
  public void testDeleteFamily() throws IOException {
    try (Table t = createTable()) {

      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col3, 1000001, value));

      t.delete(new Delete(ROW).addFamily(FAMILY, 2000000));

      t.put(new Put(ROW).addColumn(FAMILY, col3, 1500002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, 1500001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1500001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1500002, value));
      TEST_UTIL.getAdmin().flush(t.getName());
      Result r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(4, r.size());
      assertEquals(1500002, r.rawCells()[0].getTimestamp());
      assertEquals(1500001, r.rawCells()[1].getTimestamp());
      assertEquals(1500001, r.rawCells()[2].getTimestamp());
      assertEquals(1500002, r.rawCells()[3].getTimestamp());

      t.delete(new Delete(ROW).addFamilyVersion(FAMILY, 1500001));

      r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(2, r.size());
      assertEquals(1500002, r.rawCells()[0].getTimestamp());
      assertEquals(1500002, r.rawCells()[1].getTimestamp());

      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col3, 1000001, value));
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3));
      assertEquals(6, r.size());
      assertEquals(1500002, r.rawCells()[0].getTimestamp());
      assertEquals(1000002, r.rawCells()[1].getTimestamp());
      assertEquals(1000001, r.rawCells()[2].getTimestamp());
      assertEquals(1000002, r.rawCells()[3].getTimestamp());
      assertEquals(1500002, r.rawCells()[4].getTimestamp());
      assertEquals(1000001, r.rawCells()[5].getTimestamp());
    }
  }

  @Test
  public void testTimeRange() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000005, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000006, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000007, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000008, value));
      Result r = t.get(new Get(ROW).setMaxVersions(3).setTimeRange(0, 1000005));
      assertEquals(0, r.size());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3).setTimeRange(0, 1000005));
      assertEquals(0, r.size());
    }
  }

  @Test
  public void testExplicitColum() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, value));
      t.put(new Put(ROW).addColumn(FAMILY, col2, value));
      t.put(new Put(ROW).addColumn(FAMILY, col3, value));
      t.put(new Put(ROW).addColumn(FAMILY, col3, value));
      t.put(new Put(ROW).addColumn(FAMILY, col3, value));
      t.put(new Put(ROW).addColumn(FAMILY, col3, value));
      Result r = t.get(new Get(ROW).setMaxVersions(3).addColumn(FAMILY, col2));
      assertEquals(3, r.size());
      TEST_UTIL.getAdmin().flush(t.getName());
      r = t.get(new Get(ROW).setMaxVersions(3).addColumn(FAMILY, col2));
      assertEquals(3, r.size());
      TEST_UTIL.getAdmin().flush(t.getName());
    }
  }

  @Test
  public void testgetColumnHint() throws IOException {
    try (Table t = createTable()) {
      t.setOperationTimeout(10000);
      t.setRpcTimeout(10000);
      t.put(new Put(ROW).addColumn(FAMILY, col1, 100, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 101, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 102, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 103, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 104, value));
      t.put(new Put(ROW2).addColumn(FAMILY, col1, 104, value));
      TEST_UTIL.getAdmin().flush(t.getName());
      t.delete(new Delete(ROW).addColumn(FAMILY, col1));
    }
  }

  @Test
  public void testRawScanAndMajorCompaction() throws IOException {
    try (Table t = createTable()) {
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000001, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000002, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000003, value));
      t.put(new Put(ROW).addColumn(FAMILY, col1, 1000004, value));

      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000004));
      t.delete(new Delete(ROW).addColumn(FAMILY, col1, 1000003));

      try (ResultScanner scannner = t.getScanner(new Scan().setRaw(true).setMaxVersions())) {
        Result r = scannner.next();
        assertNull(scannner.next());
        assertEquals(6, r.size());
      }
      TEST_UTIL.getAdmin().flush(t.getName());
      try (ResultScanner scannner = t.getScanner(new Scan().setRaw(true).setMaxVersions())) {
        Result r = scannner.next();
        assertNull(scannner.next());
        assertEquals(6, r.size());
      }
      TEST_UTIL.getAdmin().majorCompact(t.getName());
      Threads.sleep(5000);
      try (ResultScanner scannner = t.getScanner(new Scan().setRaw(true).setMaxVersions())) {
        Result r = scannner.next();
        assertNull(scannner.next());
        assertEquals(1, r.size());
        assertEquals(1000002, r.rawCells()[0].getTimestamp());
      }
    }
  }

  @Test
  public void testNullColumnQualifier() throws IOException {
    try (Table t = createTable()) {
      Delete del = new Delete(ROW);
      del.addColumn(FAMILY, null);
      t.delete(del);
      Result r = t.get(new Get(ROW)); //NPE
      assertTrue(r.isEmpty());
    }
  }
}
