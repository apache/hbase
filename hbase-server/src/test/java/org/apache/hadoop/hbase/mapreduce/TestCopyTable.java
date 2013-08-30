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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for the CopyTable M/R tool
 */
@Category(LargeTests.class)
public class TestCopyTable {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Simple end-to-end test
   * @throws Exception
   */
  @Test
  public void testCopyTable() throws Exception {
    final byte[] TABLENAME1 = Bytes.toBytes("testCopyTable1");
    final byte[] TABLENAME2 = Bytes.toBytes("testCopyTable2");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");

    HTable t1 = TEST_UTIL.createTable(TABLENAME1, FAMILY);
    HTable t2 = TEST_UTIL.createTable(TABLENAME2, FAMILY);

    // put rows into the first table
    for (int i = 0; i < 10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.add(FAMILY, COLUMN1, COLUMN1);
      t1.put(p);
    }

    CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());

    assertEquals(
      0,
      copy.run(new String[] { "--new.name=" + Bytes.toString(TABLENAME2),
          Bytes.toString(TABLENAME1) }));

    // verify the data was copied into table 2
    for (int i = 0; i < 10; i++) {
      Get g = new Get(Bytes.toBytes("row" + i));
      Result r = t2.get(g);
      assertEquals(1, r.size());
      assertTrue(CellUtil.matchingQualifier(r.raw()[0], COLUMN1));
    }
    
    t1.close();
    t2.close();
    TEST_UTIL.deleteTable(TABLENAME1);
    TEST_UTIL.deleteTable(TABLENAME2);
  }

  @Test
  public void testStartStopRow() throws Exception {
    final byte[] TABLENAME1 = Bytes.toBytes("testStartStopRow1");
    final byte[] TABLENAME2 = Bytes.toBytes("testStartStopRow2");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");
    final byte[] ROW0 = Bytes.toBytes("row0");
    final byte[] ROW1 = Bytes.toBytes("row1");
    final byte[] ROW2 = Bytes.toBytes("row2");

    HTable t1 = TEST_UTIL.createTable(TABLENAME1, FAMILY);
    HTable t2 = TEST_UTIL.createTable(TABLENAME2, FAMILY);

    // put rows into the first table
    Put p = new Put(ROW0);
    p.add(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);
    p = new Put(ROW1);
    p.add(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);
    p = new Put(ROW2);
    p.add(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);

    CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());
    assertEquals(
      0,
      copy.run(new String[] { "--new.name=" + Bytes.toString(TABLENAME2), "--startrow=row1",
          "--stoprow=row2", Bytes.toString(TABLENAME1) }));

    // verify the data was copied into table 2
    // row1 exist, row0, row2 do not exist
    Get g = new Get(ROW1);
    Result r = t2.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.raw()[0], COLUMN1));

    g = new Get(ROW0);
    r = t2.get(g);
    assertEquals(0, r.size());
    
    g = new Get(ROW2);
    r = t2.get(g);
    assertEquals(0, r.size());
    
    t1.close();
    t2.close();
    TEST_UTIL.deleteTable(TABLENAME1);
    TEST_UTIL.deleteTable(TABLENAME2);
  }
}
