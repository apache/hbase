/**
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(MediumTests.class)
public class TestCheckAndMutate {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCheckAndMutate() throws Exception {
    final TableName tableName = TableName.valueOf("TestPutWithDelete");
    final byte[] rowKey = Bytes.toBytes("12345");
    final byte[] family = Bytes.toBytes("cf");
    Table table = TEST_UTIL.createTable(tableName, family);
    TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
    try {
      // put one row
      Put put = new Put(rowKey);
      put.add(family, Bytes.toBytes("A"), Bytes.toBytes("a"));
      put.add(family, Bytes.toBytes("B"), Bytes.toBytes("b"));
      put.add(family, Bytes.toBytes("C"), Bytes.toBytes("c"));
      table.put(put);
      // get row back and assert the values
      Get get = new Get(rowKey);
      Result result = table.get(get);
      assertTrue("Column A value should be a",
          Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a"));
      assertTrue("Column B value should be b",
          Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b"));
      assertTrue("Column C value should be c",
          Bytes.toString(result.getValue(family, Bytes.toBytes("C"))).equals("c"));

      // put the same row again with C column deleted
      RowMutations rm = new RowMutations(rowKey);
      put = new Put(rowKey);
      put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("a"));
      put.addColumn(family, Bytes.toBytes("B"), Bytes.toBytes("b"));
      rm.add(put);
      Delete del = new Delete(rowKey);
      del.addColumn(family, Bytes.toBytes("C"));
      rm.add(del);
      boolean res = table.checkAndMutate(rowKey, family, Bytes.toBytes("A"), CompareFilter.CompareOp.EQUAL,
          Bytes.toBytes("a"), rm);
      assertTrue(res);

      // get row back and assert the values
      get = new Get(rowKey);
      result = table.get(get);
      assertTrue("Column A value should be a",
          Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a"));
      assertTrue("Column B value should be b",
          Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b"));
      assertTrue("Column C should not exist",
          result.getValue(family, Bytes.toBytes("C")) == null);

      //Test that we get a region level exception
      try {
        Put p = new Put(rowKey);
        p.add(new byte[]{'b', 'o', 'g', 'u', 's'}, new byte[]{'A'},  new byte[0]);
        rm = new RowMutations(rowKey);
        rm.add(p);
        table.checkAndMutate(rowKey, family, Bytes.toBytes("A"), CompareFilter.CompareOp.EQUAL,
            Bytes.toBytes("a"), rm);
        fail("Expected NoSuchColumnFamilyException");
      } catch(NoSuchColumnFamilyException e) {
      }
    } finally {
      table.close();
    }
  }
}