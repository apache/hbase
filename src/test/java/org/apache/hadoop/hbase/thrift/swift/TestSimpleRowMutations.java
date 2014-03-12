/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.thrift.swift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableAsyncInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSimpleRowMutations {
  byte[] tableName = Bytes.toBytes("testSimpleRowMutationsUsingSwift");
  byte[] family1 = Bytes.toBytes("family1");
  byte[] family2 = Bytes.toBytes("family2");
  byte[] qual = Bytes.toBytes("qual");
  byte[] row = Bytes.toBytes("rowkey");

  protected final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 1;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRowMutationsBuilder() throws IOException {
    HTable table =
        TEST_UTIL.createTable(tableName, new byte[][] {family1, family2});
    RowMutations b = new RowMutations(row);
    Put dp = new Put(row);
    dp.add(family2, qual, family1);
    table.put(dp);

    Put p = new Put(row);
    p.add(family1, qual, family1);
    b.add(p);

    Delete d = new Delete(row);
    d.deleteFamily(family2);
    b.add(d);

    RowMutations arm = b;
    table.mutateRow(arm);

    Get g = new Get(row);
    g.addColumn(family1, qual);
    g.addColumn(family2, qual);

    Result r = table.get(g);
    assertNotNull(r.getValue(family1, qual));
    assertEquals(
        Bytes.BYTES_COMPARATOR.compare(r.getValue(family1, qual), family1), 0);
    assertNull(r.getValue(family2, qual));

    int num = 10;
    List<RowMutations> mutations = new ArrayList<RowMutations>();
    byte[] qualifier = Bytes.toBytes("qualifier");
    for (int i = 0; i<num; i++) {
      byte[] rowi = Bytes.toBytes("row" + i);
      Put pi = new Put(rowi);
      byte[] valuei = Bytes.toBytes("value" + i);
      pi.add(family1, qualifier, valuei);
      RowMutations mut = new RowMutations.Builder(rowi).add(pi).create();
      mutations.add(mut);
    }
    table.mutateRow(mutations);
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    HRegionInfo info =
        table.getRegionLocation(Bytes.toBytes("row")).getRegionInfo();
    for (int i = 0; i < num; i++) {
      byte[] rowi = Bytes.toBytes("row" + i);
      Get gi = new Get(rowi);
      byte[] expectedValue = Bytes.toBytes("value" + i);
      byte[] actualValue = server.get(info.getRegionName(),
          gi).getValue(family1, qualifier);
      System.out.println("expectedValue : " + Bytes.toString(expectedValue) +
          " actualValue : " + Bytes.toString(actualValue));
      assertTrue(Bytes.equals(expectedValue, actualValue));
    }
  }

  /**
   * Test asynchronous row mutation.
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testAsynchronousRowMutation()
      throws IOException, ExecutionException, InterruptedException {
    HTableAsyncInterface table =
        TEST_UTIL.createTable(tableName, new byte[][] {family1, family2});
    RowMutations.Builder b = new RowMutations.Builder(row);
    Put dp = new Put(row);
    dp.add(family2, qual, family1);
    table.put(dp);

    Put p = new Put(row);
    p.add(family1, qual, family1);
    b = b.add(p);

    Delete d = new Delete(row);
    d.deleteColumn(family2, qual);
    b.add(d);

    RowMutations arm = b.create();
    table.mutateRowAsync(arm).get();

    Get g = new Get(row);
    g.addColumn(family1, qual);
    g.addColumn(family2, qual);

    Result r = table.get(g);
    assertEquals(
        Bytes.BYTES_COMPARATOR.compare(r.getValue(family1, qual), family1), 0);
    assertNull(r.getValue(family2, qual));
  }
}
