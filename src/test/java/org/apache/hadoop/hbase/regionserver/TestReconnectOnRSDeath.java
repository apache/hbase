/**
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;

/**
 * This is just a test case to verify that the client is transparent to the
 * failure of the RS serving the region that it is interested in. This happens
 * perfectly fine when using Hadoop RPC, we just wanted to double check this
 * in thrift.
 */
@Category(MediumTests.class)
public class TestReconnectOnRSDeath extends TestCase {
  public static final Log LOG =
    LogFactory.getLog(TestReconnectOnRSDeath.class.getName());
  private static final int SLAVES = 3;
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  public static final byte[][] FAMILIES = { Bytes.toBytes("f1"),
    Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4"),
    Bytes.toBytes("f5") };
  public static final byte[] TABLENAME = Bytes.toBytes("t1");


  @Before
  public void setUp() throws IOException, InterruptedException {
    // Turn on using Thrift for Client <-> RS communication. Just to be sure.
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
      true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.REGIONSERVER_USE_THRIFT,
      true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  // A helper function to create puts.
  private static Put createPut(int familyNum, int putNum) {
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    Put p = new Put(row);
    p.add(FAMILIES[familyNum -1], qf, val);
    return p;
  }

  // A helper function to create puts.
  private  static Get createGet(int familyNum, int putNum) {
    byte[] row = Bytes.toBytes("row" + familyNum + "-" + putNum);
    return new Get(row);
  }

  // A helper function to verify that the edits went through.
  private static void verifyEdit(int familyNum, int putNum, HTable table)
    throws IOException {
    Result r = table.get(createGet(familyNum, putNum));
    byte[] family = FAMILIES[familyNum - 1];
    byte[] qf = Bytes.toBytes("q" + familyNum);
    byte[] val = Bytes.toBytes("val" + familyNum + "-" + putNum);
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum),
      r.getFamilyMap(family));
    assertNotNull(("Missing Put#" + putNum + " for CF# " + familyNum),
      r.getFamilyMap(family).get(qf));
    assertTrue(("Incorrect value for Put#" + putNum + " for CF# " + familyNum),
      Arrays.equals(r.getFamilyMap(family).get(qf), val));
  }

  /**
   * We will do a bunch of puts / gets to a specific region, and then kill
   * the RS hosting that region. Then immediately resume doing the operations.
   * The dying of the RS should be transparent to the clients.
   */
  @Test
  public void testIfClientsCanReconnectOnRSDeath() throws IOException {
    HTable table = TEST_UTIL.createTable(TABLENAME, FAMILIES);
    // Do a bunch of puts.
    for (int i = 0; i < 100; i++) {
      for (int j = 1; j <= 3; j++) {
        table.put(createPut(j, i));
      }
      table.flushCommits();
    }

    // Kill the region servers which are serving this table.
    for (HRegion region :
         TEST_UTIL.getMiniHBaseCluster().getRegions(TABLENAME)) {
      HRegionServer hrs = region.getRegionServer();
      hrs.abort("Testing");
    }

    // Do the corresponding gets for the puts.
    for (int i = 0; i < 100; i++) {
      for (int j = 1; j <= 3; j++) {
        verifyEdit(j, i, table);
      }
    }

  }
}
