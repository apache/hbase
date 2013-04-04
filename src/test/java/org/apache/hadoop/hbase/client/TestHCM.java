/*
 * Copyright 2010 The Apache Software Foundation
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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class is for testing HCM features
 */
public class TestHCM {
  private static final Log LOG = LogFactory.getLog(TestHCM.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");

  private static final int REGION_SERVERS = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 3);
    TEST_UTIL.getConfiguration().set("hbase.loadbalancer.impl",
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");
    TEST_UTIL.getConfiguration().setBoolean("hbase.client.record.context", true);
    TEST_UTIL.startMiniCluster(REGION_SERVERS);
  }

  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  @Test
  public void testRegionCaching() throws Exception{

    HTable table = TEST_UTIL.createTable(TABLE_NAME, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.TableServers conn =
        (HConnectionManager.TableServers) table.getConnectionAndResetOperationContext();
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    conn.deleteCachedLocation(TABLE_NAME, ROW, null);
    HRegionLocation rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);

  }

  /**
   * Test that on failures, HBase client reports the errors correctly.
   * @throws Exception
   */
  @Test
  public void testClientFailureReporting() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientFailureReporting"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    setupFailure(table,
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size());
    try {
      table.put(put);
      assertNull("Put should not succeed");
    } catch (IOException e) {
      verifyFailure(table, e);

    } finally {
      ServerManager.clearRSBlacklistInTest();
      waitForRegionsToGetReAssigned();
    }
  }

  private void waitForRegionsToGetReAssigned() {
    while(
        TEST_UTIL.getHBaseCluster().getActiveMaster().
          getRegionManager().areAllMetaRegionsOnline() == false) {}
  }


  /**
   * Test that on failures, HBase client reports the errors correctly.
   * @throws Exception
   */
  @Test
  public void testClientFailureReportingGet() throws Exception {
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientFailureReportingGet"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);


    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    setupFailure(table,
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size());
    try {
      Get get = new Get(ROW);
      table.get(get);
      assertNull("Get should not succeed");
    } catch (IOException e) {
      verifyFailure(table, e);
    } finally {
      ServerManager.clearRSBlacklistInTest();
      waitForRegionsToGetReAssigned();
    }
  }

  /**
   * Test that on failures, HBase client reports the errors correctly.
   * @throws Exception
   */
  @Test
  public void testClientFailureReportingDelete() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientFailureReportingDelete"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    setupFailure(table,
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size());
    try {
      Delete delete = new Delete(ROW);
      table.delete(delete);
      assertNull("Delete should not succeed");
    } catch (IOException e) {
      verifyFailure(table, e);
    } finally {
      ServerManager.clearRSBlacklistInTest();
      waitForRegionsToGetReAssigned();
    }

  }

  /**
   * Simulates a case where the get operation succeeds after a retry
   * @throws Exception
   */
  @Test
  public void testClientGetSuccess() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientSuccess"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    setupFailure(table, 0);
    try {
      Get g = new Get(ROW);
      table.get(g);
      List<OperationContext> op = table.getAndResetOperationContext();
      assertTrue(op.size() > 0);
      for (OperationContext p : op) {
        assertTrue(p.getError() != null);
        assertTrue(p.getLocation() != null);
      }
    } catch (IOException e) {

    } finally {
      waitForRegionsToGetReAssigned();
    }
  }

  /**
   * Assign noOfRS to the dead maps list, so that no regions are assigned to
   * them and then kill the current region server.
   * @param table
   * @param noOfRS
   * @throws IOException
   */
  private void setupFailure(HTable table, int noOfRS) throws IOException {
    HRegionLocation regLoc = table.getRegionLocation(ROW);
    List<RegionServerThread> regionServers =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();

    // Add all region servers to the black list to prevent fail overs.
    for(int i = 0; i < noOfRS; i++) {
      ServerManager.blacklistRSHostPortInTest(
          regionServers.get(i).getRegionServer().getHServerInfo().getHostnamePort());
    }

    // abort the region server
    int srcRSIdx = TEST_UTIL.getHBaseCluster().
        getServerWith(regLoc.getRegionInfo().getRegionName());
    TEST_UTIL.getHBaseCluster().stopRegionServer(srcRSIdx);
  }

  private void verifyFailure(HTable table, Exception e) {

    List<OperationContext> context = table.getAndResetOperationContext();
    assertTrue(context.size() != 0);
    for (OperationContext c : context) {
      assertTrue(c.getError() != null);
      assertTrue(c.getLocation() != null);
    }
    assertTrue(e instanceof RetriesExhaustedException);
    RetriesExhaustedException exp = (RetriesExhaustedException)(e);
    assertTrue(exp.getRegionNames() != null);
    LOG.info(exp.getFailureInfo().values());
  }
}
