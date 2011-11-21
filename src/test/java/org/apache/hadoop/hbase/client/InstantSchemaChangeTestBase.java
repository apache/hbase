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

package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MasterSchemaChangeTracker;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class InstantSchemaChangeTestBase {

  final Log LOG = LogFactory.getLog(getClass());
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected HBaseAdmin admin;
  protected static MiniHBaseCluster miniHBaseCluster = null;
  protected Configuration conf;
  protected static MasterSchemaChangeTracker msct = null;

  protected final byte [] row = Bytes.toBytes("row");
  protected final byte [] qualifier = Bytes.toBytes("qualifier");
  final byte [] value = Bytes.toBytes("value");

  @Before
  public void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.instant.schema.alter.enabled", true);
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.instant.schema.janitor.period", 10000);
    TEST_UTIL.getConfiguration().setInt("hbase.instant.schema.alter.timeout", 30000);
    //
    miniHBaseCluster = TEST_UTIL.startMiniCluster(2,5);
    msct = TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    this.admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

  }

  @After
  public void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Find the RS that is currently holding our online region.
   * @param tableName
   * @return
   */
  protected HRegionServer findRSWithOnlineRegionFor(String tableName) {
    List<JVMClusterUtil.RegionServerThread> rsThreads =
        miniHBaseCluster.getLiveRegionServerThreads();
    for (JVMClusterUtil.RegionServerThread rsT : rsThreads) {
      HRegionServer rs = rsT.getRegionServer();
      List<HRegion> regions = rs.getOnlineRegions(Bytes.toBytes(tableName));
      if (regions != null && !regions.isEmpty()) {
        return rs;
      }
    }
    return null;
  }

  protected void waitForSchemaChangeProcess(final String tableName)
      throws KeeperException, InterruptedException {
    waitForSchemaChangeProcess(tableName, 10000);
  }

  /**
   * This a pretty low cost signalling mechanism. It is quite possible that we will
   * miss out the ZK node creation signal as in some cases the schema change process
   * happens rather quickly and our thread waiting for ZK node creation might wait forver.
   * The fool-proof strategy would be to directly listen for ZK events.
   * @param tableName
   * @throws KeeperException
   * @throws InterruptedException
   */
  protected void waitForSchemaChangeProcess(final String tableName, final long waitTimeMills)
      throws KeeperException, InterruptedException {
    LOG.info("Waiting for ZK node creation for table = " + tableName);
    final MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    final Runnable r = new Runnable() {
      public void run() {
        try {
          while(!msct.doesSchemaChangeNodeExists(tableName)) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        } catch (KeeperException ke) {
            ke.printStackTrace();
        }
        LOG.info("Waiting for ZK node deletion for table = " + tableName);
        try {
          while(msct.doesSchemaChangeNodeExists(tableName)) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
          }
        }  catch (KeeperException ke) {
            ke.printStackTrace();
        }
      }
    };
    Thread t = new Thread(r);
    t.start();
    if (waitTimeMills > 0) {
      t.join(waitTimeMills);
    }  else {
      t.join(10000);
    }
  }

  protected HTable createTableAndValidate(String tableName) throws IOException {
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start createTableAndValidate()");
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    HTableDescriptor[] tables = admin.listTables();
    int numTables = 0;
    if (tables != null) {
      numTables = tables.length;
    }
    HTable ht = TEST_UTIL.createTable(Bytes.toBytes(tableName),
      HConstants.CATALOG_FAMILY);
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
    LOG.info("created table = " + tableName);
    return ht;
  }

}