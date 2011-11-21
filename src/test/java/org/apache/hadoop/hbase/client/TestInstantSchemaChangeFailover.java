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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MasterSchemaChangeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestInstantSchemaChangeFailover {

  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;
  private static MiniHBaseCluster miniHBaseCluster = null;
  private Configuration conf;
  private ZooKeeperWatcher zkw;
  private static MasterSchemaChangeTracker msct = null;

  private final byte [] row = Bytes.toBytes("row");
  private final byte [] qualifier = Bytes.toBytes("qualifier");
  final byte [] value = Bytes.toBytes("value");

  @Before
  public void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setBoolean("hbase.instant.schema.alter.enabled", true);
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
   * This a pretty low cost signalling mechanism. It is quite possible that we will
   * miss out the ZK node creation signal as in some cases the schema change process
   * happens rather quickly and our thread waiting for ZK node creation might wait forver.
   * The fool-proof strategy would be to directly listen for ZK events.
   * @param tableName
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void waitForSchemaChangeProcess(final String tableName)
      throws KeeperException, InterruptedException {
    LOG.info("Waiting for ZK node creation for table = " + tableName);
    final MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    final Runnable r = new Runnable() {
      public void run() {
        try {
          while(!msct.doesSchemaChangeNodeExists(tableName)) {
            try {
              Thread.sleep(20);
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
              Thread.sleep(20);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        } catch (KeeperException ke) {
         ke.printStackTrace();
        }
      }
    };
    Thread t = new Thread(r);
    t.start();
    t.join(10000);
  }


  /**
   * Kill a random RS and see that the schema change can succeed.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test (timeout=50000)
  public void testInstantSchemaChangeWhileRSCrash() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("Start testInstantSchemaChangeWhileRSCrash()");
    zkw = miniHBaseCluster.getMaster().getZooKeeperWatcher();

    final String tableName = "TestRSCrashDuringSchemaChange";
    HTable ht = createTableAndValidate(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("family2");
    admin.addColumn(Bytes.toBytes(tableName), hcd);

    miniHBaseCluster.getRegionServer(0).abort("Killing while instant schema change");
    // Let the dust settle down
    Thread.sleep(10000);
    waitForSchemaChangeProcess(tableName);
    Put put2 = new Put(row);
    put2.add(Bytes.toBytes("family2"), qualifier, value);
    ht.put(put2);

    Get get2 = new Get(row);
    get2.addColumn(Bytes.toBytes("family2"), qualifier);
    Result r2 = ht.get(get2);
    byte[] tvalue2 = r2.getValue(Bytes.toBytes("family2"), qualifier);
    int result2 = Bytes.compareTo(value, tvalue2);
    assertEquals(result2, 0);
    String nodePath = msct.getSchemaChangeNodePathForTable("TestRSCrashDuringSchemaChange");
    assertTrue(ZKUtil.checkExists(zkw, nodePath) == -1);
    LOG.info("result2 = " + result2);
    LOG.info("end testInstantSchemaChangeWhileRSCrash()");
  }

  /**
   * Randomly bring down/up RS servers while schema change is in progress. This test
   * is same as the above one but the only difference is that we intent to kill and start
   * new RS instances while a schema change is in progress.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test (timeout=70000)
  public void testInstantSchemaChangeWhileRandomRSCrashAndStart() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("Start testInstantSchemaChangeWhileRandomRSCrashAndStart()");
    miniHBaseCluster.getRegionServer(4).abort("Killing RS 4");
    // Start a new RS before schema change .
    // Commenting the start RS as it is failing with DFS user permission NPE.
    //miniHBaseCluster.startRegionServer();

    // Let the dust settle
    Thread.sleep(10000);
    final String tableName = "testInstantSchemaChangeWhileRandomRSCrashAndStart";
    HTable ht = createTableAndValidate(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("family2");
    admin.addColumn(Bytes.toBytes(tableName), hcd);
    // Kill 2 RS now.
    miniHBaseCluster.getRegionServer(2).abort("Killing RS 2");
    // Let the dust settle
    Thread.sleep(10000);
    // We will be left with only one RS.
    waitForSchemaChangeProcess(tableName);
    assertFalse(msct.doesSchemaChangeNodeExists(tableName));
    Put put2 = new Put(row);
    put2.add(Bytes.toBytes("family2"), qualifier, value);
    ht.put(put2);

    Get get2 = new Get(row);
    get2.addColumn(Bytes.toBytes("family2"), qualifier);
    Result r2 = ht.get(get2);
    byte[] tvalue2 = r2.getValue(Bytes.toBytes("family2"), qualifier);
    int result2 = Bytes.compareTo(value, tvalue2);
    assertEquals(result2, 0);
    LOG.info("result2 = " + result2);
    LOG.info("end testInstantSchemaChangeWhileRandomRSCrashAndStart()");
  }

  /**
   * Test scenario where primary master is brought down while processing an
   * alter request. This is harder one as it is very difficult the time this.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */

  @Test (timeout=50000)
  public void testInstantSchemaChangeWhileMasterFailover() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("Start testInstantSchemaChangeWhileMasterFailover()");
    //Thread.sleep(5000);

    final String tableName = "testInstantSchemaChangeWhileMasterFailover";
    HTable ht = createTableAndValidate(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("family2");
    admin.addColumn(Bytes.toBytes(tableName), hcd);
    // Kill primary master now.
    Thread.sleep(50);
    miniHBaseCluster.getMaster().abort("Aborting master now", new Exception("Schema exception"));

    // It may not be possible for us to check the schema change status
    // using waitForSchemaChangeProcess as our ZK session in MasterSchemachangeTracker will be
    // lost when master dies and hence may not be accurate. So relying on old-fashioned
    // sleep here.
    Thread.sleep(25000);
    Put put2 = new Put(row);
    put2.add(Bytes.toBytes("family2"), qualifier, value);
    ht.put(put2);

    Get get2 = new Get(row);
    get2.addColumn(Bytes.toBytes("family2"), qualifier);
    Result r2 = ht.get(get2);
    byte[] tvalue2 = r2.getValue(Bytes.toBytes("family2"), qualifier);
    int result2 = Bytes.compareTo(value, tvalue2);
    assertEquals(result2, 0);
    LOG.info("result2 = " + result2);
    LOG.info("end testInstantSchemaChangeWhileMasterFailover()");
  }

  /**
   * TEst the master fail over during a schema change request in ZK.
   * We create a fake schema change request in ZK and abort the primary master
   * mid-flight to simulate a master fail over scenario during a mid-flight
   * schema change process. The new master's schema janitor will eventually
   * cleanup this fake request after time out.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Ignore
  @Test
  public void testInstantSchemaOperationsInZKForMasterFailover() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("testInstantSchemaOperationsInZKForMasterFailover() ");
    String tableName = "testInstantSchemaOperationsInZKForMasterFailover";

    conf = TEST_UTIL.getConfiguration();
    MasterSchemaChangeTracker activesct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    activesct.createSchemaChangeNode(tableName, 10);
    LOG.debug(activesct.getSchemaChangeNodePathForTable(tableName)
        + " created");
    assertTrue(activesct.doesSchemaChangeNodeExists(tableName));
    // Kill primary master now.
    miniHBaseCluster.getMaster().abort("Aborting master now", new Exception("Schema exception"));
    // wait for 50 secs. This is so that our schema janitor from fail-over master will kick-in and
    // cleanup this failed/expired schema change request.
    Thread.sleep(50000);
    MasterSchemaChangeTracker newmsct = miniHBaseCluster.getMaster().getSchemaChangeTracker();
    assertFalse(newmsct.doesSchemaChangeNodeExists(tableName));
    LOG.debug(newmsct.getSchemaChangeNodePathForTable(tableName)
        + " deleted");
    LOG.info("END testInstantSchemaOperationsInZKForMasterFailover() ");
  }

  private HTable createTableAndValidate(String tableName) throws IOException {
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start createTableAndValidate()");
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


