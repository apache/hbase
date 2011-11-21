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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MasterSchemaChangeTracker;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestInstantSchemaChange extends InstantSchemaChangeTestBase {

  final Log LOG = LogFactory.getLog(getClass());

  @Test
  public void testInstantSchemaChangeForModifyTable() throws IOException,
      KeeperException, InterruptedException {

    String tableName = "testInstantSchemaChangeForModifyTable";
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start testInstantSchemaChangeForModifyTable()");
    HTable ht = createTableAndValidate(tableName);

    String newFamily = "newFamily";
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(newFamily));

    admin.modifyTable(Bytes.toBytes(tableName), htd);
    waitForSchemaChangeProcess(tableName);
    assertFalse(msct.doesSchemaChangeNodeExists(tableName));

    Put put1 = new Put(row);
    put1.add(Bytes.toBytes(newFamily), qualifier, value);
    ht.put(put1);

    Get get1 = new Get(row);
    get1.addColumn(Bytes.toBytes(newFamily), qualifier);
    Result r = ht.get(get1);
    byte[] tvalue = r.getValue(Bytes.toBytes(newFamily), qualifier);
    int result = Bytes.compareTo(value, tvalue);
    assertEquals(result, 0);
    LOG.info("END testInstantSchemaChangeForModifyTable()");

  }

  @Test
  public void testInstantSchemaChangeForAddColumn() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("Start testInstantSchemaChangeForAddColumn() ");
    String tableName = "testSchemachangeForAddColumn";
    HTable ht = createTableAndValidate(tableName);
    String newFamily = "newFamily";
    HColumnDescriptor hcd = new HColumnDescriptor("newFamily");

    admin.addColumn(Bytes.toBytes(tableName), hcd);
    waitForSchemaChangeProcess(tableName);
    assertFalse(msct.doesSchemaChangeNodeExists(tableName));

    Put put1 = new Put(row);
    put1.add(Bytes.toBytes(newFamily), qualifier, value);
    LOG.info("******** Put into new column family ");
    ht.put(put1);

    Get get1 = new Get(row);
    get1.addColumn(Bytes.toBytes(newFamily), qualifier);
    Result r = ht.get(get1);
    byte[] tvalue = r.getValue(Bytes.toBytes(newFamily), qualifier);
    LOG.info(" Value put = " + value + " value from table = " + tvalue);
    int result = Bytes.compareTo(value, tvalue);
    assertEquals(result, 0);
    LOG.info("End testInstantSchemaChangeForAddColumn() ");

 }

  @Test
  public void testInstantSchemaChangeForModifyColumn() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("Start testInstantSchemaChangeForModifyColumn() ");
    String tableName = "testSchemachangeForModifyColumn";
    createTableAndValidate(tableName);

    HColumnDescriptor hcd = new HColumnDescriptor(HConstants.CATALOG_FAMILY);
    hcd.setMaxVersions(99);
    hcd.setBlockCacheEnabled(false);

    admin.modifyColumn(Bytes.toBytes(tableName), hcd);
    waitForSchemaChangeProcess(tableName);
    assertFalse(msct.doesSchemaChangeNodeExists(tableName));

    List<HRegion> onlineRegions
        = miniHBaseCluster.getRegions(Bytes.toBytes("testSchemachangeForModifyColumn"));
    for (HRegion onlineRegion : onlineRegions) {
      HTableDescriptor htd = onlineRegion.getTableDesc();
      HColumnDescriptor tableHcd = htd.getFamily(HConstants.CATALOG_FAMILY);
      assertTrue(tableHcd.isBlockCacheEnabled() == false);
      assertEquals(tableHcd.getMaxVersions(), 99);
    }
    LOG.info("End testInstantSchemaChangeForModifyColumn() ");

 }

  @Test
  public void testInstantSchemaChangeForDeleteColumn() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("Start testInstantSchemaChangeForDeleteColumn() ");
    String tableName = "testSchemachangeForDeleteColumn";
    int numTables = 0;
    HTableDescriptor[] tables = admin.listTables();
    if (tables != null) {
      numTables = tables.length;
    }

    byte[][] FAMILIES = new byte[][] {
      Bytes.toBytes("A"), Bytes.toBytes("B"), Bytes.toBytes("C") };

    HTable ht = TEST_UTIL.createTable(Bytes.toBytes(tableName),
      FAMILIES);
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);
    LOG.info("Table testSchemachangeForDeleteColumn created");

    admin.deleteColumn(tableName, "C");

    waitForSchemaChangeProcess(tableName);
    assertFalse(msct.doesSchemaChangeNodeExists(tableName));
    HTableDescriptor modifiedHtd = this.admin.getTableDescriptor(Bytes.toBytes(tableName));
    HColumnDescriptor hcd = modifiedHtd.getFamily(Bytes.toBytes("C"));
    assertTrue(hcd == null);
    LOG.info("End testInstantSchemaChangeForDeleteColumn() ");
 }

  @Test
  public void testInstantSchemaChangeWhenTableIsNotEnabled() throws IOException,
      KeeperException {
    final String tableName = "testInstantSchemaChangeWhenTableIsDisabled";
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start testInstantSchemaChangeWhenTableIsDisabled()");
    HTable ht = createTableAndValidate(tableName);
    // Disable table
    admin.disableTable("testInstantSchemaChangeWhenTableIsDisabled");
    // perform schema changes
    HColumnDescriptor hcd = new HColumnDescriptor("newFamily");
    admin.addColumn(Bytes.toBytes(tableName), hcd);
    MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    assertTrue(msct.doesSchemaChangeNodeExists(tableName) == false);
  }

  /**
   * Test that when concurrent alter requests are received for a table we don't miss any.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test
  public void testConcurrentInstantSchemaChangeForAddColumn() throws IOException,
      KeeperException, InterruptedException {
    final String tableName = "testConcurrentInstantSchemaChangeForModifyTable";
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start testConcurrentInstantSchemaChangeForModifyTable()");
    HTable ht = createTableAndValidate(tableName);

    Runnable run1 = new Runnable() {
      public void run() {
        HColumnDescriptor hcd = new HColumnDescriptor("family1");
        try {
          admin.addColumn(Bytes.toBytes(tableName), hcd);
        } catch (IOException ioe) {
          ioe.printStackTrace();

        }
      }
    };
    Runnable run2 = new Runnable() {
      public void run() {
        HColumnDescriptor hcd = new HColumnDescriptor("family2");
        try {
          admin.addColumn(Bytes.toBytes(tableName), hcd);
        } catch (IOException ioe) {
          ioe.printStackTrace();

        }
      }
    };

    run1.run();
    // We have to add a sleep here as in concurrent scenarios the HTD update
    // in HDFS fails and returns with null HTD. This needs to be investigated,
    // but it doesn't impact the instant alter functionality in any way.
    Thread.sleep(100);
    run2.run();

    waitForSchemaChangeProcess(tableName);

    Put put1 = new Put(row);
    put1.add(Bytes.toBytes("family1"), qualifier, value);
    ht.put(put1);

    Get get1 = new Get(row);
    get1.addColumn(Bytes.toBytes("family1"), qualifier);
    Result r = ht.get(get1);
    byte[] tvalue = r.getValue(Bytes.toBytes("family1"), qualifier);
    int result = Bytes.compareTo(value, tvalue);
    assertEquals(result, 0);
    Thread.sleep(10000);

    Put put2 = new Put(row);
    put2.add(Bytes.toBytes("family2"), qualifier, value);
    ht.put(put2);

    Get get2 = new Get(row);
    get2.addColumn(Bytes.toBytes("family2"), qualifier);
    Result r2 = ht.get(get2);
    byte[] tvalue2 = r2.getValue(Bytes.toBytes("family2"), qualifier);
    int result2 = Bytes.compareTo(value, tvalue2);
    assertEquals(result2, 0);
    LOG.info("END testConcurrentInstantSchemaChangeForModifyTable()");
  }

  /**
   * The schema change request blocks while a LB run is in progress. This
   * test validates this behavior.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test
  public void testConcurrentInstantSchemaChangeAndLoadBalancerRun() throws IOException,
      InterruptedException, KeeperException {
    final String tableName = "testInstantSchemaChangeWithLoadBalancerRunning";
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start testInstantSchemaChangeWithLoadBalancerRunning()");
    final String newFamily = "newFamily";
    HTable ht = createTableAndValidate(tableName);
    final MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();


    Runnable balancer = new Runnable() {
      public void run() {
        // run the balancer now.
        miniHBaseCluster.getMaster().balance();
      }
    };

    Runnable schemaChanger = new Runnable() {
      public void run() {
        HColumnDescriptor hcd = new HColumnDescriptor(newFamily);
        try {
          admin.addColumn(Bytes.toBytes(tableName), hcd);
        } catch (IOException ioe) {
          ioe.printStackTrace();

        }
      }
    };

    balancer.run();
    schemaChanger.run();
    waitForSchemaChangeProcess(tableName, 40000);
    assertFalse(msct.doesSchemaChangeNodeExists(tableName));

    Put put1 = new Put(row);
    put1.add(Bytes.toBytes(newFamily), qualifier, value);
    LOG.info("******** Put into new column family ");
    ht.put(put1);
    ht.flushCommits();

    LOG.info("******** Get from new column family ");
    Get get1 = new Get(row);
    get1.addColumn(Bytes.toBytes(newFamily), qualifier);
    Result r = ht.get(get1);
    byte[] tvalue = r.getValue(Bytes.toBytes(newFamily), qualifier);
    LOG.info(" Value put = " + value + " value from table = " + tvalue);
    int result = Bytes.compareTo(value, tvalue);
    assertEquals(result, 0);

    LOG.info("End testInstantSchemaChangeWithLoadBalancerRunning() ");
  }


  /**
   * This test validates two things. One is that the LoadBalancer does not run when a schema
   * change process is in progress. The second thing is that it also checks that failed/expired
   * schema changes are expired to unblock the load balancer run.
   *
   */
  @Test (timeout=70000)
  public void testLoadBalancerBlocksDuringSchemaChangeRequests() throws KeeperException,
      IOException, InterruptedException {
    LOG.info("Start testConcurrentLoadBalancerSchemaChangeRequests() ");
    final MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    // Test that the load balancer does not run while an in-flight schema
    // change operation is in progress.
    // Simulate a new schema change request.
    msct.createSchemaChangeNode("testLoadBalancerBlocks", 0);
    // The schema change node is created.
    assertTrue(msct.doesSchemaChangeNodeExists("testLoadBalancerBlocks"));
    // Now, request an explicit LB run.

    Runnable balancer1 = new Runnable() {
      public void run() {
        // run the balancer now.
        miniHBaseCluster.getMaster().balance();
      }
    };
    balancer1.run();

    // Load balancer should not run now.
    assertTrue(miniHBaseCluster.getMaster().isLoadBalancerRunning() == false);
    LOG.debug("testConcurrentLoadBalancerSchemaChangeRequests Asserted");
    LOG.info("End testConcurrentLoadBalancerSchemaChangeRequests() ");
  }

  /**
   * Test that instant schema change blocks while LB is running.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=10000)
  public void testInstantSchemaChangeBlocksDuringLoadBalancerRun() throws KeeperException,
      IOException, InterruptedException {
    final MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();

    final String tableName = "testInstantSchemaChangeBlocksDuringLoadBalancerRun";
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start testInstantSchemaChangeBlocksDuringLoadBalancerRun()");
    final String newFamily = "newFamily";
    createTableAndValidate(tableName);

    // Test that the schema change request does not run while an in-flight LB run
    // is in progress.
    // First, request an explicit LB run.

    Runnable balancer1 = new Runnable() {
      public void run() {
        // run the balancer now.
        miniHBaseCluster.getMaster().balance();
      }
    };

    Runnable schemaChanger = new Runnable() {
      public void run() {
        HColumnDescriptor hcd = new HColumnDescriptor(newFamily);
        try {
          admin.addColumn(Bytes.toBytes(tableName), hcd);
        } catch (IOException ioe) {
          ioe.printStackTrace();

        }
      }
    };

    Thread t1 = new Thread(balancer1);
    Thread t2 = new Thread(schemaChanger);
    t1.start();
    t2.start();

    // check that they both happen concurrently
    Runnable balancerCheck = new Runnable() {
      public void run() {
        // check whether balancer is running.
        while(!miniHBaseCluster.getMaster().isLoadBalancerRunning()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        try {
           assertFalse(msct.doesSchemaChangeNodeExists("testSchemaChangeBlocks"));
        } catch (KeeperException ke) {
          ke.printStackTrace();
        }
        LOG.debug("Load Balancer is now running or skipped");
        while(miniHBaseCluster.getMaster().isLoadBalancerRunning()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        assertTrue(miniHBaseCluster.getMaster().isLoadBalancerRunning() == false);
        try {
          assertTrue(msct.doesSchemaChangeNodeExists("testSchemaChangeBlocks"));
        } catch (KeeperException ke) {

        }

      }
    };

    Thread t = new Thread(balancerCheck);
    t.start();
    t.join(1000);
    // Load balancer should not run now.
    //assertTrue(miniHBaseCluster.getMaster().isLoadBalancerRunning() == false);
    // Schema change request node should now exist.
   // assertTrue(msct.doesSchemaChangeNodeExists("testSchemaChangeBlocks"));
    LOG.debug("testInstantSchemaChangeBlocksDuringLoadBalancerRun Asserted");
    LOG.info("End testInstantSchemaChangeBlocksDuringLoadBalancerRun() ");
  }

  /**
   * To test the schema janitor (that it cleans expired/failed schema alter attempts) we
   * simply create a fake table (that doesn't exist, with fake number of online regions) in ZK.
   * This schema alter request will time out (after 30 seconds) and our janitor will clean it up.
   * regions
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test
  public void testInstantSchemaJanitor() throws IOException,
      KeeperException, InterruptedException {
    LOG.info("testInstantSchemaWithFailedExpiredOperations() ");
    String fakeTableName = "testInstantSchemaWithFailedExpiredOperations";
    MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    msct.createSchemaChangeNode(fakeTableName, 10);
    LOG.debug(msct.getSchemaChangeNodePathForTable(fakeTableName)
        + " created");
    Thread.sleep(40000);
    assertFalse(msct.doesSchemaChangeNodeExists(fakeTableName));
    LOG.debug(msct.getSchemaChangeNodePathForTable(fakeTableName)
        + " deleted");
    LOG.info("END testInstantSchemaWithFailedExpiredOperations() ");
  }


}