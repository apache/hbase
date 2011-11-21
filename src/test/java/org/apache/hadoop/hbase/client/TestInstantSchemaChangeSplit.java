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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MasterSchemaChangeTracker;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestInstantSchemaChangeSplit extends InstantSchemaChangeTestBase {

  final Log LOG = LogFactory.getLog(getClass());

  /**
   * The objective of the following test is to validate that schema exclusions happen properly.
   * When a RS server dies or crashes(?) mid-flight during a schema refresh, we would exclude
   * all online regions in that RS, as well as the RS itself from schema change process.
   *
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test
  public void testInstantSchemaChangeExclusions() throws IOException,
      KeeperException, InterruptedException {
    MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();
    LOG.info("Start testInstantSchemaChangeExclusions() ");
    String tableName = "testInstantSchemaChangeExclusions";
    HTable ht = createTableAndValidate(tableName);

    HColumnDescriptor hcd = new HColumnDescriptor(HConstants.CATALOG_FAMILY);
    hcd.setMaxVersions(99);
    hcd.setBlockCacheEnabled(false);

    HRegionServer hrs = findRSWithOnlineRegionFor(tableName);
    //miniHBaseCluster.getRegionServer(0).abort("killed for test");
    admin.modifyColumn(Bytes.toBytes(tableName), hcd);
    hrs.abort("Aborting for tests");
    hrs.getSchemaChangeTracker().setSleepTimeMillis(20000);

    //admin.modifyColumn(Bytes.toBytes(tableName), hcd);
    LOG.debug("Waiting for Schema Change process to complete");
    waitForSchemaChangeProcess(tableName, 15000);
    assertEquals(msct.doesSchemaChangeNodeExists(tableName), false);
    // Sleep for some time so that our region is reassigned to some other RS
    // by master.
    Thread.sleep(10000);
    List<HRegion> onlineRegions
        = miniHBaseCluster.getRegions(Bytes.toBytes("testInstantSchemaChangeExclusions"));
    assertTrue(!onlineRegions.isEmpty());
    for (HRegion onlineRegion : onlineRegions) {
      HTableDescriptor htd = onlineRegion.getTableDesc();
      HColumnDescriptor tableHcd = htd.getFamily(HConstants.CATALOG_FAMILY);
      assertTrue(tableHcd.isBlockCacheEnabled() == false);
      assertEquals(tableHcd.getMaxVersions(), 99);
    }
    LOG.info("End testInstantSchemaChangeExclusions() ");

 }

  /**
   * This test validates that when a schema change request fails on the
   * RS side, we appropriately register the failure in the Master Schema change
   * tracker's node as well as capture the error cause.
   *
   * Currently an alter request fails if RS fails with an IO exception say due to
   * missing or incorrect codec. With instant schema change the same failure happens
   * and we register the failure with associated cause and also update the
   * monitor status appropriately.
   *
   * The region(s) will be orphaned in both the cases.
   *
   */
  @Test
  public void testInstantSchemaChangeWhileRSOpenRegionFailure() throws IOException,
      KeeperException, InterruptedException {
    MasterSchemaChangeTracker msct =
    TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();

    LOG.info("Start testInstantSchemaChangeWhileRSOpenRegionFailure() ");
    String tableName = "testInstantSchemaChangeWhileRSOpenRegionFailure";
    HTable ht = createTableAndValidate(tableName);

    // create now 100 regions
    TEST_UTIL.createMultiRegions(conf, ht,
      HConstants.CATALOG_FAMILY, 10);

    // wait for all the regions to be assigned
    Thread.sleep(10000);
    List<HRegion> onlineRegions
        = miniHBaseCluster.getRegions(
        Bytes.toBytes("testInstantSchemaChangeWhileRSOpenRegionFailure"));
    int size = onlineRegions.size();
    // we will not have any online regions
    LOG.info("Size of online regions = " + onlineRegions.size());

    HColumnDescriptor hcd = new HColumnDescriptor(HConstants.CATALOG_FAMILY);
    hcd.setMaxVersions(99);
    hcd.setBlockCacheEnabled(false);
    hcd.setCompressionType(Compression.Algorithm.SNAPPY);

    admin.modifyColumn(Bytes.toBytes(tableName), hcd);
    Thread.sleep(100);

    assertEquals(msct.doesSchemaChangeNodeExists(tableName), true);
    Thread.sleep(10000);
    // get the current alter status and validate that its failure with appropriate error msg.
    MasterSchemaChangeTracker.MasterAlterStatus mas = msct.getMasterAlterStatus(tableName);
    assertTrue(mas != null);
    assertEquals(mas.getCurrentAlterStatus(),
        MasterSchemaChangeTracker.MasterAlterStatus.AlterState.FAILURE);
    assertTrue(mas.getErrorCause() != null);
    LOG.info("End testInstantSchemaChangeWhileRSOpenRegionFailure() ");
 }

  @Test
  public void testConcurrentInstantSchemaChangeAndSplit() throws IOException,
  InterruptedException, KeeperException {
    final String tableName = "testConcurrentInstantSchemaChangeAndSplit";
    conf = TEST_UTIL.getConfiguration();
    LOG.info("Start testConcurrentInstantSchemaChangeAndSplit()");
    final String newFamily = "newFamily";
    HTable ht = createTableAndValidate(tableName);
    final MasterSchemaChangeTracker msct =
      TEST_UTIL.getHBaseCluster().getMaster().getSchemaChangeTracker();

    // create now 10 regions
    TEST_UTIL.createMultiRegions(conf, ht,
        HConstants.CATALOG_FAMILY, 4);
    int rowCount = TEST_UTIL.loadTable(ht, HConstants.CATALOG_FAMILY);
    //assertRowCount(t, rowCount);

    Runnable splitter = new Runnable() {
      public void run() {
        // run the splits now.
        try {
          LOG.info("Splitting table now ");
          admin.split(Bytes.toBytes(tableName));
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
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
    schemaChanger.run();
    Thread.sleep(50);
    splitter.run();
    waitForSchemaChangeProcess(tableName, 40000);

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
    LOG.info("End testConcurrentInstantSchemaChangeAndSplit() ");
  }
      

}


