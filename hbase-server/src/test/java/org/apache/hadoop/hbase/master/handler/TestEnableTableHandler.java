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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestEnableTableHandler {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestEnableTableHandler.class);
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:meta");
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterSyncObserver.class.getName());
    TEST_UTIL.startMiniCluster(1, 2);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 300000)
  public void testDisableTableAndRestart() throws Exception {
    final TableName tableName = TableName.valueOf("testDisableTableAndRestart");
    final HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    admin.createTable(desc);
    admin.disableTable(tableName);
    TEST_UTIL.waitTableDisabled(tableName.getName());

    TEST_UTIL.getHBaseCluster().shutdown();
    TEST_UTIL.getHBaseCluster().waitUntilShutDown();

    TEST_UTIL.restartHBaseCluster(2);

    final HBaseAdmin newAdmin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    newAdmin.enableTable(tableName);
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return newAdmin.isTableAvailable(tableName);
      }
    });
  }

  public static class MasterSyncObserver extends BaseMasterObserver {
    volatile CountDownLatch tableCreationLatch = null;
    volatile CountDownLatch tableDeletionLatch = null;

    @Override
    public void postCreateTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
      // the AccessController test, some times calls only and directly the postCreateTableHandler()
      if (tableCreationLatch != null) {
        tableCreationLatch.countDown();
      }
    }

    @Override
    public void postDeleteTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName)
        throws IOException {
      // the AccessController test, some times calls only and directly the postDeleteTableHandler()
      if (tableDeletionLatch != null) {
        tableDeletionLatch.countDown();
      }
    }
  }

  public static void createTable(HBaseTestingUtility testUtil, HTableDescriptor htd,
      byte[][] splitKeys)
      throws Exception {
    createTable(testUtil, testUtil.getHBaseAdmin(), htd, splitKeys);
  }

  public static void createTable(HBaseTestingUtility testUtil, HBaseAdmin admin,
      HTableDescriptor htd, byte[][] splitKeys)
      throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver) testUtil.getHBaseCluster().getMaster()
        .getCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    observer.tableCreationLatch = new CountDownLatch(1);
    if (splitKeys != null) {
      admin.createTable(htd, splitKeys);
    } else {
      admin.createTable(htd);
    }
    observer.tableCreationLatch.await();
    observer.tableCreationLatch = null;
    testUtil.waitUntilAllRegionsAssigned(htd.getTableName());
  }

  public static void deleteTable(HBaseTestingUtility testUtil, TableName tableName)
      throws Exception {
    deleteTable(testUtil, testUtil.getHBaseAdmin(), tableName);
  }

  public static void deleteTable(HBaseTestingUtility testUtil, HBaseAdmin admin,
      TableName tableName)
      throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver) testUtil.getHBaseCluster().getMaster()
        .getCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    observer.tableDeletionLatch = new CountDownLatch(1);
    try {
      admin.disableTable(tableName);
    } catch (Exception e) {
      LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
    }
    admin.deleteTable(tableName);
    observer.tableDeletionLatch.await();
    observer.tableDeletionLatch = null;
  }
}
