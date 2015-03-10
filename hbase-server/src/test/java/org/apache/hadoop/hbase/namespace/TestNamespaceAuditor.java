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
package org.apache.hadoop.hbase.namespace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(MediumTests.class)
public class TestNamespaceAuditor {
  private static final Log LOG = LogFactory.getLog(TestNamespaceAuditor.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin ADMIN;
  private String prefix = "TestNamespaceAuditor";

  @BeforeClass
  public static void before() throws Exception {
    UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      CustomObserver.class.getName());
    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      MasterSyncObserver.class.getName());
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    conf.setClass("hbase.coprocessor.regionserver.classes", CPRegionServerObserver.class,
      RegionServerObserver.class);
    UTIL.startMiniCluster(1, 1);
    UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getHBaseCluster().getMaster().getMasterQuotaManager().isQuotaEnabled();
      }
    });
    ADMIN = UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void cleanup() throws Exception, KeeperException {
    for (HTableDescriptor table : ADMIN.listTables()) {
      ADMIN.disableTable(table.getTableName());
      deleteTable(table.getTableName());
    }
    for (NamespaceDescriptor ns : ADMIN.listNamespaceDescriptors()) {
      if (ns.getName().startsWith(prefix)) {
        ADMIN.deleteNamespace(ns.getName());
      }
    }
    assertTrue("Quota manager not enabled", UTIL.getHBaseCluster().getMaster()
      .getMasterQuotaManager().isQuotaEnabled());
  }

  @Test(timeout = 60000)
  public void testTableOperations() throws Exception {
    String nsp = prefix + "_np2";
    NamespaceDescriptor nspDesc =
        NamespaceDescriptor.create(nsp).addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "5")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    ADMIN.createNamespace(nspDesc);
    assertNotNull("Namespace descriptor found null.", ADMIN.getNamespaceDescriptor(nsp));
    assertEquals(ADMIN.listNamespaceDescriptors().length, 3);
    HTableDescriptor tableDescOne =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1"));
    HTableDescriptor tableDescTwo =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table2"));
    HTableDescriptor tableDescThree =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table3"));
    ADMIN.createTable(tableDescOne);
    boolean constraintViolated = false;
    try {
      ADMIN.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 5);
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue("Constraint not violated for table " + tableDescTwo.getTableName(),
        constraintViolated);
    }
    ADMIN.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 4);
    NamespaceTableAndRegionInfo nspState = getQuotaManager().getState(nsp);
    assertNotNull(nspState);
    assertTrue(nspState.getTables().size() == 2);
    assertTrue(nspState.getRegionCount() == 5);
    constraintViolated = false;
    try {
      ADMIN.createTable(tableDescThree);
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue("Constraint not violated for table " + tableDescThree.getTableName(),
        constraintViolated);
    }
  }

  @Test
  public void testValidQuotas() throws Exception {
    boolean exceptionCaught = false;
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    NamespaceDescriptor nspDesc =
        NamespaceDescriptor.create(prefix + "vq1")
            .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "hihdufh")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    try {
      ADMIN.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    nspDesc =
        NamespaceDescriptor.create(prefix + "vq2")
            .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "-456")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    try {
      ADMIN.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    nspDesc =
        NamespaceDescriptor.create(prefix + "vq3")
            .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "10")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "sciigd").build();
    try {
      ADMIN.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    nspDesc =
        NamespaceDescriptor.create(prefix + "vq4")
            .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "10")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "-1500").build();
    try {
      ADMIN.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    String namespace = prefix + "_dummy";
    NamespaceDescriptor nspDesc =
        NamespaceDescriptor.create(namespace)
            .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "100")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "3").build();
    ADMIN.createNamespace(nspDesc);
    assertNotNull("Namespace descriptor found null.", ADMIN.getNamespaceDescriptor(namespace));
    NamespaceTableAndRegionInfo stateInfo = getNamespaceState(nspDesc.getName());
    assertNotNull("Namespace state found null for " + namespace, stateInfo);
    HTableDescriptor tableDescOne =
        new HTableDescriptor(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + "table1"));
    HTableDescriptor tableDescTwo =
        new HTableDescriptor(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + "table2"));
    ADMIN.createTable(tableDescOne);
    ADMIN.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 5);
    stateInfo = getNamespaceState(nspDesc.getName());
    assertNotNull("Namespace state found to be null.", stateInfo);
    assertEquals(2, stateInfo.getTables().size());
    assertEquals(5, stateInfo.getRegionCountOfTable(tableDescTwo.getTableName()));
    assertEquals(6, stateInfo.getRegionCount());
    ADMIN.disableTable(tableDescOne.getTableName());
    deleteTable(tableDescOne.getTableName());
    stateInfo = getNamespaceState(nspDesc.getName());
    assertNotNull("Namespace state found to be null.", stateInfo);
    assertEquals(5, stateInfo.getRegionCount());
    assertEquals(1, stateInfo.getTables().size());
    ADMIN.disableTable(tableDescTwo.getTableName());
    deleteTable(tableDescTwo.getTableName());
    ADMIN.deleteNamespace(namespace);
    stateInfo = getNamespaceState(namespace);
    assertNull("Namespace state not found to be null.", stateInfo);
  }

  public static class CPRegionServerObserver extends BaseRegionServerObserver {
    private volatile boolean shouldFailMerge = false;

    public void failMerge(boolean fail) {
      shouldFailMerge = fail;
    }

    private boolean triggered = false;

    public synchronized void waitUtilTriggered() throws InterruptedException {
      while (!triggered) {
        wait();
      }
    }

    @Override
    public synchronized void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
        HRegion regionA, HRegion regionB) throws IOException {
      triggered = true;
      notifyAll();
      if (shouldFailMerge) {
        throw new IOException("fail merge");
      }
    }
  }

  @Test
  public void testRegionMerge() throws Exception {
    String nsp1 = prefix + "_regiontest";
    NamespaceDescriptor nspDesc =
        NamespaceDescriptor.create(nsp1)
            .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "3")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    ADMIN.createNamespace(nspDesc);
    final TableName tableTwo = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table2");
    byte[] columnFamily = Bytes.toBytes("info");
    HTableDescriptor tableDescOne = new HTableDescriptor(tableTwo);
    tableDescOne.addFamily(new HColumnDescriptor(columnFamily));
    final int initialRegions = 3;
    ADMIN.createTable(tableDescOne, Bytes.toBytes("1"), Bytes.toBytes("2000"), initialRegions);
    Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
    try (Table table = connection.getTable(tableTwo)) {
      UTIL.loadNumericRows(table, Bytes.toBytes("info"), 1000, 1999);
    }
    ADMIN.flush(tableTwo);
    List<HRegionInfo> hris = ADMIN.getTableRegions(tableTwo);
    Collections.sort(hris);
    // merge the two regions
    final Set<String> encodedRegionNamesToMerge =
        Sets.newHashSet(hris.get(0).getEncodedName(), hris.get(1).getEncodedName());
    ADMIN.mergeRegions(hris.get(0).getEncodedNameAsBytes(), hris.get(1).getEncodedNameAsBytes(),
      false);
    UTIL.waitFor(10000, 100, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        RegionStates regionStates =
            UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
        for (HRegionInfo hri : ADMIN.getTableRegions(tableTwo)) {
          if (encodedRegionNamesToMerge.contains(hri.getEncodedName())) {
            return false;
          }
          if (!regionStates.isRegionInState(hri, RegionState.State.OPEN)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public String explainFailure() throws Exception {
        RegionStates regionStates =
            UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
        for (HRegionInfo hri : ADMIN.getTableRegions(tableTwo)) {
          if (encodedRegionNamesToMerge.contains(hri.getEncodedName())) {
            return hri + " which is expected to be merged is still online";
          }
          if (!regionStates.isRegionInState(hri, RegionState.State.OPEN)) {
            return hri + " is still in not opened";
          }
        }
        return "Unknown";
      }
    });
    hris = ADMIN.getTableRegions(tableTwo);
    assertEquals(initialRegions - 1, hris.size());
    Collections.sort(hris);

    final HRegionInfo hriToSplit = hris.get(1);
    ADMIN.split(tableTwo, Bytes.toBytes("500"));

    UTIL.waitFor(10000, 100, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        RegionStates regionStates =
            UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
        for (HRegionInfo hri : ADMIN.getTableRegions(tableTwo)) {
          if (hri.getEncodedName().equals(hriToSplit.getEncodedName())) {
            return false;
          }
          if (!regionStates.isRegionInState(hri, RegionState.State.OPEN)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public String explainFailure() throws Exception {
        RegionStates regionStates =
            UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
        for (HRegionInfo hri : ADMIN.getTableRegions(tableTwo)) {
          if (hri.getEncodedName().equals(hriToSplit.getEncodedName())) {
            return hriToSplit + " which is expected to be split is still online";
          }
          if (!regionStates.isRegionInState(hri, RegionState.State.OPEN)) {
            return hri + " is still in not opened";
          }
        }
        return "Unknown";
      }
    });
    hris = ADMIN.getTableRegions(tableTwo);
    assertEquals(initialRegions, hris.size());
    Collections.sort(hris);

    // fail region merge through Coprocessor hook
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    RegionServerCoprocessorHost cpHost = regionServer.getRegionServerCoprocessorHost();
    Coprocessor coprocessor = cpHost.findCoprocessor(CPRegionServerObserver.class.getName());
    CPRegionServerObserver regionServerObserver = (CPRegionServerObserver) coprocessor;
    regionServerObserver.failMerge(true);
    regionServerObserver.triggered = false;

    ADMIN.mergeRegions(hris.get(1).getEncodedNameAsBytes(), hris.get(2).getEncodedNameAsBytes(),
      false);
    regionServerObserver.waitUtilTriggered();
    hris = ADMIN.getTableRegions(tableTwo);
    assertEquals(initialRegions, hris.size());
    Collections.sort(hris);
    // verify that we cannot split
    HRegionInfo hriToSplit2 = hris.get(1);
    ADMIN.split(tableTwo,
      TableInputFormatBase.getSplitKey(hriToSplit2.getStartKey(), hriToSplit2.getEndKey(), true));
    Thread.sleep(2000);
    assertEquals(initialRegions, ADMIN.getTableRegions(tableTwo).size());
  }

  @Test
  public void testRegionOperations() throws Exception {
    String nsp1 = prefix + "_regiontest";
    NamespaceDescriptor nspDesc = NamespaceDescriptor.create(nsp1)
        .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2")
        .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    ADMIN.createNamespace(nspDesc);
    boolean constraintViolated = false;
    final TableName tableOne = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table1");
    byte[] columnFamily = Bytes.toBytes("info");
    HTableDescriptor tableDescOne = new HTableDescriptor(tableOne);
    tableDescOne.addFamily(new HColumnDescriptor(columnFamily));
    NamespaceTableAndRegionInfo stateInfo;
    try {
      ADMIN.createTable(tableDescOne, Bytes.toBytes("1"), Bytes.toBytes("1000"), 7);
    } catch (Exception exp) {
      assertTrue(exp instanceof DoNotRetryIOException);
      LOG.info(exp);
      constraintViolated = true;
    } finally {
      assertTrue(constraintViolated);
    }
    assertFalse(ADMIN.tableExists(tableOne));
    // This call will pass.
    ADMIN.createTable(tableDescOne);
    Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
    HTable htable = (HTable)connection.getTable(tableOne);
    UTIL.loadNumericRows(htable, Bytes.toBytes("info"), 1, 1000);
    ADMIN.flush(tableOne);
    stateInfo = getNamespaceState(nsp1);
    assertEquals(1, stateInfo.getTables().size());
    assertEquals(1, stateInfo.getRegionCount());
    restartMaster();
    ADMIN.split(tableOne, Bytes.toBytes("500"));
    HRegion actualRegion = UTIL.getHBaseCluster().getRegions(tableOne).get(0);
    CustomObserver observer = (CustomObserver) actualRegion.getCoprocessorHost().findCoprocessor(
      CustomObserver.class.getName());
    assertNotNull(observer);
    observer.postSplit.await();
    assertEquals(2, ADMIN.getTableRegions(tableOne).size());
    actualRegion = UTIL.getHBaseCluster().getRegions(tableOne).get(0);
    observer = (CustomObserver) actualRegion.getCoprocessorHost().findCoprocessor(
      CustomObserver.class.getName());
    assertNotNull(observer);
    ADMIN.split(tableOne, getSplitKey(actualRegion.getStartKey(), actualRegion.getEndKey()));
    observer.postSplit.await();
    // Make sure no regions have been added.
    List<HRegionInfo> hris = ADMIN.getTableRegions(tableOne);
    assertEquals(2, hris.size());
    assertTrue("split completed", observer.preSplitBeforePONR.getCount() == 1);

    htable.close();
  }

  private NamespaceTableAndRegionInfo getNamespaceState(String namespace) throws KeeperException,
      IOException {
    return getQuotaManager().getState(namespace);
  }

  byte[] getSplitKey(byte[] startKey, byte[] endKey) {
    String skey = Bytes.toString(startKey);
    int key;
    if (StringUtils.isBlank(skey)) {
      key = Integer.parseInt(Bytes.toString(endKey))/2 ;
    } else {
      key = (int) (Integer.parseInt(skey) * 1.5);
    }
    return Bytes.toBytes("" + key);
  }

  public static class CustomObserver extends BaseRegionObserver{
    volatile CountDownLatch postSplit;
    volatile CountDownLatch preSplitBeforePONR;

    @Override
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
      postSplit.countDown();
    }

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx,
        byte[] splitKey, List<Mutation> metaEntries) throws IOException {
      preSplitBeforePONR.countDown();
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      postSplit = new CountDownLatch(1);
      preSplitBeforePONR = new CountDownLatch(1);
    }
  }

  @Test
  public void testStatePreserve() throws Exception {
    final String nsp1 = prefix + "_testStatePreserve";
    NamespaceDescriptor nspDesc = NamespaceDescriptor.create(nsp1)
        .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "20")
        .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "10").build();
    ADMIN.createNamespace(nspDesc);
    TableName tableOne = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table1");
    TableName tableTwo = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table2");
    TableName tableThree = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table3");
    HTableDescriptor tableDescOne = new HTableDescriptor(tableOne);
    HTableDescriptor tableDescTwo = new HTableDescriptor(tableTwo);
    HTableDescriptor tableDescThree = new HTableDescriptor(tableThree);
    ADMIN.createTable(tableDescOne, Bytes.toBytes("1"), Bytes.toBytes("1000"), 3);
    ADMIN.createTable(tableDescTwo, Bytes.toBytes("1"), Bytes.toBytes("1000"), 3);
    ADMIN.createTable(tableDescThree, Bytes.toBytes("1"), Bytes.toBytes("1000"), 4);
    ADMIN.disableTable(tableThree);
    deleteTable(tableThree);
    // wait for chore to complete
    UTIL.waitFor(1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
       return (getNamespaceState(nsp1).getTables().size() == 2);
      }
    });
    NamespaceTableAndRegionInfo before = getNamespaceState(nsp1);
    restartMaster();
    NamespaceTableAndRegionInfo after = getNamespaceState(nsp1);
    assertEquals("Expected: " + before.getTables() + " Found: " + after.getTables(), before
        .getTables().size(), after.getTables().size());
  }

  private void restartMaster() throws Exception {
    UTIL.getHBaseCluster().getMaster(0).stop("Stopping to start again");
    UTIL.getHBaseCluster().waitOnMaster(0);
    UTIL.getHBaseCluster().startMaster();
    UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        HMaster master = UTIL.getHBaseCluster().getMaster();
        if (master == null) {
          return false;
        }
        MasterQuotaManager quotaManager = master.getMasterQuotaManager();
        return quotaManager != null && quotaManager.isQuotaEnabled();
      }
    });
  }

  private NamespaceAuditor getQuotaManager() {
    return UTIL.getHBaseCluster().getMaster()
        .getMasterQuotaManager().getNamespaceQuotaManager();
  }

  public static class MasterSyncObserver extends BaseMasterObserver {
    volatile CountDownLatch tableDeletionLatch;

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName) throws IOException {
      tableDeletionLatch = new CountDownLatch(1);
    }

    @Override
    public void postDeleteTableHandler(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
        throws IOException {
      tableDeletionLatch.countDown();
    }
  }

  private void deleteTable(final TableName tableName) throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver)UTIL.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    ADMIN.deleteTable(tableName);
    observer.tableDeletionLatch.await();
  }
}
