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
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestNamespaceAuditor {
  private static final Log LOG = LogFactory.getLog(TestNamespaceAuditor.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin;
  private String prefix = "TestNamespaceAuditor";

  @BeforeClass
  public static void before() throws Exception {
    UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      CustomObserver.class.getName());
    UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    UTIL.startMiniCluster(1, 3);
    UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getHBaseCluster().getMaster().getMasterQuotaManager().isQuotaEnabled();
      }
    });
    admin = UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void cleanup() throws IOException, KeeperException {
    for (HTableDescriptor table : admin.listTables()) {
      admin.disableTable(table.getTableName());
      admin.deleteTable(table.getTableName());
    }
    for (NamespaceDescriptor ns : admin.listNamespaceDescriptors()) {
      if (ns.getName().startsWith(prefix)) {
        admin.deleteNamespace(ns.getName());
      }
    }
    assertTrue("Quota manager not enabled", UTIL.getHBaseCluster().getMaster()
      .getMasterQuotaManager().isQuotaEnabled());
  }

  @Test
  public void testTableOperations() throws Exception {
    String nsp = prefix + "_np2";
    NamespaceDescriptor nspDesc =
        NamespaceDescriptor.create(nsp).addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "5")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    assertNotNull("Namespace descriptor found null.", admin.getNamespaceDescriptor(nsp));
    assertEquals(admin.listNamespaceDescriptors().length, 3);
    HTableDescriptor tableDescOne =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1"));
    HTableDescriptor tableDescTwo =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table2"));
    HTableDescriptor tableDescThree =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table3"));
    admin.createTable(tableDescOne);
    boolean constraintViolated = false;
    try {
      admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 5);
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue("Constraint not violated for table " + tableDescTwo.getTableName(),
        constraintViolated);
    }
    admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 4);
    NamespaceTableAndRegionInfo nspState = getQuotaManager().getState(nsp);
    assertNotNull(nspState);
    assertTrue(nspState.getTables().size() == 2);
    assertTrue(nspState.getRegionCount() == 5);
    constraintViolated = false;
    try {
      admin.createTable(tableDescThree);
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
      admin.createNamespace(nspDesc);
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
      admin.createNamespace(nspDesc);
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
      admin.createNamespace(nspDesc);
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
      admin.createNamespace(nspDesc);
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
    admin.createNamespace(nspDesc);
    assertNotNull("Namespace descriptor found null.", admin.getNamespaceDescriptor(namespace));
    NamespaceTableAndRegionInfo stateInfo = getNamespaceState(nspDesc.getName());
    assertNotNull("Namespace state found null for " + namespace, stateInfo);
    HTableDescriptor tableDescOne =
        new HTableDescriptor(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + "table1"));
    HTableDescriptor tableDescTwo =
        new HTableDescriptor(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + "table2"));
    admin.createTable(tableDescOne);
    admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 5);
    stateInfo = getNamespaceState(nspDesc.getName());
    assertNotNull("Namespace state found to be null.", stateInfo);
    assertEquals(2, stateInfo.getTables().size());
    assertEquals(5, stateInfo.getRegionCountOfTable(tableDescTwo.getTableName()));
    assertEquals(6, stateInfo.getRegionCount());
    admin.disableTable(tableDescOne.getTableName());
    admin.deleteTable(tableDescOne.getTableName());
    stateInfo = getNamespaceState(nspDesc.getName());
    assertNotNull("Namespace state found to be null.", stateInfo);
    assertEquals(5, stateInfo.getRegionCount());
    assertEquals(1, stateInfo.getTables().size());
    admin.disableTable(tableDescTwo.getTableName());
    admin.deleteTable(tableDescTwo.getTableName());
    admin.deleteNamespace(namespace);
    stateInfo = getNamespaceState(namespace);
    assertNull("Namespace state not found to be null.", stateInfo);
  }

  @Test
  public void testRegionOperations() throws Exception {
    String nsp1 = prefix + "_regiontest";
    NamespaceDescriptor nspDesc = NamespaceDescriptor.create(nsp1)
        .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2")
        .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    boolean constraintViolated = false;
    final TableName tableOne = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table1");
    byte[] columnFamily = Bytes.toBytes("info");
    HTableDescriptor tableDescOne = new HTableDescriptor(tableOne);
    tableDescOne.addFamily(new HColumnDescriptor(columnFamily));
    NamespaceTableAndRegionInfo stateInfo;
    try {
      admin.createTable(tableDescOne, Bytes.toBytes("1"), Bytes.toBytes("1000"), 7);
    } catch (Exception exp) {
      assertTrue(exp instanceof DoNotRetryIOException);
      LOG.info(exp);
      constraintViolated = true;
    } finally {
      assertTrue(constraintViolated);
    }
    assertFalse(admin.tableExists(tableOne));
    // This call will pass.
    admin.createTable(tableDescOne);
    Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
    HTable htable = (HTable)connection.getTable(tableOne);
    UTIL.loadNumericRows(htable, Bytes.toBytes("info"), 1, 1000);
    admin.flush(tableOne);
    stateInfo = getNamespaceState(nsp1);
    assertEquals(1, stateInfo.getTables().size());
    assertEquals(1, stateInfo.getRegionCount());
    restartMaster();
    admin.split(tableOne, Bytes.toBytes("500"));
    HRegion actualRegion = UTIL.getHBaseCluster().getRegions(tableOne).get(0);
    CustomObserver observer = (CustomObserver) actualRegion.getCoprocessorHost().findCoprocessor(
      CustomObserver.class.getName());
    assertNotNull(observer);
    observer.postSplit.await();
    assertEquals(2, admin.getTableRegions(tableOne).size());
    actualRegion = UTIL.getHBaseCluster().getRegions(tableOne).get(0);
    observer = (CustomObserver) actualRegion.getCoprocessorHost().findCoprocessor(
      CustomObserver.class.getName());
    assertNotNull(observer);
    admin.split(tableOne, getSplitKey(actualRegion.getStartKey(), actualRegion.getEndKey()));
    observer.postSplit.await();
    // Make sure no regions have been added.
    assertEquals(2, admin.getTableRegions(tableOne).size());
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
    admin.createNamespace(nspDesc);
    TableName tableOne = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table1");
    TableName tableTwo = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table2");
    TableName tableThree = TableName.valueOf(nsp1 + TableName.NAMESPACE_DELIM + "table3");
    HTableDescriptor tableDescOne = new HTableDescriptor(tableOne);
    HTableDescriptor tableDescTwo = new HTableDescriptor(tableTwo);
    HTableDescriptor tableDescThree = new HTableDescriptor(tableThree);
    admin.createTable(tableDescOne, Bytes.toBytes("1"), Bytes.toBytes("1000"), 3);
    admin.createTable(tableDescTwo, Bytes.toBytes("1"), Bytes.toBytes("1000"), 3);
    admin.createTable(tableDescThree, Bytes.toBytes("1"), Bytes.toBytes("1000"), 4);
    admin.disableTable(tableThree);
    admin.deleteTable(tableThree);
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
    UTIL.getHBaseCluster().getMaster().stop("Stopping to start again");
    UTIL.getHBaseCluster().startMaster();
    Thread.sleep(60000);
    UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getHBaseCluster().getMaster().getMasterQuotaManager().isQuotaEnabled();
      }
    });
  }

  private NamespaceAuditor getQuotaManager() {
    return UTIL.getHBaseCluster().getMaster()
        .getMasterQuotaManager().getNamespaceQuotaManager();
  }

}
