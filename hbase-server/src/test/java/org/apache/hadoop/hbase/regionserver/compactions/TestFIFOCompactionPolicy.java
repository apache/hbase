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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.TimeOffsetEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestFIFOCompactionPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFIFOCompactionPolicy.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  private final byte[] family = Bytes.toBytes("f");

  private final byte[] qualifier = Bytes.toBytes("q");

  @Rule
  public ExpectedException error = ExpectedException.none();

  private HStore getStoreWithName(TableName tableName) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (HRegion region : hrs.getRegions(tableName)) {
        return region.getStores().iterator().next();
      }
    }
    return null;
  }

  private HStore prepareData() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          FIFOCompactionPolicy.class.getName())
        .setValue(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DisabledRegionSplitPolicy.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family).setTimeToLive(1).build())
        .build();
    admin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    TimeOffsetEnvironmentEdge edge =
        (TimeOffsetEnvironmentEdge) EnvironmentEdgeManager.getDelegate();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[128 * 1024];
        Bytes.random(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
      admin.flush(tableName);
      edge.increment(1001);
    }
    return getStoreWithName(tableName);
  }

  @BeforeClass
  public static void setEnvironmentEdge() throws Exception {
    EnvironmentEdge ee = new TimeOffsetEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(ee);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    // Expired store file deletion during compaction optimization interferes with the FIFO
    // compaction policy. The race causes changes to in-flight-compaction files resulting in a
    // non-deterministic number of files selected by compaction policy. Disables that optimization
    // for this test run.
    conf.setBoolean("hbase.store.delete.expired.storefile", false);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void resetEnvironmentEdge() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testPurgeExpiredFiles() throws Exception {
    HStore store = prepareData();
    assertEquals(10, store.getStorefilesCount());
    TEST_UTIL.getAdmin().majorCompact(tableName);
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return store.getStorefilesCount() == 1;
      }

      @Override
      public String explainFailure() throws Exception {
        return "The store file count " + store.getStorefilesCount() + " is still greater than 1";
      }
    });
  }

  @Test
  public void testSanityCheckTTL() throws IOException {
    error.expect(DoNotRetryIOException.class);
    error.expectMessage("Default TTL is not supported");
    TableName tableName = TableName.valueOf(getClass().getSimpleName() + "-TTL");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          FIFOCompactionPolicy.class.getName())
        .setValue(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DisabledRegionSplitPolicy.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    TEST_UTIL.getAdmin().createTable(desc);
  }

  @Test
  public void testSanityCheckMinVersion() throws IOException {
    error.expect(DoNotRetryIOException.class);
    error.expectMessage("MIN_VERSION > 0 is not supported for FIFO compaction");
    TableName tableName = TableName.valueOf(getClass().getSimpleName() + "-MinVersion");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          FIFOCompactionPolicy.class.getName())
        .setValue(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DisabledRegionSplitPolicy.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family).setTimeToLive(1)
            .setMinVersions(1).build())
        .build();
    TEST_UTIL.getAdmin().createTable(desc);
  }

  @Test
  public void testSanityCheckBlockingStoreFiles() throws IOException {
    error.expect(DoNotRetryIOException.class);
    error.expectMessage("Blocking file count 'hbase.hstore.blockingStoreFiles'");
    error.expectMessage("is below recommended minimum of 1000 for column family");
    TableName tableName = TableName.valueOf(getClass().getSimpleName() + "-BlockingStoreFiles");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          FIFOCompactionPolicy.class.getName())
        .setValue(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DisabledRegionSplitPolicy.class.getName())
        .setValue(HStore.BLOCKING_STOREFILES_KEY, "10")
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family).setTimeToLive(1).build())
        .build();
    TEST_UTIL.getAdmin().createTable(desc);
  }

  /**
   * Unit test for HBASE-21504
   */
  @Test
  public void testFIFOCompactionPolicyExpiredEmptyHFiles() throws Exception {
    TableName tableName = TableName.valueOf("testFIFOCompactionPolicyExpiredEmptyHFiles");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          FIFOCompactionPolicy.class.getName())
        .setValue(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
          DisabledRegionSplitPolicy.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family).setTimeToLive(1).build())
        .build();
    Table table = TEST_UTIL.createTable(desc, null);
    long ts = System.currentTimeMillis() - 10 * 1000;
    Put put =
        new Put(Bytes.toBytes("row1")).addColumn(family, qualifier, ts, Bytes.toBytes("value0"));
    table.put(put);
    TEST_UTIL.getAdmin().flush(tableName); // HFile-0
    put = new Put(Bytes.toBytes("row2")).addColumn(family, qualifier, ts, Bytes.toBytes("value1"));
    table.put(put);
    final int testWaitTimeoutMs = 20000;
    TEST_UTIL.getAdmin().flush(tableName); // HFile-1

    HStore store = Preconditions.checkNotNull(getStoreWithName(tableName));
    Assert.assertEquals(2, store.getStorefilesCount());

    TEST_UTIL.getAdmin().majorCompact(tableName);
    TEST_UTIL.waitFor(testWaitTimeoutMs,
        (Waiter.Predicate<Exception>) () -> store.getStorefilesCount() == 1);

    Assert.assertEquals(1, store.getStorefilesCount());
    HStoreFile sf = Preconditions.checkNotNull(store.getStorefiles().iterator().next());
    Assert.assertEquals(0, sf.getReader().getEntries());

    put = new Put(Bytes.toBytes("row3")).addColumn(family, qualifier, ts, Bytes.toBytes("value1"));
    table.put(put);
    TEST_UTIL.getAdmin().flush(tableName); // HFile-2
    Assert.assertEquals(2, store.getStorefilesCount());

    TEST_UTIL.getAdmin().majorCompact(tableName);
    TEST_UTIL.waitFor(testWaitTimeoutMs,
        (Waiter.Predicate<Exception>) () -> store.getStorefilesCount() == 1);

    Assert.assertEquals(1, store.getStorefilesCount());
    sf = Preconditions.checkNotNull(store.getStorefiles().iterator().next());
    Assert.assertEquals(0, sf.getReader().getEntries());
  }
}
