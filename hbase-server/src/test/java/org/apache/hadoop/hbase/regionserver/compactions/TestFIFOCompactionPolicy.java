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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.TimeOffsetEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestFIFOCompactionPolicy {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();


  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  private final byte[] family = Bytes.toBytes("f");

  private final byte[] qualifier = Bytes.toBytes("q");

  private Store getStoreWithName(TableName tableName) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getOnlineRegions(tableName)) {
        return region.getStores().iterator().next();
      }
    }
    return null;
  }

  private Store prepareData() throws IOException {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setConfiguration(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, 
      FIFOCompactionPolicy.class.getName());
    desc.setConfiguration(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, 
      DisabledRegionSplitPolicy.class.getName());
    HColumnDescriptor colDesc = new HColumnDescriptor(family);
    colDesc.setTimeToLive(1); // 1 sec
    desc.addFamily(colDesc);

    admin.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    Random rand = new Random();
    TimeOffsetEnvironmentEdge edge =
        (TimeOffsetEnvironmentEdge) EnvironmentEdgeManager.getDelegate();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[128 * 1024];
        rand.nextBytes(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
      admin.flush(tableName);
      edge.increment(1001);
    }
    return getStoreWithName(tableName);
  }

  @BeforeClass   
  public static void setEnvironmentEdge()
  {
    EnvironmentEdge ee = new TimeOffsetEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(ee);
  }
  
  @AfterClass
  public static void resetEnvironmentEdge()
  {
    EnvironmentEdgeManager.reset();
  }
  
  @Test
  public void testPurgeExpiredFiles() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);

    TEST_UTIL.startMiniCluster(1);
    try {
      Store store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      TEST_UTIL.getHBaseAdmin().majorCompact(tableName);
      while (store.getStorefilesCount() > 1) {
        Thread.sleep(100);
      }
      assertTrue(store.getStorefilesCount() == 1);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }
  
  @Test  
  public void testSanityCheckTTL() throws Exception
  {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    TEST_UTIL.startMiniCluster(1);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String tableName = this.tableName.getNameAsString()+"-TTL";
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setConfiguration(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, 
      FIFOCompactionPolicy.class.getName());
    desc.setConfiguration(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, 
      DisabledRegionSplitPolicy.class.getName());
    HColumnDescriptor colDesc = new HColumnDescriptor(family);
    desc.addFamily(colDesc);
    try{
      admin.createTable(desc);
      Assert.fail();
    }catch(Exception e){      
    }finally{
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test  
  public void testSanityCheckMinVersion() throws Exception
  {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    TEST_UTIL.startMiniCluster(1);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String tableName = this.tableName.getNameAsString()+"-MinVersion";
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setConfiguration(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, 
      FIFOCompactionPolicy.class.getName());
    desc.setConfiguration(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, 
      DisabledRegionSplitPolicy.class.getName());
    HColumnDescriptor colDesc = new HColumnDescriptor(family);
    colDesc.setTimeToLive(1); // 1 sec
    colDesc.setMinVersions(1);
    desc.addFamily(colDesc);
    try{
      admin.createTable(desc);
      Assert.fail();
    }catch(Exception e){      
    }finally{
      TEST_UTIL.shutdownMiniCluster();
    }
  }
  
  @Test  
  public void testSanityCheckBlockingStoreFiles() throws Exception
  {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10);
    TEST_UTIL.startMiniCluster(1);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String tableName = this.tableName.getNameAsString()+"-MinVersion";
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setConfiguration(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, 
      FIFOCompactionPolicy.class.getName());
    desc.setConfiguration(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, 
      DisabledRegionSplitPolicy.class.getName());
    HColumnDescriptor colDesc = new HColumnDescriptor(family);
    colDesc.setTimeToLive(1); // 1 sec
    desc.addFamily(colDesc);
    try{
      admin.createTable(desc);
      Assert.fail();
    }catch(Exception e){      
    }finally{
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}
