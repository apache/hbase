/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPerTableCFReplication {

  private static final Log LOG = LogFactory.getLog(TestPerTableCFReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration conf3;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static HBaseTestingUtility utility3;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 100;

  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] tabAName = Bytes.toBytes("TA");
  private static final byte[] tabBName = Bytes.toBytes("TB");
  private static final byte[] tabCName = Bytes.toBytes("TC");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] f1Name = Bytes.toBytes("f1");
  private static final byte[] f2Name = Bytes.toBytes("f2");
  private static final byte[] f3Name = Bytes.toBytes("f3");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");
  private static final byte[] val = Bytes.toBytes("myval");

  private static HTableDescriptor table;
  private static HTableDescriptor tabA;
  private static HTableDescriptor tabB;
  private static HTableDescriptor tabC;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024*20);
    conf1.setInt("replication.source.size.capacity", 1024);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    conf3 = new Configuration(conf1);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster3", null, true);

    utility3 = new HBaseTestingUtility(conf3);
    utility3.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf3, "cluster3", null, true);

    table = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    fam = new HColumnDescriptor(noRepfamName);
    table.addFamily(fam);

    tabA = new HTableDescriptor(tabAName);
    fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabA.addFamily(fam);
    fam = new HColumnDescriptor(f2Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabA.addFamily(fam);
    fam = new HColumnDescriptor(f3Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabA.addFamily(fam);

    tabB = new HTableDescriptor(tabBName);
    fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabB.addFamily(fam);
    fam = new HColumnDescriptor(f2Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabB.addFamily(fam);
    fam = new HColumnDescriptor(f3Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabB.addFamily(fam);

    tabC = new HTableDescriptor(tabCName);
    fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabC.addFamily(fam);
    fam = new HColumnDescriptor(f2Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabC.addFamily(fam);
    fam = new HColumnDescriptor(f3Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabC.addFamily(fam);

    utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Test
  public void testParseTableCFsFromConfig() {
    Map<String, List<String>> tabCFsMap = null;

    // 1. null or empty string, result should be null
    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig(null);
    assertEquals(null, tabCFsMap);

    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("");
    assertEquals(null, tabCFsMap);

    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("   ");
    assertEquals(null, tabCFsMap);

    // 2. single table: "tab1" / "tab2:cf1" / "tab3:cf1,cf3"
    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("tab1");
    assertEquals(1, tabCFsMap.size()); // only one table
    assertTrue(tabCFsMap.containsKey("tab1"));   // its table name is "tab1"
    assertFalse(tabCFsMap.containsKey("tab2"));  // not other table
    assertEquals(null, tabCFsMap.get("tab1"));   // null cf-list,

    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("tab2:cf1");
    assertEquals(1, tabCFsMap.size()); // only one table
    assertTrue(tabCFsMap.containsKey("tab2"));   // its table name is "tab2"
    assertFalse(tabCFsMap.containsKey("tab1"));  // not other table
    assertEquals(1, tabCFsMap.get("tab2").size());   // cf-list contains only 1 cf
    assertEquals("cf1", tabCFsMap.get("tab2").get(0));// the only cf is "cf1"

    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("tab3 : cf1 , cf3");
    assertEquals(1, tabCFsMap.size()); // only one table
    assertTrue(tabCFsMap.containsKey("tab3"));   // its table name is "tab2"
    assertFalse(tabCFsMap.containsKey("tab1"));  // not other table
    assertEquals(2, tabCFsMap.get("tab3").size());   // cf-list contains 2 cf
    assertTrue(tabCFsMap.get("tab3").contains("cf1"));// contains "cf1"
    assertTrue(tabCFsMap.get("tab3").contains("cf3"));// contains "cf3"

    // 3. multiple tables: "tab1 ; tab2:cf1 ; tab3:cf1,cf3"
    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("tab1 ; tab2:cf1 ; tab3:cf1,cf3");
    // 3.1 contains 3 tables : "tab1", "tab2" and "tab3"
    assertEquals(3, tabCFsMap.size());
    assertTrue(tabCFsMap.containsKey("tab1"));
    assertTrue(tabCFsMap.containsKey("tab2"));
    assertTrue(tabCFsMap.containsKey("tab3"));
    // 3.2 table "tab1" : null cf-list
    assertEquals(null, tabCFsMap.get("tab1"));
    // 3.3 table "tab2" : cf-list contains a single cf "cf1"
    assertEquals(1, tabCFsMap.get("tab2").size());
    assertEquals("cf1", tabCFsMap.get("tab2").get(0));
    // 3.4 table "tab3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get("tab3").size());
    assertTrue(tabCFsMap.get("tab3").contains("cf1"));
    assertTrue(tabCFsMap.get("tab3").contains("cf3"));

    // 4. contiguous or additional ";"(table delimiter) or ","(cf delimiter) can be tolerated
    // still use the example of multiple tables: "tab1 ; tab2:cf1 ; tab3:cf1,cf3"
    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("tab1 ; ; tab2:cf1 ; tab3:cf1,,cf3 ;");
    // 4.1 contains 3 tables : "tab1", "tab2" and "tab3"
    assertEquals(3, tabCFsMap.size());
    assertTrue(tabCFsMap.containsKey("tab1"));
    assertTrue(tabCFsMap.containsKey("tab2"));
    assertTrue(tabCFsMap.containsKey("tab3"));
    // 4.2 table "tab1" : null cf-list
    assertEquals(null, tabCFsMap.get("tab1"));
    // 4.3 table "tab2" : cf-list contains a single cf "cf1"
    assertEquals(1, tabCFsMap.get("tab2").size());
    assertEquals("cf1", tabCFsMap.get("tab2").get(0));
    // 4.4 table "tab3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get("tab3").size());
    assertTrue(tabCFsMap.get("tab3").contains("cf1"));
    assertTrue(tabCFsMap.get("tab3").contains("cf3"));

    // 5. invalid format "tab1:tt:cf1 ; tab2::cf1 ; tab3:cf1,cf3"
    //    "tab1:tt:cf1" and "tab2::cf1" are invalid and will be ignored totally
    tabCFsMap = ReplicationPeer.parseTableCFsFromConfig("tab1:tt:cf1 ; tab2::cf1 ; tab3:cf1,cf3");
    // 5.1 no "tab1" and "tab2", only "tab3"
    assertEquals(1, tabCFsMap.size()); // only one table
    assertFalse(tabCFsMap.containsKey("tab1"));
    assertFalse(tabCFsMap.containsKey("tab2"));
    assertTrue(tabCFsMap.containsKey("tab3"));
   // 5.2 table "tab3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get("tab3").size());
    assertTrue(tabCFsMap.get("tab3").contains("cf1"));
    assertTrue(tabCFsMap.get("tab3").contains("cf3"));
 }

  @Test(timeout=300000)
  public void testPerTableCFReplication() throws Exception {
    LOG.info("testPerTableCFReplication");
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);

    new HBaseAdmin(conf1).createTable(tabA);
    new HBaseAdmin(conf1).createTable(tabB);
    new HBaseAdmin(conf1).createTable(tabC);
    new HBaseAdmin(conf2).createTable(tabA);
    new HBaseAdmin(conf2).createTable(tabB);
    new HBaseAdmin(conf2).createTable(tabC);
    new HBaseAdmin(conf3).createTable(tabA);
    new HBaseAdmin(conf3).createTable(tabB);
    new HBaseAdmin(conf3).createTable(tabC);

    HTable htab1A = new HTable(conf1, tabAName);
    HTable htab2A = new HTable(conf2, tabAName);
    HTable htab3A = new HTable(conf3, tabAName);

    HTable htab1B = new HTable(conf1, tabBName);
    HTable htab2B = new HTable(conf2, tabBName);
    HTable htab3B = new HTable(conf3, tabBName);

    HTable htab1C = new HTable(conf1, tabCName);
    HTable htab2C = new HTable(conf2, tabCName);
    HTable htab3C = new HTable(conf3, tabCName);

    // A. add cluster2/cluster3 as peers to cluster1
    admin1.addPeer("2", utility2.getClusterKey(), "TC;TB:f1,f3");
    admin1.addPeer("3", utility3.getClusterKey(), "TA;TB:f1,f2");

    // A1. tableA can only replicated to cluster3
    putAndWaitWithFamily(row1, f1Name, htab1A, htab3A);
    ensureRowNotReplicated(row1, f1Name, htab2A);
    deleteAndWaitWithFamily(row1, f1Name, htab1A, htab3A);

    putAndWaitWithFamily(row1, f2Name, htab1A, htab3A);
    ensureRowNotReplicated(row1, f2Name, htab2A);
    deleteAndWaitWithFamily(row1, f2Name, htab1A, htab3A);

    putAndWaitWithFamily(row1, f3Name, htab1A, htab3A);
    ensureRowNotReplicated(row1, f3Name, htab2A);
    deleteAndWaitWithFamily(row1, f3Name, htab1A, htab3A);

    // A2. cf 'f1' of tableB can replicated to both cluster2 and cluster3
    putAndWaitWithFamily(row1, f1Name, htab1B, htab2B, htab3B);
    deleteAndWaitWithFamily(row1, f1Name, htab1B, htab2B, htab3B);

    //  cf 'f2' of tableB can only replicated to cluster3
    putAndWaitWithFamily(row1, f2Name, htab1B, htab3B);
    ensureRowNotReplicated(row1, f2Name, htab2B);
    deleteAndWaitWithFamily(row1, f2Name, htab1B, htab3B);

    //  cf 'f3' of tableB can only replicated to cluster2
    putAndWaitWithFamily(row1, f3Name, htab1B, htab2B);
    ensureRowNotReplicated(row1, f3Name, htab3B);
    deleteAndWaitWithFamily(row1, f3Name, htab1B, htab2B);

    // A3. tableC can only replicated to cluster2
    putAndWaitWithFamily(row1, f1Name, htab1C, htab2C);
    ensureRowNotReplicated(row1, f1Name, htab3C);
    deleteAndWaitWithFamily(row1, f1Name, htab1C, htab2C);

    putAndWaitWithFamily(row1, f2Name, htab1C, htab2C);
    ensureRowNotReplicated(row1, f2Name, htab3C);
    deleteAndWaitWithFamily(row1, f2Name, htab1C, htab2C);

    putAndWaitWithFamily(row1, f3Name, htab1C, htab2C);
    ensureRowNotReplicated(row1, f3Name, htab3C);
    deleteAndWaitWithFamily(row1, f3Name, htab1C, htab2C);

    // B. change peers' replicable table-cf config
    admin1.setPeerTableCFs("2", "TA:f1,f2; TC:f2,f3");
    admin1.setPeerTableCFs("3", "TB; TC:f3");

    // B1. cf 'f1' of tableA can only replicated to cluster2
    putAndWaitWithFamily(row2, f1Name, htab1A, htab2A);
    ensureRowNotReplicated(row2, f1Name, htab3A);
    deleteAndWaitWithFamily(row2, f1Name, htab1A, htab2A);
    //     cf 'f2' of tableA can only replicated to cluster2
    putAndWaitWithFamily(row2, f2Name, htab1A, htab2A);
    ensureRowNotReplicated(row2, f2Name, htab3A);
    deleteAndWaitWithFamily(row2, f2Name, htab1A, htab2A);
    //     cf 'f3' of tableA isn't replicable to either cluster2 or cluster3
    putAndWaitWithFamily(row2, f3Name, htab1A);
    ensureRowNotReplicated(row2, f3Name, htab2A, htab3A);
    deleteAndWaitWithFamily(row2, f3Name, htab1A);

    // B2. tableB can only replicated to cluster3
    putAndWaitWithFamily(row2, f1Name, htab1B, htab3B);
    ensureRowNotReplicated(row2, f1Name, htab2B);
    deleteAndWaitWithFamily(row2, f1Name, htab1B, htab3B);

    putAndWaitWithFamily(row2, f2Name, htab1B, htab3B);
    ensureRowNotReplicated(row2, f2Name, htab2B);
    deleteAndWaitWithFamily(row2, f2Name, htab1B, htab3B);

    putAndWaitWithFamily(row2, f3Name, htab1B, htab3B);
    ensureRowNotReplicated(row2, f3Name, htab2B);
    deleteAndWaitWithFamily(row2, f3Name, htab1B, htab3B);

    // B3. cf 'f1' of tableC non-replicable to either cluster
    putAndWaitWithFamily(row2, f1Name, htab1C);
    ensureRowNotReplicated(row2, f1Name, htab2C, htab3C);
    deleteAndWaitWithFamily(row2, f1Name, htab1C);
    //     cf 'f2' of tableC can only replicated to cluster2
    putAndWaitWithFamily(row2, f2Name, htab1C, htab2C);
    ensureRowNotReplicated(row2, f2Name, htab3C);
    deleteAndWaitWithFamily(row2, f2Name, htab1C, htab2C);
    //     cf 'f3' of tableC can replicated to cluster2 and cluster3
    putAndWaitWithFamily(row2, f3Name, htab1C, htab2C, htab3C);
    deleteAndWaitWithFamily(row2, f3Name, htab1C, htab2C, htab3C);
 }

  private void ensureRowNotReplicated(byte[] row, byte[] fam, HTable... tables) throws IOException {
    Get get = new Get(row);
    get.addFamily(fam);
    for (HTable table : tables) {
      Result res = table.get(get);
      assertEquals(0, res.size());
    }
  }

  private void deleteAndWaitWithFamily(byte[] row, byte[] fam,
      HTable source, HTable... targets)
    throws Exception {
    Delete del = new Delete(row);
    del.deleteFamily(fam);
    source.delete(del);

    Get get = new Get(row);
    get.addFamily(fam);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      boolean removedFromAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
          removedFromAll = false;
          break;
        }
      }
      if (removedFromAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  private void putAndWaitWithFamily(byte[] row, byte[] fam,
      HTable source, HTable... targets)
    throws Exception {
    Put put = new Put(row);
    put.add(fam, row, val);
    source.put(put);

    Get get = new Get(row);
    get.addFamily(fam);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      boolean replicatedToAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() == 0) {
          LOG.info("Row not available");
          replicatedToAll = false;
          break;
        } else {
          assertEquals(res.size(), 1);
          assertArrayEquals(res.value(), val);
        }
      }
      if (replicatedToAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }
}
