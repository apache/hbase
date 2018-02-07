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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

@Category({FlakeyTests.class, LargeTests.class})
public class TestPerTableCFReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPerTableCFReplication.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestPerTableCFReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration conf3;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static HBaseTestingUtility utility3;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 100;

  private static final TableName tableName = TableName.valueOf("test");
  private static final TableName tabAName = TableName.valueOf("TA");
  private static final TableName tabBName = TableName.valueOf("TB");
  private static final TableName tabCName = TableName.valueOf("TC");
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

  @Rule
  public TestName name = new TestName();

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
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZKWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    conf3 = new Configuration(conf1);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZKWatcher(conf2, "cluster3", null, true);

    utility3 = new HBaseTestingUtility(conf3);
    utility3.setZkCluster(miniZK);
    new ZKWatcher(conf3, "cluster3", null, true);

    table = new HTableDescriptor(tableName);
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
    Map<TableName, List<String>> tabCFsMap = null;

    // 1. null or empty string, result should be null
    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(null);
    assertEquals(null, tabCFsMap);

    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig("");
    assertEquals(null, tabCFsMap);

    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig("   ");
    assertEquals(null, tabCFsMap);

    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    final TableName tableName3 = TableName.valueOf(name.getMethodName() + "3");

    // 2. single table: "tableName1" / "tableName2:cf1" / "tableName3:cf1,cf3"
    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableName1.getNameAsString());
    assertEquals(1, tabCFsMap.size()); // only one table
    assertTrue(tabCFsMap.containsKey(tableName1));   // its table name is "tableName1"
    assertFalse(tabCFsMap.containsKey(tableName2));  // not other table
    assertEquals(null, tabCFsMap.get(tableName1));   // null cf-list,

    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableName2 + ":cf1");
    assertEquals(1, tabCFsMap.size()); // only one table
    assertTrue(tabCFsMap.containsKey(tableName2));   // its table name is "tableName2"
    assertFalse(tabCFsMap.containsKey(tableName1));  // not other table
    assertEquals(1, tabCFsMap.get(tableName2).size());   // cf-list contains only 1 cf
    assertEquals("cf1", tabCFsMap.get(tableName2).get(0));// the only cf is "cf1"

    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableName3 + " : cf1 , cf3");
    assertEquals(1, tabCFsMap.size()); // only one table
    assertTrue(tabCFsMap.containsKey(tableName3));   // its table name is "tableName2"
    assertFalse(tabCFsMap.containsKey(tableName1));  // not other table
    assertEquals(2, tabCFsMap.get(tableName3).size());   // cf-list contains 2 cf
    assertTrue(tabCFsMap.get(tableName3).contains("cf1"));// contains "cf1"
    assertTrue(tabCFsMap.get(tableName3).contains("cf3"));// contains "cf3"

    // 3. multiple tables: "tableName1 ; tableName2:cf1 ; tableName3:cf1,cf3"
    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableName1 + " ; " + tableName2
            + ":cf1 ; " + tableName3 + ":cf1,cf3");
    // 3.1 contains 3 tables : "tableName1", "tableName2" and "tableName3"
    assertEquals(3, tabCFsMap.size());
    assertTrue(tabCFsMap.containsKey(tableName1));
    assertTrue(tabCFsMap.containsKey(tableName2));
    assertTrue(tabCFsMap.containsKey(tableName3));
    // 3.2 table "tab1" : null cf-list
    assertEquals(null, tabCFsMap.get(tableName1));
    // 3.3 table "tab2" : cf-list contains a single cf "cf1"
    assertEquals(1, tabCFsMap.get(tableName2).size());
    assertEquals("cf1", tabCFsMap.get(tableName2).get(0));
    // 3.4 table "tab3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get(tableName3).size());
    assertTrue(tabCFsMap.get(tableName3).contains("cf1"));
    assertTrue(tabCFsMap.get(tableName3).contains("cf3"));

    // 4. contiguous or additional ";"(table delimiter) or ","(cf delimiter) can be tolerated
    // still use the example of multiple tables: "tableName1 ; tableName2:cf1 ; tableName3:cf1,cf3"
    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(
      tableName1 + " ; ; " + tableName2 + ":cf1 ; " + tableName3 + ":cf1,,cf3 ;");
    // 4.1 contains 3 tables : "tableName1", "tableName2" and "tableName3"
    assertEquals(3, tabCFsMap.size());
    assertTrue(tabCFsMap.containsKey(tableName1));
    assertTrue(tabCFsMap.containsKey(tableName2));
    assertTrue(tabCFsMap.containsKey(tableName3));
    // 4.2 table "tab1" : null cf-list
    assertEquals(null, tabCFsMap.get(tableName1));
    // 4.3 table "tab2" : cf-list contains a single cf "cf1"
    assertEquals(1, tabCFsMap.get(tableName2).size());
    assertEquals("cf1", tabCFsMap.get(tableName2).get(0));
    // 4.4 table "tab3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get(tableName3).size());
    assertTrue(tabCFsMap.get(tableName3).contains("cf1"));
    assertTrue(tabCFsMap.get(tableName3).contains("cf3"));

    // 5. invalid format "tableName1:tt:cf1 ; tableName2::cf1 ; tableName3:cf1,cf3"
    //    "tableName1:tt:cf1" and "tableName2::cf1" are invalid and will be ignored totally
    tabCFsMap = ReplicationPeerConfigUtil.parseTableCFsFromConfig(
      tableName1 + ":tt:cf1 ; " + tableName2 + "::cf1 ; " + tableName3 + ":cf1,cf3");
    // 5.1 no "tableName1" and "tableName2", only "tableName3"
    assertEquals(1, tabCFsMap.size()); // only one table
    assertFalse(tabCFsMap.containsKey(tableName1));
    assertFalse(tabCFsMap.containsKey(tableName2));
    assertTrue(tabCFsMap.containsKey(tableName3));
   // 5.2 table "tableName3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get(tableName3).size());
    assertTrue(tabCFsMap.get(tableName3).contains("cf1"));
    assertTrue(tabCFsMap.get(tableName3).contains("cf3"));
 }

  @Test
  public void testTableCFsHelperConverter() {

    ReplicationProtos.TableCF[] tableCFs = null;
    Map<TableName, List<String>> tabCFsMap = null;

    // 1. null or empty string, result should be null
    assertNull(ReplicationPeerConfigUtil.convert(tabCFsMap));

    tabCFsMap = new HashMap<>();
    tableCFs = ReplicationPeerConfigUtil.convert(tabCFsMap);
    assertEquals(0, tableCFs.length);

    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    final TableName tableName3 = TableName.valueOf(name.getMethodName() + "3");

    // 2. single table: "tab1" / "tab2:cf1" / "tab3:cf1,cf3"
    tabCFsMap.clear();
    tabCFsMap.put(tableName1, null);
    tableCFs = ReplicationPeerConfigUtil.convert(tabCFsMap);
    assertEquals(1, tableCFs.length); // only one table
    assertEquals(tableName1.toString(),
        tableCFs[0].getTableName().getQualifier().toStringUtf8());
    assertEquals(0, tableCFs[0].getFamiliesCount());

    tabCFsMap.clear();
    tabCFsMap.put(tableName2, new ArrayList<>());
    tabCFsMap.get(tableName2).add("cf1");
    tableCFs = ReplicationPeerConfigUtil.convert(tabCFsMap);
    assertEquals(1, tableCFs.length); // only one table
    assertEquals(tableName2.toString(),
        tableCFs[0].getTableName().getQualifier().toStringUtf8());
    assertEquals(1, tableCFs[0].getFamiliesCount());
    assertEquals("cf1", tableCFs[0].getFamilies(0).toStringUtf8());

    tabCFsMap.clear();
    tabCFsMap.put(tableName3, new ArrayList<>());
    tabCFsMap.get(tableName3).add("cf1");
    tabCFsMap.get(tableName3).add("cf3");
    tableCFs = ReplicationPeerConfigUtil.convert(tabCFsMap);
    assertEquals(1, tableCFs.length);
    assertEquals(tableName3.toString(),
        tableCFs[0].getTableName().getQualifier().toStringUtf8());
    assertEquals(2, tableCFs[0].getFamiliesCount());
    assertEquals("cf1", tableCFs[0].getFamilies(0).toStringUtf8());
    assertEquals("cf3", tableCFs[0].getFamilies(1).toStringUtf8());

    tabCFsMap.clear();
    tabCFsMap.put(tableName1, null);
    tabCFsMap.put(tableName2, new ArrayList<>());
    tabCFsMap.get(tableName2).add("cf1");
    tabCFsMap.put(tableName3, new ArrayList<>());
    tabCFsMap.get(tableName3).add("cf1");
    tabCFsMap.get(tableName3).add("cf3");

    tableCFs = ReplicationPeerConfigUtil.convert(tabCFsMap);
    assertEquals(3, tableCFs.length);
    assertNotNull(ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName1.toString()));
    assertNotNull(ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName2.toString()));
    assertNotNull(ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName3.toString()));

    assertEquals(0,
        ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName1.toString()).getFamiliesCount());

    assertEquals(1, ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName2.toString())
        .getFamiliesCount());
    assertEquals("cf1", ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName2.toString())
        .getFamilies(0).toStringUtf8());

    assertEquals(2, ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName3.toString())
        .getFamiliesCount());
    assertEquals("cf1", ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName3.toString())
        .getFamilies(0).toStringUtf8());
    assertEquals("cf3", ReplicationPeerConfigUtil.getTableCF(tableCFs, tableName3.toString())
        .getFamilies(1).toStringUtf8());

    tabCFsMap = ReplicationPeerConfigUtil.convert2Map(tableCFs);
    assertEquals(3, tabCFsMap.size());
    assertTrue(tabCFsMap.containsKey(tableName1));
    assertTrue(tabCFsMap.containsKey(tableName2));
    assertTrue(tabCFsMap.containsKey(tableName3));
    // 3.2 table "tab1" : null cf-list
    assertEquals(null, tabCFsMap.get(tableName1));
    // 3.3 table "tab2" : cf-list contains a single cf "cf1"
    assertEquals(1, tabCFsMap.get(tableName2).size());
    assertEquals("cf1", tabCFsMap.get(tableName2).get(0));
    // 3.4 table "tab3" : cf-list contains "cf1" and "cf3"
    assertEquals(2, tabCFsMap.get(tableName3).size());
    assertTrue(tabCFsMap.get(tableName3).contains("cf1"));
    assertTrue(tabCFsMap.get(tableName3).contains("cf3"));
  }

  @Test
  public void testPerTableCFReplication() throws Exception {
    LOG.info("testPerTableCFReplication");
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf1);
    Connection connection1 = ConnectionFactory.createConnection(conf1);
    Connection connection2 = ConnectionFactory.createConnection(conf2);
    Connection connection3 = ConnectionFactory.createConnection(conf3);
    try {
      Admin admin1 = connection1.getAdmin();
      Admin admin2 = connection2.getAdmin();
      Admin admin3 = connection3.getAdmin();

      admin1.createTable(tabA);
      admin1.createTable(tabB);
      admin1.createTable(tabC);
      admin2.createTable(tabA);
      admin2.createTable(tabB);
      admin2.createTable(tabC);
      admin3.createTable(tabA);
      admin3.createTable(tabB);
      admin3.createTable(tabC);

      Table htab1A = connection1.getTable(tabAName);
      Table htab2A = connection2.getTable(tabAName);
      Table htab3A = connection3.getTable(tabAName);

      Table htab1B = connection1.getTable(tabBName);
      Table htab2B = connection2.getTable(tabBName);
      Table htab3B = connection3.getTable(tabBName);

      Table htab1C = connection1.getTable(tabCName);
      Table htab2C = connection2.getTable(tabCName);
      Table htab3C = connection3.getTable(tabCName);

      // A. add cluster2/cluster3 as peers to cluster1
      ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
      rpc2.setClusterKey(utility2.getClusterKey());
      rpc2.setReplicateAllUserTables(false);
      Map<TableName, List<String>> tableCFs = new HashMap<>();
      tableCFs.put(tabCName, null);
      tableCFs.put(tabBName, new ArrayList<>());
      tableCFs.get(tabBName).add("f1");
      tableCFs.get(tabBName).add("f3");
      replicationAdmin.addPeer("2", rpc2, tableCFs);

      ReplicationPeerConfig rpc3 = new ReplicationPeerConfig();
      rpc3.setClusterKey(utility3.getClusterKey());
      rpc3.setReplicateAllUserTables(false);
      tableCFs.clear();
      tableCFs.put(tabAName, null);
      tableCFs.put(tabBName, new ArrayList<>());
      tableCFs.get(tabBName).add("f1");
      tableCFs.get(tabBName).add("f2");
      replicationAdmin.addPeer("3", rpc3, tableCFs);

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
      tableCFs.clear();
      tableCFs.put(tabAName, new ArrayList<>());
      tableCFs.get(tabAName).add("f1");
      tableCFs.get(tabAName).add("f2");
      tableCFs.put(tabCName, new ArrayList<>());
      tableCFs.get(tabCName).add("f2");
      tableCFs.get(tabCName).add("f3");
      replicationAdmin.setPeerTableCFs("2", tableCFs);

      tableCFs.clear();
      tableCFs.put(tabBName, null);
      tableCFs.put(tabCName, new ArrayList<>());
      tableCFs.get(tabCName).add("f3");
      replicationAdmin.setPeerTableCFs("3", tableCFs);

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
    } finally {
      connection1.close();
      connection2.close();
      connection3.close();
    }
  }

  private void ensureRowNotReplicated(byte[] row, byte[] fam, Table... tables) throws IOException {
    Get get = new Get(row);
    get.addFamily(fam);
    for (Table table : tables) {
      Result res = table.get(get);
      assertEquals(0, res.size());
    }
  }

  private void deleteAndWaitWithFamily(byte[] row, byte[] fam,
      Table source, Table... targets)
    throws Exception {
    Delete del = new Delete(row);
    del.addFamily(fam);
    source.delete(del);

    Get get = new Get(row);
    get.addFamily(fam);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      boolean removedFromAll = true;
      for (Table target : targets) {
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
      Table source, Table... targets)
    throws Exception {
    Put put = new Put(row);
    put.addColumn(fam, row, val);
    source.put(put);

    Get get = new Get(row);
    get.addFamily(fam);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      boolean replicatedToAll = true;
      for (Table target : targets) {
        Result res = target.get(get);
        if (res.isEmpty()) {
          LOG.info("Row not available");
          replicatedToAll = false;
          break;
        } else {
          assertEquals(1, res.size());
          assertArrayEquals(val, res.value());
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
