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
package org.apache.hadoop.hbase.replication.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ZKReplicationPeerStorage;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
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

@Category({ReplicationTests.class, SmallTests.class})
public class TestTableCFsUpdater extends ReplicationPeerConfigUpgrader {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableCFsUpdater.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTableCFsUpdater.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZKWatcher zkw = null;
  private static Abortable abortable = null;
  private static ZKStorageUtil zkStorageUtil = null;

  private static class ZKStorageUtil extends ZKReplicationPeerStorage {
    public ZKStorageUtil(ZKWatcher zookeeper, Configuration conf) {
      super(zookeeper, conf);
    }
  }

  @Rule
  public TestName name = new TestName();

  public TestTableCFsUpdater() {
    super(zkw, TEST_UTIL.getConfiguration());
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.info(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    zkw = new ZKWatcher(conf, "TableCFs", abortable, true);
    zkStorageUtil = new ZKStorageUtil(zkw, conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testUpgrade() throws Exception {
    String peerId = "1";
    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    final TableName tableName3 = TableName.valueOf(name.getMethodName() + "3");

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(zkw.getQuorum());
    String peerNode = zkStorageUtil.getPeerNode(peerId);
    ZKUtil.createWithParents(zkw, peerNode, ReplicationPeerConfigUtil.toByteArray(rpc));

    String tableCFs = tableName1 + ":cf1,cf2;" + tableName2 + ":cf3;" + tableName3;
    String tableCFsNode = getTableCFsNode(peerId);
    LOG.info("create tableCFs :" + tableCFsNode + " for peerId=" + peerId);
    ZKUtil.createWithParents(zkw, tableCFsNode, Bytes.toBytes(tableCFs));

    ReplicationPeerConfig actualRpc =
        ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    String actualTableCfs = Bytes.toString(ZKUtil.getData(zkw, tableCFsNode));

    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    assertNull(actualRpc.getTableCFsMap());
    assertEquals(tableCFs, actualTableCfs);

    peerId = "2";
    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(zkw.getQuorum());
    peerNode = zkStorageUtil.getPeerNode(peerId);
    ZKUtil.createWithParents(zkw, peerNode, ReplicationPeerConfigUtil.toByteArray(rpc));

    tableCFs = tableName1 + ":cf1,cf3;" + tableName2 + ":cf2";
    tableCFsNode = getTableCFsNode(peerId);
    LOG.info("create tableCFs :" + tableCFsNode + " for peerId=" + peerId);
    ZKUtil.createWithParents(zkw, tableCFsNode, Bytes.toBytes(tableCFs));

    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    actualTableCfs = Bytes.toString(ZKUtil.getData(zkw, tableCFsNode));

    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    assertNull(actualRpc.getTableCFsMap());
    assertEquals(tableCFs, actualTableCfs);

    peerId = "3";
    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(zkw.getQuorum());
    peerNode = zkStorageUtil.getPeerNode(peerId);
    ZKUtil.createWithParents(zkw, peerNode, ReplicationPeerConfigUtil.toByteArray(rpc));

    tableCFs = "";
    tableCFsNode = getTableCFsNode(peerId);
    LOG.info("create tableCFs :" + tableCFsNode + " for peerId=" + peerId);
    ZKUtil.createWithParents(zkw, tableCFsNode, Bytes.toBytes(tableCFs));

    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    actualTableCfs = Bytes.toString(ZKUtil.getData(zkw, tableCFsNode));

    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    assertNull(actualRpc.getTableCFsMap());
    assertEquals(tableCFs, actualTableCfs);

    peerId = "4";
    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(zkw.getQuorum());
    peerNode = zkStorageUtil.getPeerNode(peerId);
    ZKUtil.createWithParents(zkw, peerNode, ReplicationPeerConfigUtil.toByteArray(rpc));

    tableCFsNode = getTableCFsNode(peerId);
    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    actualTableCfs = Bytes.toString(ZKUtil.getData(zkw, tableCFsNode));

    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    assertNull(actualRpc.getTableCFsMap());
    assertNull(actualTableCfs);

    copyTableCFs();

    peerId = "1";
    peerNode = zkStorageUtil.getPeerNode(peerId);
    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    Map<TableName, List<String>> tableNameListMap = actualRpc.getTableCFsMap();
    assertEquals(3, tableNameListMap.size());
    assertTrue(tableNameListMap.containsKey(tableName1));
    assertTrue(tableNameListMap.containsKey(tableName2));
    assertTrue(tableNameListMap.containsKey(tableName3));
    assertEquals(2, tableNameListMap.get(tableName1).size());
    assertEquals("cf1", tableNameListMap.get(tableName1).get(0));
    assertEquals("cf2", tableNameListMap.get(tableName1).get(1));
    assertEquals(1, tableNameListMap.get(tableName2).size());
    assertEquals("cf3", tableNameListMap.get(tableName2).get(0));
    assertNull(tableNameListMap.get(tableName3));

    peerId = "2";
    peerNode = zkStorageUtil.getPeerNode(peerId);
    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    tableNameListMap = actualRpc.getTableCFsMap();
    assertEquals(2, tableNameListMap.size());
    assertTrue(tableNameListMap.containsKey(tableName1));
    assertTrue(tableNameListMap.containsKey(tableName2));
    assertEquals(2, tableNameListMap.get(tableName1).size());
    assertEquals("cf1", tableNameListMap.get(tableName1).get(0));
    assertEquals("cf3", tableNameListMap.get(tableName1).get(1));
    assertEquals(1, tableNameListMap.get(tableName2).size());
    assertEquals("cf2", tableNameListMap.get(tableName2).get(0));

    peerId = "3";
    peerNode = zkStorageUtil.getPeerNode(peerId);
    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    tableNameListMap = actualRpc.getTableCFsMap();
    assertNull(tableNameListMap);

    peerId = "4";
    peerNode = zkStorageUtil.getPeerNode(peerId);
    actualRpc = ReplicationPeerConfigUtil.parsePeerFrom(ZKUtil.getData(zkw, peerNode));
    assertEquals(rpc.getClusterKey(), actualRpc.getClusterKey());
    tableNameListMap = actualRpc.getTableCFsMap();
    assertNull(tableNameListMap);
  }
}
