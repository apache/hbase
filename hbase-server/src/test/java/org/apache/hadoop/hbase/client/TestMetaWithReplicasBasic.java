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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category({ MiscTests.class, MediumTests.class })
public class TestMetaWithReplicasBasic extends MetaWithReplicasTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaWithReplicasBasic.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster();
  }

  @Test
  public void testMetaHTDReplicaCount() throws Exception {
    assertEquals(3,
      TEST_UTIL.getAdmin().getDescriptor(TableName.META_TABLE_NAME).getRegionReplication());
  }

  @Test
  public void testZookeeperNodesForReplicas() throws Exception {
    // Checks all the znodes exist when meta's replicas are enabled
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    Configuration conf = TEST_UTIL.getConfiguration();
    String baseZNode =
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode =
      ZNodePaths.joinZNode(baseZNode, conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ProtobufUtil.parseServerNameFrom(data);
    for (int i = 1; i < 3; i++) {
      String secZnode = ZNodePaths.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server") + "-" + i);
      String str = zkw.getZNodePaths().getZNodeForReplica(i);
      assertTrue(str.equals(secZnode));
      // check that the data in the znode is parseable (this would also mean the znode exists)
      data = ZKUtil.getData(zkw, secZnode);
      ProtobufUtil.parseServerNameFrom(data);
    }
  }

  @Test
  public void testAccessingUnknownTables() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);
    Table table = TEST_UTIL.getConnection().getTable(name.getTableName());
    Get get = new Get(Bytes.toBytes("foo"));
    assertThrows(TableNotFoundException.class, () -> table.get(get));
  }
}
