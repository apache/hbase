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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category({ MiscTests.class, MediumTests.class })
public class TestMetaReplicasAddressChange extends MetaWithReplicasTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaReplicasAddressChange.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaReplicasAddressChange.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster();
  }

  @Test
  public void testMetaAddressChange() throws Exception {
    // checks that even when the meta's location changes, the various
    // caches update themselves. Uses the master operations to test
    // this
    Configuration conf = TEST_UTIL.getConfiguration();
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    String baseZNode =
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode =
      ZNodePaths.joinZNode(baseZNode, conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName currentServer = ProtobufUtil.toServerName(data);
    Collection<ServerName> liveServers = TEST_UTIL.getAdmin()
      .getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().keySet();
    ServerName moveToServer =
      liveServers.stream().filter(s -> !currentServer.equals(s)).findAny().get();
    final TableName tableName = name.getTableName();
    TEST_UTIL.createTable(tableName, "f");
    assertTrue(TEST_UTIL.getAdmin().tableExists(tableName));
    RegionInfo metaRegionInfo = TEST_UTIL.getAdmin().getRegions(TableName.META_TABLE_NAME).get(0);
    TEST_UTIL.getAdmin().move(metaRegionInfo.getEncodedNameAsBytes(), moveToServer);
    assertNotEquals(currentServer, moveToServer);
    LOG.debug("CurrentServer={}, moveToServer={}", currentServer, moveToServer);
    TEST_UTIL.waitFor(60000, () -> {
      byte[] bytes = ZKUtil.getData(zkw, primaryMetaZnode);
      ServerName actualServer = ProtobufUtil.toServerName(bytes);
      return moveToServer.equals(actualServer);
    });
    TEST_UTIL.getAdmin().disableTable(tableName);
    assertTrue(TEST_UTIL.getAdmin().isTableDisabled(tableName));
  }
}
