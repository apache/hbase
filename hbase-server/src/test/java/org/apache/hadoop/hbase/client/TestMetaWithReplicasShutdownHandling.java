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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category({ MiscTests.class, MediumTests.class })
public class TestMetaWithReplicasShutdownHandling extends MetaWithReplicasTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaWithReplicasShutdownHandling.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMetaWithReplicasShutdownHandling.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster();
  }

  @Test
  public void testShutdownHandling() throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    shutdownMetaAndDoValidations(TEST_UTIL);
  }

  public static void shutdownMetaAndDoValidations(HBaseTestingUtility util) throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    ZKWatcher zkw = util.getZooKeeperWatcher();
    Configuration conf = util.getConfiguration();
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);

    String baseZNode =
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode =
      ZNodePaths.joinZNode(baseZNode, conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName primary = ProtobufUtil.parseServerNameFrom(data);
    LOG.info("Primary=" + primary.toString());

    TableName TABLE = TableName.valueOf("testShutdownHandling");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    if (util.getAdmin().tableExists(TABLE)) {
      util.getAdmin().disableTable(TABLE);
      util.getAdmin().deleteTable(TABLE);
    }
    byte[] row = Bytes.toBytes("test");
    ServerName master = null;
    try (Connection c = ConnectionFactory.createConnection(util.getConfiguration())) {
      try (Table htable = util.createTable(TABLE, FAMILIES)) {
        util.getAdmin().flush(TableName.META_TABLE_NAME);
        Thread.sleep(
          conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 30000) * 6);
        List<RegionInfo> regions = MetaTableAccessor.getTableRegions(c, TABLE);
        HRegionLocation hrl = MetaTableAccessor.getRegionLocation(c, regions.get(0));
        // Ensure that the primary server for test table is not the same one as the primary
        // of the meta region since we will be killing the srv holding the meta's primary...
        // We want to be able to write to the test table even when the meta is not present ..
        // If the servers are the same, then move the test table's region out of the server
        // to another random server
        if (hrl.getServerName().equals(primary)) {
          util.getAdmin().move(hrl.getRegion().getEncodedNameAsBytes());
          // wait for the move to complete
          do {
            Thread.sleep(10);
            hrl = MetaTableAccessor.getRegionLocation(c, regions.get(0));
          } while (primary.equals(hrl.getServerName()));
          util.getAdmin().flush(TableName.META_TABLE_NAME);
          Thread.sleep(
            conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 30000) * 3);
        }
        // Ensure all metas are not on same hbase:meta replica=0 server!

        master = util.getHBaseClusterInterface().getClusterMetrics().getMasterName();
        // kill the master so that regionserver recovery is not triggered at all
        // for the meta server
        LOG.info("Stopping master=" + master.toString());
        util.getHBaseClusterInterface().stopMaster(master);
        util.getHBaseClusterInterface().waitForMasterToStop(master, 60000);
        LOG.info("Master " + master + " stopped!");
        if (!master.equals(primary)) {
          util.getHBaseClusterInterface().killRegionServer(primary);
          util.getHBaseClusterInterface().waitForRegionServerToStop(primary, 60000);
        }
        c.clearRegionLocationCache();
      }
      LOG.info("Running GETs");
      try (Table htable = c.getTable(TABLE)) {
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("foo"), row, row);
        BufferedMutator m = c.getBufferedMutator(TABLE);
        m.mutate(put);
        m.flush();
        // Try to do a get of the row that was just put
        Result r = htable.get(new Get(row));
        assertTrue(Arrays.equals(r.getRow(), row));
        // now start back the killed servers and disable use of replicas. That would mean
        // calls go to the primary
        LOG.info("Starting Master");
        util.getHBaseClusterInterface().startMaster(master.getHostname(), 0);
        util.getHBaseClusterInterface().startRegionServer(primary.getHostname(), 0);
        util.getHBaseClusterInterface().waitForActiveAndReadyMaster();
        LOG.info("Master active!");
        c.clearRegionLocationCache();
      }
    }
    conf.setBoolean(HConstants.USE_META_REPLICAS, false);
    LOG.info("Running GETs no replicas");
    try (Connection c = ConnectionFactory.createConnection(conf);
      Table htable = c.getTable(TABLE)) {
      Result r = htable.get(new Get(row));
      assertArrayEquals(row, r.getRow());
    }
  }
}
