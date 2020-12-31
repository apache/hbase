/*
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestZKConnectionRegistry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZKConnectionRegistry.class);

  @Rule
  public final TestName name = new TestName();

  static final Logger LOG = LoggerFactory.getLogger(TestZKConnectionRegistry.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZKConnectionRegistry REGISTRY;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    HBaseTestingUtility.setReplicas(TEST_UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
    REGISTRY = new ZKConnectionRegistry(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(REGISTRY, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException, IOException {
    LOG.info("STARTED TEST");
    String clusterId = REGISTRY.getClusterId().get();
    String expectedClusterId = TEST_UTIL.getHBaseCluster().getMaster().getClusterId();
    assertEquals("Expected " + expectedClusterId + ", found=" + clusterId, expectedClusterId,
      clusterId);
    assertEquals(TEST_UTIL.getHBaseCluster().getMaster().getServerName(),
      REGISTRY.getActiveMaster().get());
    RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, REGISTRY);
    RegionLocations locs = REGISTRY.getMetaRegionLocations().get();
    assertEquals(3, locs.getRegionLocations().length);
    IntStream.range(0, 3).forEach(i -> {
      HRegionLocation loc = locs.getRegionLocation(i);
      assertNotNull("Replica " + i + " doesn't have location", loc);
      assertEquals(TableName.META_TABLE_NAME, loc.getRegion().getTable());
      assertEquals(i, loc.getRegion().getReplicaId());
    });
  }

  @Test
  public void testIndependentZKConnections() throws IOException {
    try (ReadOnlyZKClient zk1 = REGISTRY.getZKClient()) {
      Configuration otherConf = new Configuration(TEST_UTIL.getConfiguration());
      otherConf.set(HConstants.ZOOKEEPER_QUORUM, MiniZooKeeperCluster.HOST);
      try (ZKConnectionRegistry otherRegistry = new ZKConnectionRegistry(otherConf)) {
        ReadOnlyZKClient zk2 = otherRegistry.getZKClient();
        assertNotSame("Using a different configuration / quorum should result in " +
            "different backing zk connection.", zk1, zk2);
        assertNotEquals(
          "Using a different configrution / quorum should be reflected in the zk connection.",
          zk1.getConnectString(), zk2.getConnectString());
      }
    } finally {
      LOG.info("DONE!");
    }
  }

  @Test
  public void testNoMetaAvailable() throws InterruptedException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set("zookeeper.znode.metaserver", "whatever");
    try (ZKConnectionRegistry registry = new ZKConnectionRegistry(conf)) {
      try {
        registry.getMetaRegionLocations().get();
        fail("Should have failed since we set an incorrect meta znode prefix");
      } catch (ExecutionException e) {
        assertThat(e.getCause(), instanceOf(IOException.class));
      }
    }
  }

  /**
   * Pass discontinuous list of znodes to registry getMetaRegionLocation. Should work fine.
   * It used to throw ArrayOutOfBoundsException. See HBASE-25280.
   */
  @Test
  public void testDiscontinuousLocations()
    throws ExecutionException, InterruptedException, IOException, KeeperException,
    TimeoutException {
    // Write discontinuous meta replica locations to a zk namespace particular to this test to
    // avoid polluting other tests.
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/" + this.name.getMethodName());
    ZooKeeperProtos.MetaRegionServer pbrsr = ZooKeeperProtos.MetaRegionServer.newBuilder()
      .setServer(ProtobufUtil.toServerName(ServerName.valueOf("example.org,1,1")))
      .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
      .setState(RegionState.State.OPEN.convert()).build();
    byte[] data = ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
    try (ZKWatcher zkw = new ZKWatcher(conf, this.name.getMethodName(), new Abortable() {
      @Override public void abort(String why, Throwable e) {}
      @Override public boolean isAborted() {
        return false;
      }
    })) {
      // Write default replica and then a replica for replicaId #3.
      ZKUtil.createSetData(zkw, zkw.getZNodePaths().getZNodeForReplica(0), data);
      ZKUtil.createSetData(zkw, zkw.getZNodePaths().getZNodeForReplica(3), data);
      List<String> znodes = zkw.getMetaReplicaNodes();
      assertEquals(2, znodes.size());
      try (ZKConnectionRegistry registry = new ZKConnectionRegistry(conf)) {
        CompletableFuture<RegionLocations> cf = registry.getMetaRegionLocations();
        RegionLocations locations = cf.get(60, TimeUnit.SECONDS);
        assertEquals(2, locations.numNonNullElements());
      }
    }
  }
}
