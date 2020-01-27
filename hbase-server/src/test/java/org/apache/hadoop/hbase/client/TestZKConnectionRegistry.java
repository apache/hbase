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

import static org.apache.hadoop.hbase.HConstants.META_REPLICAS_NUM;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class, ClientTests.class })
public class TestZKConnectionRegistry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZKConnectionRegistry.class);

  static final Logger LOG = LoggerFactory.getLogger(TestZKConnectionRegistry.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZKConnectionRegistry REGISTRY;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(META_REPLICAS_NUM, 3);
    TEST_UTIL.startMiniCluster(3);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    // make sure that we do not depend on this config when getting locations for meta replicas, see
    // HBASE-21658.
    conf.setInt(META_REPLICAS_NUM, 1);
    REGISTRY = new ZKConnectionRegistry(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
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
    RegionReplicaTestHelper
      .waitUntilAllMetaReplicasAreReady(TEST_UTIL, REGISTRY);
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
      otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      try (ZKConnectionRegistry otherRegistry = new ZKConnectionRegistry(otherConf)) {
        ReadOnlyZKClient zk2 = otherRegistry.getZKClient();
        assertNotSame("Using a different configuration / quorum should result in different " +
          "backing zk connection.", zk1, zk2);
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
}
