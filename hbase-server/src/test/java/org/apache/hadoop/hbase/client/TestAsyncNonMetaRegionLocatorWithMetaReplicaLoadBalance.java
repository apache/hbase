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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncNonMetaRegionLocatorWithMetaReplicaLoadBalance
  extends TestAsyncNonMetaRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncNonMetaRegionLocatorWithMetaReplicaLoadBalance.class);

  private static final int NB_SERVERS = 4;
  private static int numOfMetaReplica = NB_SERVERS - 1;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);
    // Enable hbase:meta replication.
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CATALOG_CONF_KEY,
      true);
    // Set hbase:meta replicas to be 3.
    conf.setInt(HConstants.META_REPLICAS_NUM, numOfMetaReplica);
    TEST_UTIL.startMiniCluster(NB_SERVERS);
    TEST_UTIL.waitFor(30000, () -> TEST_UTIL.getMiniHBaseCluster().getRegions(
      TableName.META_TABLE_NAME).size() >= numOfMetaReplica);

    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    ConnectionRegistry registry =
        ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());

    // Enable meta replica LoadBalance mode for this connection.
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.set(HConstants.META_REPLICAS_MODE, "LoadBalance");
    c.setBoolean(HConstants.USE_META_REPLICAS, true);

    CONN = new AsyncConnectionImpl(c, registry,
      registry.getClusterId().get(), null, User.getCurrent());
    LOCATOR = new AsyncNonMetaRegionLocator(CONN);
    SPLIT_KEYS = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      SPLIT_KEYS[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
  }
}
