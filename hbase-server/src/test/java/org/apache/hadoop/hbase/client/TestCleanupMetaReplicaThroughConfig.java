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

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure we will honor the {@link HConstants#META_REPLICAS_NUM}.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestCleanupMetaReplicaThroughConfig extends MetaWithReplicasTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCleanupMetaReplicaThroughConfig.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster();
  }

  @Test
  public void testReplicaCleanup() throws Exception {
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    List<String> metaReplicaZnodes = zkw.getMetaReplicaNodes();
    assertEquals(3, metaReplicaZnodes.size());

    final HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    master.stop("Restarting");
    TEST_UTIL.waitFor(30000, () -> master.isStopped());
    TEST_UTIL.getMiniHBaseCluster().getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 1);

    JVMClusterUtil.MasterThread newMasterThread = TEST_UTIL.getMiniHBaseCluster().startMaster();
    final HMaster newMaster = newMasterThread.getMaster();

    // wait until new master finished meta replica assignment logic
    TEST_UTIL.waitFor(30000, () -> newMaster.getMasterQuotaManager() != null);
    TEST_UTIL.waitFor(30000,
      () -> TEST_UTIL.getZooKeeperWatcher().getMetaReplicaNodes().size() == 1);
  }
}
