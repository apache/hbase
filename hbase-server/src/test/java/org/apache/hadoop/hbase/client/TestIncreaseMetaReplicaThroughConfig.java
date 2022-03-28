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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure we will honor the {@link HConstants#META_REPLICAS_NUM}.And also test upgrading.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestIncreaseMetaReplicaThroughConfig extends MetaWithReplicasTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncreaseMetaReplicaThroughConfig.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster();
  }

  @Test
  public void testUpgradeAndIncreaseReplicaCount() throws Exception {
    HMaster oldMaster = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TableDescriptors oldTds = oldMaster.getTableDescriptors();
    TableDescriptor oldMetaTd = oldTds.get(TableName.META_TABLE_NAME);
    assertEquals(3, oldMetaTd.getRegionReplication());
    // force update the replica count to 1 and then kill the master, to simulate that hen upgrading,
    // we have no region replication in meta table descriptor but we actually have meta region
    // replicas
    oldTds.update(TableDescriptorBuilder.newBuilder(oldMetaTd).setRegionReplication(1).build());
    oldMaster.stop("Restarting");
    TEST_UTIL.waitFor(30000, () -> oldMaster.isStopped());

    // increase replica count to 5 through Configuration
    TEST_UTIL.getMiniHBaseCluster().getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 5);
    TEST_UTIL.getMiniHBaseCluster().startMaster();
    TEST_UTIL.waitFor(30000,
      () -> TEST_UTIL.getZooKeeperWatcher().getMetaReplicaNodes().size() == 5);
  }
}
