/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.compaction.TestMajorCompactorTTL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

public class TestRSGroupMajorCompactionTTL extends TestMajorCompactorTTL {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupMajorCompactionTTL.class);

  private final static int NUM_SLAVES_BASE = 6;

  @Before
  @Override
  public void setUp() throws Exception {
    utility = new HBaseTestingUtility();
    Configuration conf = utility.getConfiguration();
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_SLAVES_BASE);
    conf.setInt("hbase.hfile.compaction.discharger.interval", 10);
    utility.startMiniCluster(NUM_SLAVES_BASE);
    MiniHBaseCluster cluster = utility.getHBaseCluster();
    final HMaster master = cluster.getMaster();

    //wait for balancer to come online
    utility.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() {
        return master.isInitialized() &&
            ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline();
      }
    });
    admin = utility.getAdmin();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Test
  public void testCompactingTables() throws Exception {
    List<TableName> tableNames = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      tableNames.add(createTable(name.getMethodName() + "___" + i));
    }

    // Delay a bit, so we can set the table TTL to 5 seconds
    Thread.sleep(10 * 1000);

    for (TableName tableName : tableNames) {
      int numberOfRegions = admin.getRegions(tableName).size();
      int numHFiles = utility.getNumHFiles(tableName, FAMILY);
      modifyTTL(tableName);
    }

    RSGroupMajorCompactionTTL compactor = new RSGroupMajorCompactionTTL();
    compactor.compactTTLRegionsOnGroup(utility.getConfiguration(),
        RSGroupInfo.DEFAULT_GROUP, 1, 200, -1, -1, false, false);

    for (TableName tableName : tableNames) {
      int numberOfRegions = admin.getRegions(tableName).size();
      int numHFiles = utility.getNumHFiles(tableName, FAMILY);
      assertEquals(numberOfRegions, numHFiles);
    }
  }
}
