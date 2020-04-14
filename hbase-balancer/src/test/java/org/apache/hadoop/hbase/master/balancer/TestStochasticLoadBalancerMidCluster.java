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
package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerMidCluster extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancerMidCluster.class);

  @Test
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int replication = 1;
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int replication = 1;
    int numTables = 400;
    // num large num regions means may not always get to best balance with one run
    boolean assertFullyBalanced = false;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables,
      assertFullyBalanced, false);
  }

  @Test
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int replication = 1;
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }
}
