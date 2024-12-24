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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBalancerConditionals extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBalancerConditionals.class);

  private static final ServerName SERVER_1 = ServerName.valueOf("server1", 12345, 1);
  private static final ServerName SERVER_2 = ServerName.valueOf("server2", 12345, 1);

  private BalancerConditionals balancerConditionals;
  private BalancerClusterState mockCluster;

  @Before
  public void setUp() {
    balancerConditionals = BalancerConditionals.INSTANCE;
    mockCluster = mockCluster(new int[] { 0, 1, 2 });
  }

  @Test
  public void testDefaultConfiguration() {
    Configuration conf = new Configuration();
    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertEquals("No conditionals should be loaded by default", 0,
      balancerConditionals.getConditionalClasses().size());
  }

  @Test
  public void testSystemTableIsolationConditionalEnabled() {
    Configuration conf = new Configuration();
    conf.setBoolean(BalancerConditionals.ISOLATE_SYSTEM_TABLES_KEY, true);

    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertTrue("SystemTableIsolationConditional should be active",
      balancerConditionals.shouldSkipSloppyServerEvaluation());
  }

  @Test
  public void testMetaTableIsolationConditionalEnabled() {
    Configuration conf = new Configuration();
    conf.setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);

    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertTrue("MetaTableIsolationConditional should be active",
      balancerConditionals.shouldSkipSloppyServerEvaluation());
  }

  @Test
  public void testCustomConditionalsViaConfiguration() {
    Configuration conf = new Configuration();
    conf.set(BalancerConditionals.ADDITIONAL_CONDITIONALS_KEY,
      MetaTableIsolationConditional.class.getName());

    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertTrue("Custom conditionals should be loaded",
      balancerConditionals.shouldSkipSloppyServerEvaluation());
  }

  @Test
  public void testInvalidCustomConditionalClass() {
    Configuration conf = new Configuration();
    conf.set(BalancerConditionals.ADDITIONAL_CONDITIONALS_KEY, "java.lang.String");

    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertEquals("Invalid classes should not be loaded as conditionals", 0,
      balancerConditionals.getConditionalClasses().size());
  }

  @Test
  public void testNoViolationsWithoutConditionals() {
    Configuration conf = new Configuration();
    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf("test")).build();
    RegionPlan regionPlan = new RegionPlan(regionInfo, SERVER_1, SERVER_2);

    int violations = balancerConditionals.getConditionalViolationChange(List.of(regionPlan));

    assertEquals("No conditionals should result in zero violations", 0, violations);
  }

  @Test
  public void testShouldSkipSloppyServerEvaluationWithMixedConditionals() {
    Configuration conf = new Configuration();
    conf.setBoolean(BalancerConditionals.ISOLATE_SYSTEM_TABLES_KEY, true);
    conf.setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);

    balancerConditionals.loadConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertTrue("Sloppy server evaluation should be skipped with relevant conditionals",
      balancerConditionals.shouldSkipSloppyServerEvaluation());
  }
}
