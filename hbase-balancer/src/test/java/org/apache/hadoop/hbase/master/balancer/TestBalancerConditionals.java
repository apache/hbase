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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestBalancerConditionals extends BalancerTestBase {

  private BalancerConditionals balancerConditionals;
  private BalancerClusterState mockCluster;

  @BeforeEach
  public void setUp() {
    balancerConditionals = BalancerConditionals.create();
    mockCluster = mockCluster(new int[] { 0, 1, 2 });
  }

  @Test
  public void testDefaultConfiguration() {
    Configuration conf = new Configuration();
    balancerConditionals.setConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertEquals(0, balancerConditionals.getConditionalClasses().size(),
      "No conditionals should be loaded by default");
  }

  @Test
  public void testCustomConditionalsViaConfiguration() {
    Configuration conf = new Configuration();
    conf.set(BalancerConditionals.ADDITIONAL_CONDITIONALS_KEY,
      DistributeReplicasConditional.class.getName());

    balancerConditionals.setConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertTrue(balancerConditionals.isConditionalBalancingEnabled(),
      "Custom conditionals should be loaded");
  }

  @Test
  public void testInvalidCustomConditionalClass() {
    Configuration conf = new Configuration();
    conf.set(BalancerConditionals.ADDITIONAL_CONDITIONALS_KEY, "java.lang.String");

    balancerConditionals.setConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertEquals(0, balancerConditionals.getConditionalClasses().size(),
      "Invalid classes should not be loaded as conditionals");
  }

  @Test
  public void testMetaTableIsolationConditionalEnabled() {
    Configuration conf = new Configuration();
    conf.setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);

    balancerConditionals.setConf(conf);
    balancerConditionals.loadClusterState(mockCluster);

    assertTrue(balancerConditionals.isTableIsolationEnabled(),
      "MetaTableIsolationConditional should be active");
  }
}
