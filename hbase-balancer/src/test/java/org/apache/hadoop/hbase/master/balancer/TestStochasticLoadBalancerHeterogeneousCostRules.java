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

import static org.apache.hadoop.hbase.master.balancer.HeterogeneousCostRulesTestHelper.DEFAULT_RULES_FILE_NAME;
import static org.apache.hadoop.hbase.master.balancer.HeterogeneousCostRulesTestHelper.cleanup;
import static org.apache.hadoop.hbase.master.balancer.HeterogeneousCostRulesTestHelper.createRulesFile;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MasterTests.class, MediumTests.class })
public class TestStochasticLoadBalancerHeterogeneousCostRules extends StochasticBalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancerHeterogeneousCostRules.class);
  @Rule
  public TestName name = new TestName();

  private HeterogeneousRegionCountCostFunction costFunction;
  private static final HBaseCommonTestingUtil HTU = new HBaseCommonTestingUtil();

  /**
   * Make a file for rules that is inside a temporary test dir named for the method so it doesn't
   * clash w/ other rule files.
   */
  private String rulesFilename;

  @BeforeClass
  public static void beforeClass() throws IOException {
    // Ensure test dir is created
    HTU.getDataTestDir().getFileSystem(HTU.getConfiguration()).mkdirs(HTU.getDataTestDir());
  }

  @Before
  public void before() throws IOException {
    // New rules file name per test.
    this.rulesFilename = HTU
      .getDataTestDir(
        this.name.getMethodName() + "." + DEFAULT_RULES_FILE_NAME)
      .toString();
    // Set the created rules filename into the configuration.
    HTU.getConfiguration().set(
      HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
      this.rulesFilename);
  }

  @Test
  public void testNoRules() throws IOException {
    // Override what is in the configuration with the name of a non-existent file!
    HTU.getConfiguration().set(
      HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
      "non-existent-file!");
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(0, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadFormatInRules() throws IOException {
    // See {@link #before} above. It sets this.rulesFilename, and
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(0, this.costFunction.getNumberOfRulesLoaded());

    createRulesFile(this.rulesFilename, Collections.singletonList("bad rules format"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(0, this.costFunction.getNumberOfRulesLoaded());

    createRulesFile(this.rulesFilename, Arrays.asList("srv[1-2] 10",
      "bad_rules format", "a"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(1, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testTwoRules() throws IOException {
    // See {@link #before} above. It sets this.rulesFilename, and
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    // See {@link #before} above. It sets
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    createRulesFile(this.rulesFilename, Arrays.asList("^server1$ 10", "^server2 21"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(2, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadRegexp() throws IOException {
    // See {@link #before} above. It sets this.rulesFilename, and
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    // See {@link #before} above. It sets
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    createRulesFile(this.rulesFilename, Collections.singletonList("server[ 1"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(0, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testNoOverride() throws IOException {
    // See {@link #before} above. It sets this.rulesFilename, and
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    createRulesFile(this.rulesFilename, Arrays.asList("^server1$ 10", "^server2 21"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    assertEquals(2, this.costFunction.getNumberOfRulesLoaded());

    // loading malformed configuration does not overload current
    cleanup(this.rulesFilename);
    this.costFunction.loadRules();
    assertEquals(2, this.costFunction.getNumberOfRulesLoaded());
  }
}