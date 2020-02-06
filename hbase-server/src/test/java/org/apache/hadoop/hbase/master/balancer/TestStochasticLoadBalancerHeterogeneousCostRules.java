/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import static junit.framework.TestCase.assertTrue;

@Category({ MasterTests.class, MediumTests.class })
public class TestStochasticLoadBalancerHeterogeneousCostRules extends BalancerTestBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancerHeterogeneousCostRules.class);
  @Rule
  public TestName name = new TestName();

  static final String DEFAULT_RULES_FILE_NAME = "hbase-balancer.rules";
  private HeterogeneousRegionCountCostFunction costFunction;
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  /**
   * Make a file for rules that is inside a temporary test dir named for the method so it doesn't
   * clash w/ other rule files.
   */
  private String rulesFilename;

  @BeforeClass
  public static void beforeClass() throws IOException {
    // Ensure test dir is created
    HTU.getTestFileSystem().mkdirs(HTU.getDataTestDir());
  }

  @Before
  public void before() throws IOException {
    // New rules file name per test.
    this.rulesFilename = HTU.getDataTestDir(
      this.name.getMethodName() + "." + DEFAULT_RULES_FILE_NAME).toString();
    // Set the created rules filename into the configuration.
    HTU.getConfiguration().set(
      HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
      this.rulesFilename);
  }

  /**
   * @param file Name of file to write rules into.
   * @return Full file name of the rules file which is <code>dir</code> + DEFAULT_RULES_FILE_NAME.
   */
  static String createRulesFile(String file, final List<String> lines) throws IOException {
    cleanup(file);
    java.nio.file.Path path =
      java.nio.file.Files.createFile(FileSystems.getDefault().getPath(file));
    return java.nio.file.Files.write(path, lines, Charset.forName("UTF-8")).toString();
  }

  /**
   * @param file Name of file to write rules into.
   * @return Full file name of the rules file which is <code>dir</code> + DEFAULT_RULES_FILE_NAME.
   */
  static String createRulesFile(String file) throws IOException {
    return createRulesFile(file, Collections.emptyList());
  }

  private static void cleanup(String file) throws IOException {
    try {
      java.nio.file.Files.delete(FileSystems.getDefault().getPath(file));
    } catch (NoSuchFileException nsfe) {
      System.out.println("FileNotFoundException for " + file);
    }
  }

  @Test
  public void testNoRules() throws IOException {
    // Override what is in the configuration with the name of a non-existent file!
    HTU.getConfiguration().set(
      HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
      "non-existent-file!");
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadFormatInRules() throws IOException {
    // See {@link #before} above. It sets this.rulesFilename, and
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());

    createRulesFile(this.rulesFilename, Collections.singletonList("bad rules format"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());

    createRulesFile(this.rulesFilename, Arrays.asList("srv[1-2] 10",
      "bad_rules format", "a"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    Assert.assertEquals(1, this.costFunction.getNumberOfRulesLoaded());
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
    Assert.assertEquals(2, this.costFunction.getNumberOfRulesLoaded());
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
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testNoOverride() throws IOException {
    // See {@link #before} above. It sets this.rulesFilename, and
    // HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
    // in the configuration.
    createRulesFile(this.rulesFilename, Arrays.asList("^server1$ 10", "^server2 21"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(HTU.getConfiguration());
    this.costFunction.loadRules();
    Assert.assertEquals(2, this.costFunction.getNumberOfRulesLoaded());

    // loading malformed configuration does not overload current
    cleanup(this.rulesFilename);
    this.costFunction.loadRules();
    Assert.assertEquals(2, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testLoadingFomHDFS() throws Exception {
    HTU.startMiniDFSCluster(3);
    try {
      MiniDFSCluster cluster = HTU.getDFSCluster();
      DistributedFileSystem fs = cluster.getFileSystem();
      // Writing file
      Path path = new Path(fs.getHomeDirectory(), DEFAULT_RULES_FILE_NAME);
      FSDataOutputStream stream = fs.create(path);
      stream.write("server1 10".getBytes());
      stream.flush();
      stream.close();

      Configuration configuration = HTU.getConfiguration();

      // start costFunction
      configuration.set(
        HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
        path.toString());
      this.costFunction = new HeterogeneousRegionCountCostFunction(configuration);
      this.costFunction.loadRules();
      Assert.assertEquals(1, this.costFunction.getNumberOfRulesLoaded());
    } finally {
      HTU.shutdownMiniCluster();
    }
  }
}
