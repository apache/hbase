/**
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestStochasticLoadBalancerHeterogeneousCostRules extends BalancerTestBase {

  static final String DEFAULT_RULES_TMP_LOCATION = "/tmp/hbase-balancer.rules";
  static Configuration conf;
  private HeterogeneousRegionCountCostFunction costFunction;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    createSimpleRulesFile(new ArrayList<String>());
    conf = new Configuration();
    conf.set(HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
      DEFAULT_RULES_TMP_LOCATION);
  }

  static void createSimpleRulesFile(final List<String> lines) throws IOException {
    cleanup();
    final Path file = Paths.get(DEFAULT_RULES_TMP_LOCATION);
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  protected static void cleanup() {
    final File file = new File(DEFAULT_RULES_TMP_LOCATION);
    file.delete();
  }

  @AfterClass
  public static void afterAllTests() {
    cleanup();
  }

  @Test
  public void testNoRules() {
    cleanup();
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadFormatInRules() throws IOException {
    createSimpleRulesFile(new ArrayList<String>());
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());

    createSimpleRulesFile(Collections.singletonList("bad rules format"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());

    createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "bad_rules format", "a"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(1, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testTwoRules() throws IOException {
    createSimpleRulesFile(Arrays.asList("^server1$ 10", "^server2 21"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(2, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadRegexp() throws IOException {
    createSimpleRulesFile(Collections.singletonList("server[ 1"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(0, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testNoOverride() throws IOException {
    createSimpleRulesFile(Arrays.asList("^server1$ 10", "^server2 21"));
    this.costFunction = new HeterogeneousRegionCountCostFunction(conf);
    this.costFunction.loadRules();
    Assert.assertEquals(2, this.costFunction.getNumberOfRulesLoaded());

    // loading malformed configuration does not overload current
    cleanup();
    this.costFunction.loadRules();
    Assert.assertEquals(2, this.costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testLoadingFomHDFS() throws Exception {

    HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniDFSCluster(3);

    MiniDFSCluster cluster = hBaseTestingUtility.getDFSCluster();
    DistributedFileSystem fs = cluster.getFileSystem();

    String path = cluster.getURI() + DEFAULT_RULES_TMP_LOCATION;

    // writing file
    FSDataOutputStream stream = fs.create(new org.apache.hadoop.fs.Path(path));
    stream.write("server1 10".getBytes());
    stream.flush();
    stream.close();

    Configuration configuration = hBaseTestingUtility.getConfiguration();

    // start costFunction
    configuration.set(
      HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE, path);
    this.costFunction = new HeterogeneousRegionCountCostFunction(configuration);
    this.costFunction.loadRules();
    Assert.assertEquals(1, this.costFunction.getNumberOfRulesLoaded());

    hBaseTestingUtility.shutdownMiniCluster();
  }
}
