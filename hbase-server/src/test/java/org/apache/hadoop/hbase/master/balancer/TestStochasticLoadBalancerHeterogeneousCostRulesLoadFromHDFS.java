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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestStochasticLoadBalancerHeterogeneousCostRulesLoadFromHDFS
  extends StochasticBalancerTestBase {

  private HeterogeneousRegionCountCostFunction costFunction;
  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  @BeforeEach
  public void setUp() throws Exception {
    HTU.startMiniCluster(1);
  }

  @AfterEach
  public void tearDown() throws IOException {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testLoadingFomHDFS() throws Exception {
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
    assertEquals(1, this.costFunction.getNumberOfRulesLoaded());
  }
}
