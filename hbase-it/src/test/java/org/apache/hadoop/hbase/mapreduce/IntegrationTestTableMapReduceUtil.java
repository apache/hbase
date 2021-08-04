/**
 *
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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we add tmpjars correctly including the named dependencies. Runs
 * as an integration test so that classpath is realistic.
 */
@Category(IntegrationTests.class)
public class IntegrationTestTableMapReduceUtil implements Configurable, Tool {

  private static IntegrationTestingUtility util;

  @BeforeClass
  public static void provisionCluster() throws Exception {
    if (null == util) {
      util = new IntegrationTestingUtility();
    }
  }

  @Before
  public void skipMiniCluster() {
    // test probably also works with a local cluster, but
    // IntegrationTestingUtility doesn't support this concept.
    assumeTrue("test requires a distributed cluster.", util.isDistributedCluster());
  }

  /**
   * Look for jars we expect to be on the classpath by name.
   */
  @Test
  public void testAddDependencyJars() throws Exception {
    Job job = new Job();
    TableMapReduceUtil.addDependencyJars(job);
    String tmpjars = job.getConfiguration().get("tmpjars");

    // verify presence of modules
    assertTrue(tmpjars.contains("hbase-common"));
    assertTrue(tmpjars.contains("hbase-protocol"));
    assertTrue(tmpjars.contains("hbase-client"));
    assertTrue(tmpjars.contains("hbase-hadoop-compat"));
    assertTrue(tmpjars.contains("hbase-server"));

    // verify presence of 3rd party dependencies.
    assertTrue(tmpjars.contains("zookeeper"));
    assertTrue(tmpjars.contains("netty"));
    assertTrue(tmpjars.contains("protobuf"));
    assertTrue(tmpjars.contains("guava"));
    assertTrue(tmpjars.contains("htrace"));
  }

  @Override
  public int run(String[] args) throws Exception {
    provisionCluster();
    skipMiniCluster();
    testAddDependencyJars();
    return 0;
  }

  public void setConf(Configuration conf) {
    if (util != null) {
      throw new IllegalArgumentException(
          "setConf not supported after the test has been initialized.");
    }
    util = new IntegrationTestingUtility(conf);
  }

  @Override
  public Configuration getConf() {
    return util.getConfiguration();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestTableMapReduceUtil(), args);
    System.exit(status);
  }
}
