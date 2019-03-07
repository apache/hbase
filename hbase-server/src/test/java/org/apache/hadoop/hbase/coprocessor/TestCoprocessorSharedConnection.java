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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.SharedConnection;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Ensure Coprocessors get ShardConnections when they get a Connection from their
 * CoprocessorEnvironment.
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestCoprocessorSharedConnection {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorSharedConnection.class);

  @Rule
  public TestName name = new TestName();
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  /**
   * Start up a mini cluster with my three CPs loaded.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    // Set my test Coprocessors into the Configuration before we start up the cluster.
    Configuration conf = HTU.getConfiguration();
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        TestMasterCoprocessor.class.getName());
    conf.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
        TestRegionServerCoprocessor.class.getName());
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TestRegionCoprocessor.class.getName());
    HTU.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  // Three test coprocessors, one of each type that has a Connection in its environment
  // (WALCoprocessor does not).
  public static class TestMasterCoprocessor implements MasterCoprocessor {
    public TestMasterCoprocessor() {
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      // At start, we get base CoprocessorEnvironment Type, not MasterCoprocessorEnvironment,
      checkShared(((MasterCoprocessorEnvironment) env).getConnection());
    }
  }

  public static class TestRegionServerCoprocessor implements RegionServerCoprocessor {
    public TestRegionServerCoprocessor() {
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      // At start, we get base CoprocessorEnvironment Type, not RegionServerCoprocessorEnvironment,
      checkShared(((RegionServerCoprocessorEnvironment) env).getConnection());
    }
  }

  public static class TestRegionCoprocessor implements RegionCoprocessor {
    public TestRegionCoprocessor() {
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      // At start, we get base CoprocessorEnvironment Type, not RegionCoprocessorEnvironment,
      checkShared(((RegionCoprocessorEnvironment) env).getConnection());
    }
  }

  private static void checkShared(Connection connection) {
    assertTrue(connection instanceof SharedConnection);
  }

  @Test
  public void test() throws IOException {
    // Nothing to do in here. The checks are done as part of the cluster spinup when CPs get
    // loaded. Need this here so this class looks like a test.
  }
}
