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
package org.apache.hadoop.hbase.client.rsgroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminEndpoint;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Separate Shell test class for Groups
//Since we need to use a different balancer and run more than 1 RS
@Category({ClientTests.class, LargeTests.class})
public class TestShellRSGroups {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestShellRSGroups.class);

  final Logger LOG = LoggerFactory.getLogger(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static ScriptingContainer jruby = new ScriptingContainer();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    // Start mini cluster
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    // Security setup configuration
    SecureTestUtil.enableSecurity(TEST_UTIL.getConfiguration());
    VisibilityTestUtil.enableVisiblityLabels(TEST_UTIL.getConfiguration());

    //Setup RegionServer Groups
    TEST_UTIL.getConfiguration().set(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(
        CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        RSGroupAdminEndpoint.class.getName());

    TEST_UTIL.startMiniCluster(4);

    // Configure jruby runtime
    List<String> loadPaths = new ArrayList<>(2);
    loadPaths.add("src/test/ruby");
    jruby.setLoadPaths(loadPaths);
    jruby.put("$TEST_CLUSTER", TEST_UTIL);
    System.setProperty("jruby.jit.logging.verbose", "true");
    System.setProperty("jruby.jit.logging", "true");
    System.setProperty("jruby.native.verbose", "true");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRunShellTests() throws IOException {
    try {
      // Start only GroupShellTest
      System.setProperty("shell.test", "Hbase::RSGroupShellTest");
      jruby.runScriptlet(PathType.ABSOLUTE, "src/test/ruby/tests_runner.rb");
    } finally {
      System.clearProperty("shell.test");
    }
  }
}
