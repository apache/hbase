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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminEndpoint;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.jruby.embed.PathType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, LargeTests.class })
public class TestRSGroupShell extends AbstractTestShell {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupShell.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpConfig();

    // enable rs group
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      TEST_UTIL.getConfiguration().get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY) + "," +
        RSGroupAdminEndpoint.class.getName());
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());

    TEST_UTIL.startMiniCluster(3);

    setUpJRubyRuntime();
  }

  @Test
  public void testRunShellTests() throws IOException {
    System.setProperty("shell.test.include", "rsgroup_shell_test.rb");
    // Start all ruby tests
    jruby.runScriptlet(PathType.ABSOLUTE, "src/test/ruby/tests_runner.rb");
  }
}
