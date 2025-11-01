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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.keymeta.ManagedKeyTestBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.jruby.embed.ScriptingContainer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, IntegrationTests.class })
public class TestKeymetaMockProviderShell extends ManagedKeyTestBase implements RubyShellTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeymetaMockProviderShell.class);

  private final ScriptingContainer jruby = new ScriptingContainer();

  @Before
  @Override
  public void setUp() throws Exception {
    // Enable to be able to debug without timing out.
    // final Configuration conf = TEST_UTIL.getConfiguration();
    // conf.set("zookeeper.session.timeout", "6000000");
    // conf.set("hbase.rpc.timeout", "6000000");
    // conf.set("hbase.rpc.read.timeout", "6000000");
    // conf.set("hbase.rpc.write.timeout", "6000000");
    // conf.set("hbase.client.operation.timeout", "6000000");
    // conf.set("hbase.client.scanner.timeout.period", "6000000");
    // conf.set("hbase.ipc.client.socket.timeout.connect", "6000000");
    // conf.set("hbase.ipc.client.socket.timeout.read", "6000000");
    // conf.set("hbase.ipc.client.socket.timeout.write", "6000000");
    // conf.set("hbase.master.start.timeout.localHBaseCluster", "6000000");
    // conf.set("hbase.master.init.timeout.localHBaseCluster", "6000000");
    // conf.set("hbase.client.sync.wait.timeout.msec", "6000000");
    // conf.set("hbase.client.retries.number", "1000");
    RubyShellTest.setUpConfig(this);
    super.setUp();
    RubyShellTest.setUpJRubyRuntime(this);
    RubyShellTest.doTestSetup(this);
    jruby.put("$TEST", this);
  }

  @Override
  public HBaseTestingUtil getTEST_UTIL() {
    return TEST_UTIL;
  }

  @Override
  public ScriptingContainer getJRuby() {
    return jruby;
  }

  @Override
  public String getSuitePattern() {
    return "**/*_keymeta_mock_provider_test.rb";
  }

  @Test
  public void testRunShellTests() throws Exception {
    RubyShellTest.testRunShellTests(this);
  }
}
