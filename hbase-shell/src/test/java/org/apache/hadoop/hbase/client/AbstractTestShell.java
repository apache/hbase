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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestShell {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestShell.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected final static ScriptingContainer jruby = new ScriptingContainer();

  protected static void setUpConfig() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setBoolean("hbase.quota.enabled", true);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    conf.setInt("hfile.format.version", 3);

    // Below settings are necessary for task monitor test.
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);
    conf.setBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO, true);
    // Security setup configuration
    SecureTestUtil.enableSecurity(conf);
    VisibilityTestUtil.enableVisiblityLabels(conf);
  }

  protected static void setUpJRubyRuntime() {
    LOG.debug("Configure jruby runtime, cluster set to {}", TEST_UTIL);
    List<String> loadPaths = new ArrayList<>(2);
    loadPaths.add("src/test/ruby");
    jruby.setLoadPaths(loadPaths);
    jruby.put("$TEST_CLUSTER", TEST_UTIL);
    System.setProperty("jruby.jit.logging.verbose", "true");
    System.setProperty("jruby.jit.logging", "true");
    System.setProperty("jruby.native.verbose", "true");
  }

  /**
   * @return comma separated list of ruby script names for tests
   */
  protected String getIncludeList() {
    return "";
  }

  /**
   * @return comma separated list of ruby script names for tests to skip
   */
  protected String getExcludeList() {
    return "";
  }

  @Test
  public void testRunShellTests() throws IOException {
    final String tests = getIncludeList();
    final String excludes = getExcludeList();
    if (!tests.isEmpty()) {
      System.setProperty("shell.test.include", tests);
    }
    if (!excludes.isEmpty()) {
      System.setProperty("shell.test.exclude", excludes);
    }
    LOG.info("Starting ruby tests. includes: {} excludes: {}", tests, excludes);
    jruby.runScriptlet(PathType.ABSOLUTE, "src/test/ruby/tests_runner.rb");
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpConfig();

    // Start mini cluster
    TEST_UTIL.startMiniCluster(1);

    setUpJRubyRuntime();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
