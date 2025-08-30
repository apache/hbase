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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface RubyShellTest {
  static Logger LOG = LoggerFactory.getLogger(RubyShellTest.class);

  HBaseTestingUtil getTEST_UTIL();
  ScriptingContainer getJRuby();

  /** Returns comma separated list of ruby script names for tests */
  default String getIncludeList() {
    return "";
  }

  /** Returns comma separated list of ruby script names for tests to skip */
  default String getExcludeList() {
    return "";
  }

  String getSuitePattern();

  static void setUpConfig(RubyShellTest test) throws IOException {
    Configuration conf = test.getTEST_UTIL().getConfiguration();
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

  static void setUpJRubyRuntime(RubyShellTest test) {
    LOG.debug("Configure jruby runtime, cluster set to {}", test.getTEST_UTIL());
    List<String> loadPaths = new ArrayList<>(2);
    loadPaths.add("src/test/ruby");
    test.getJRuby().setLoadPaths(loadPaths);
    test.getJRuby().put("$TEST_CLUSTER", test.getTEST_UTIL());
    System.setProperty("jruby.jit.logging.verbose", "true");
    System.setProperty("jruby.jit.logging", "true");
    System.setProperty("jruby.native.verbose", "true");
  }

  static void doTestSetup(RubyShellTest test) {
    System.setProperty("shell.test.suite_name", test.getClass().getSimpleName());
    System.setProperty("shell.test.suite_pattern", test.getSuitePattern());
    if (!test.getIncludeList().isEmpty()) {
      System.setProperty("shell.test.include", test.getIncludeList());
    }
    if (!test.getExcludeList().isEmpty()) {
      System.setProperty("shell.test.exclude", test.getExcludeList());
    }
    LOG.info("Starting ruby tests on script: {} includes: {} excludes: {}",
      test.getClass().getSimpleName(), test.getIncludeList(), test.getExcludeList());
  }

  static void testRunShellTests(RubyShellTest test) throws IOException {
    test.getJRuby().runScriptlet(PathType.ABSOLUTE, "src/test/ruby/tests_runner.rb");
  }
}