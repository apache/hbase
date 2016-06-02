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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.jruby.embed.ScriptingContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractTestShell {

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected final static ScriptingContainer jruby = new ScriptingContainer();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start mini cluster
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);

    // Below settings are necessary for task monitor test.
    TEST_UTIL.getConfiguration().setInt(HConstants.MASTER_INFO_PORT, 0);
    TEST_UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_INFO_PORT, 0);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO, true);
    // Security setup configuration
    SecureTestUtil.enableSecurity(TEST_UTIL.getConfiguration());
    VisibilityTestUtil.enableVisiblityLabels(TEST_UTIL.getConfiguration());

    TEST_UTIL.startMiniCluster();

    // Configure jruby runtime
    List<String> loadPaths = new ArrayList();
    loadPaths.add("src/main/ruby");
    loadPaths.add("src/test/ruby");
    jruby.getProvider().setLoadPaths(loadPaths);
    jruby.put("$TEST_CLUSTER", TEST_UTIL);
    System.setProperty("jruby.jit.logging.verbose", "true");
    System.setProperty("jruby.jit.logging", "true");
    System.setProperty("jruby.native.verbose", "true");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
