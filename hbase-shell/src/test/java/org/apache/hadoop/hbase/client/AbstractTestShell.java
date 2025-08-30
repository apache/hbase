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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jruby.embed.ScriptingContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractTestShell implements RubyShellTest {
  protected final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected final ScriptingContainer jruby = new ScriptingContainer();

  public HBaseTestingUtil getTEST_UTIL() {
    return TEST_UTIL;
  }

  public ScriptingContainer getJRuby() {
    return jruby;
  }

  public String getSuitePattern() {
    return "**/*_test.rb";
  }

  @Before
  public void setUp() throws Exception {
    RubyShellTest.setUpConfig(this);

    // Start mini cluster
    // 3 datanodes needed for erasure coding checks
    TEST_UTIL.startMiniCluster(3);

    RubyShellTest.setUpJRubyRuntime(this);

    RubyShellTest.doTestSetup(this);
  }

  protected void setupDFS() throws IOException {
    DistributedFileSystem dfs =
      (DistributedFileSystem) FileSystem.get(TEST_UTIL.getConfiguration());
    dfs.enableErasureCodingPolicy("XOR-2-1-1024k");
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRunShellTests() throws IOException {
    RubyShellTest.testRunShellTests(this);
  }
}
