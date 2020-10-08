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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.jruby.embed.PathType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, MediumTests.class })
public class TestShellNoCluster extends AbstractTestShell {
  private static final Logger LOG = LoggerFactory.getLogger(TestShellNoCluster.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestShellNoCluster.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // no cluster
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
    // no cluster
  }

  // Keep the same name so we override the with-a-cluster test
  @Override
  @Test
  public void testRunShellTests() throws IOException {
    LOG.info("Start ruby tests without cluster");
    jruby.runScriptlet(PathType.CLASSPATH, "no_cluster_tests_runner.rb");
  }
}
