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

import java.util.Collections;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.fs.ErasureCodingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, LargeTests.class })
public class TestAdminShell extends AbstractTestShell {
  private static final Logger LOG = LoggerFactory.getLogger(TestAdminShell.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAdminShell.class);

  @Override
  public String getIncludeList() {
    return "admin_test.rb";
  }

  protected boolean erasureCodingSupported = false;

  @Override
  @Before
  public void setUp() throws Exception {
    RubyShellTest.setUpConfig(this);

    // Start mini cluster
    // 3 datanodes needed for erasure coding checks
    TEST_UTIL.startMiniCluster(3);
    try {
      ErasureCodingUtils.enablePolicy(FileSystem.get(TEST_UTIL.getConfiguration()),
        "XOR-2-1-1024k");
      erasureCodingSupported = true;
    } catch (UnsupportedOperationException e) {
      LOG.info(
        "Current hadoop version does not support erasure coding, only validation tests will run.");
    }

    // we'll use this extra variable to trigger some differences in the tests
    RubyShellTest.setUpJRubyRuntime(this,
      Collections.singletonMap("$ERASURE_CODING_SUPPORTED", erasureCodingSupported));

    RubyShellTest.doTestSetup(this);
  }
}
