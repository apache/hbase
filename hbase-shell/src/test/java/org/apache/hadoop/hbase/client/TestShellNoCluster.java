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
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class })
public class TestShellNoCluster extends AbstractTestShell {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestShellNoCluster.class);

  @Override
  @Before
  public void setUp() throws Exception {
    RubyShellTest.setUpConfig(this);

    // no cluster

    RubyShellTest.setUpJRubyRuntime(this);

    RubyShellTest.doTestSetup(this);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    // no cluster
  }

  @Override
  public String getSuitePattern() {
    return "**/*_test_no_cluster.rb";
  }
}
