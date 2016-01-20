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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.regionserver.NoOpScanPolicyObserver;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Before;
import org.junit.experimental.categories.Category;

/**
 * Test all {@link Increment} client operations with a coprocessor that
 * just implements the default flush/compact/scan policy.
 *
 * This test takes a long time. The test it derives from is parameterized so we run through both
 * options of the test.
 */
@Category(LargeTests.class)
public class TestIncrementFromClientSideWithCoprocessor extends TestIncrementsFromClientSide {
  public TestIncrementFromClientSideWithCoprocessor(final boolean fast) {
    super(fast);
  }

  @Before
  public void before() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MultiRowMutationEndpoint.class.getName(), NoOpScanPolicyObserver.class.getName());
    conf.setBoolean("hbase.table.sanity.checks", true); // enable for below tests
    super.before();
  }
}