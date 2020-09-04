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

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.regionserver.NoOpScanPolicyObserver;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

/**
 * Test all client operations with a coprocessor that
 * just implements the default flush/compact/scan policy.
 */
@Category(LargeTests.class)
public class TestFromClientSideWithCoprocessor extends TestFromClientSide {

  @Parameterized.Parameters
  public static Collection parameters() {
    return Arrays.asList(new Object[][] {
        { ZKConnectionRegistry.class }
    });
  }

  public TestFromClientSideWithCoprocessor(Class registry) throws Exception {
    initialize(registry);
  }

  public static void initialize(Class<? extends ConnectionRegistry> registry) throws Exception {
    if (isSameParameterizedCluster(registry)) {
      return;
    }
    if (TEST_UTIL != null) {
      // We reached end of a parameterized run, clean up.
      TEST_UTIL.shutdownMiniCluster();
    }
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MultiRowMutationEndpoint.class.getName(), NoOpScanPolicyObserver.class.getName());
    conf.setBoolean("hbase.table.sanity.checks", true); // enable for below tests
    conf.setClass(HConstants.REGISTRY_IMPL_CONF_KEY, registry, ConnectionRegistry.class);
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster(SLAVES);
  }
}