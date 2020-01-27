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

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.regionserver.NoOpScanPolicyObserver;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

/**
 * Test all client operations with a coprocessor that just implements the default flush/compact/scan
 * policy.
 */
@Category({ LargeTests.class, ClientTests.class })
public class TestFromClientSideWithCoprocessor extends TestFromClientSide {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFromClientSideWithCoprocessor.class);

  // Override the parameters from the parent class. We just want to run it for the default
  // param combination.
  @Parameterized.Parameters
  public static Collection parameters() {
    return Arrays.asList(new Object[][] {
        { MasterRegistry.class, 1},
        { ZKConnectionRegistry.class, 1}
    });
  }

  public TestFromClientSideWithCoprocessor(Class registry, int numHedgedReqs) throws Exception {
    if (TEST_UTIL == null) {
      // It is ok to initialize once because the test is parameterized for a single dimension.
      initialize(registry, numHedgedReqs, NoOpScanPolicyObserver.class,
          MultiRowMutationEndpoint.class);
    }
  }
}
