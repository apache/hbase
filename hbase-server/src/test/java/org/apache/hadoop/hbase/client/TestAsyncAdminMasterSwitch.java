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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Testcase for HBASE-22135.
 */
@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncAdminMasterSwitch extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncAdminMasterSwitch.class);

  @Test
  public void testSwitch() throws IOException, InterruptedException {
    assertEquals(TEST_UTIL.getHBaseCluster().getRegionServerThreads().size(),
      admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.SERVERS_NAME)).join()
        .getServersName().size());
    TEST_UTIL.getMiniHBaseCluster().stopMaster(0).join();
    assertTrue(TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(30000));
    // make sure that we could still call master
    assertEquals(TEST_UTIL.getHBaseCluster().getRegionServerThreads().size(),
      admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.SERVERS_NAME)).join()
        .getServersName().size());
  }
}
