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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

/**
 * Testcase for HBASE-22135.
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: policy = {0}")
public class TestAsyncAdminMasterSwitch extends TestAsyncAdminBase {

  public TestAsyncAdminMasterSwitch(Supplier<AsyncAdmin> admin) {
    super(admin);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TestAsyncAdminBase.setUpBeforeClass();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TestAsyncAdminBase.tearDownAfterClass();
  }

  @TestTemplate
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
