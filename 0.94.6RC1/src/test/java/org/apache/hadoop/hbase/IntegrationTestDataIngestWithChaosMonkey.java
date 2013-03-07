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

package org.apache.hadoop.hbase;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A system test which does large data ingestion and verify using {@link LoadTestTool},
 * while killing the region servers and the master(s) randomly. You can configure how long
 * should the load test run by using "hbase.IntegrationTestDataIngestWithChaosMonkey.runtime"
 * configuration parameter.
 */
@Category(IntegrationTests.class)
public class IntegrationTestDataIngestWithChaosMonkey extends IngestIntegrationTestBase {

  private static final int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster

  // run for 5 min by default
  private static final long DEFAULT_RUN_TIME = 5 * 60 * 1000;

  private ChaosMonkey monkey;

  @Before
  public void setUp() throws Exception {
    super.setUp(NUM_SLAVES_BASE);
    monkey = new ChaosMonkey(util, ChaosMonkey.EVERY_MINUTE_RANDOM_ACTION_POLICY);
    monkey.start();
  }

  @After
  public void tearDown() throws Exception {
    if (monkey != null) {
      monkey.stop("test has finished, that's why");
      monkey.waitForStop();
    }
    super.tearDown();
  }

  @Test
  public void testDataIngest() throws Exception {
    runIngestTest(DEFAULT_RUN_TIME, 2500, 10, 100, 20);
  }
}
