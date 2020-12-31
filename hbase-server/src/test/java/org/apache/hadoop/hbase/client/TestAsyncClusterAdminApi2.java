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

import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Only used to test stopMaster/stopRegionServer/shutdown methods.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncClusterAdminApi2 extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncClusterAdminApi2.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // do nothing
  }

  @Before
  @Override
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    admin = ASYNC_CONN.getAdmin();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStop() throws Exception {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    assertFalse(rs.isStopped());
    admin.stopRegionServer(rs.getServerName()).join();
    assertTrue(rs.isStopped());

    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    assertFalse(master.isStopped());
    admin.stopMaster().join();
    assertTrue(master.isStopped());
  }

  @Test
  public void testShutdown() throws Exception {
    TEST_UTIL.getMiniHBaseCluster().getMasterThreads().forEach(thread -> {
      assertFalse(thread.getMaster().isStopped());
    });
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      assertFalse(thread.getRegionServer().isStopped());
    });

    admin.shutdown().join();
    TEST_UTIL.getMiniHBaseCluster().getMasterThreads().forEach(thread -> {
      while (!thread.getMaster().isStopped()) {
        trySleep(100, TimeUnit.MILLISECONDS);
      }
    });
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      while (!thread.getRegionServer().isStopped()) {
        trySleep(100, TimeUnit.MILLISECONDS);
      }
    });
  }

  private void trySleep(long timeout, TimeUnit unit) {
    try {
      unit.sleep(timeout);
    } catch (InterruptedException e) {
    }
  }
}
