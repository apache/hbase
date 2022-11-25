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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, MediumTests.class })
public class TestConnectionImplementation {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnectionImplementation.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestConnectionImplementation.class);

  private static final IntegrationTestingUtility TEST_UTIL = new IntegrationTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetMasterTTLCache() throws IOException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();

    // Test with hbase.client.master.state.cache.timeout.sec = 0 sec
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 0L);
    ConnectionImplementation conn =
      new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    long startTime = System.currentTimeMillis();
    conn.getMaster();
    long endTime = System.currentTimeMillis();
    long totalMillisWithoutCache = endTime - startTime;

    LOG.info("TotalMillisWithoutCache:{} ms", totalMillisWithoutCache);


    // Test with hbase.client.master.state.cache.timeout.sec = 15 Sec
    conf.setLong(ConnectionImplementation.MASTER_STATE_CACHE_TIMEOUT_SEC, 15L);
    conn = new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent());
    conn.getMaster();

    startTime = System.currentTimeMillis();
    conn.getMaster();
    endTime = System.currentTimeMillis();
    long totalMillisWithCache = endTime - startTime;

    LOG.info("totalMillisWithCache:{} ms", totalMillisWithCache);

    Thread.sleep(20000);
    startTime = System.currentTimeMillis();
    conn.getMaster();
    endTime = System.currentTimeMillis();
    long totalMillisAfterCacheExpiry = endTime - startTime;
    LOG.info("totalMillisAfterCacheExpiry:{} ms", totalMillisAfterCacheExpiry);

    // Verify that retrieval from cache is faster than retrieval without cache.
    assert totalMillisWithCache < totalMillisWithoutCache;

    // Verify that retrieval from cache is faster than retrieval after expiry.
    assert totalMillisWithCache < totalMillisAfterCacheExpiry;
  }
}
