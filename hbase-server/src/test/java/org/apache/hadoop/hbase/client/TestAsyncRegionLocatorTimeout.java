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

import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncRegionLocatorTimeout {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static AsyncConnectionImpl CONN;

  private static AsyncRegionLocator LOCATOR;

  private static volatile long SLEEP_MS = 0L;

  public static class SleepRegionObserver extends BaseRegionObserver {

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
        RegionScanner s) throws IOException {
      if (SLEEP_MS > 0) {
        Threads.sleepWithoutInterrupt(SLEEP_MS);
      }
      return super.preScannerOpen(e, scan, s);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(REGION_COPROCESSOR_CONF_KEY, SleepRegionObserver.class.getName());
    conf.setLong(HBASE_CLIENT_META_OPERATION_TIMEOUT, 2000);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    CONN = new AsyncConnectionImpl(conf, User.getCurrent());
    LOCATOR = CONN.getLocator();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    SLEEP_MS = 1000;
    long startNs = System.nanoTime();
    try {
      LOCATOR.getRegionLocation(TABLE_NAME, EMPTY_START_ROW, TimeUnit.MILLISECONDS.toNanos(500))
          .get();
      fail();
    } catch (ExecutionException e) {
      e.printStackTrace();
      assertThat(e.getCause(), instanceOf(TimeoutIOException.class));
    }
    long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    assertTrue(costMs >= 500);
    assertTrue(costMs < 1000);
    // wait for the background task finish
    Thread.sleep(2000);
    // Now the location should be in cache, so we will not visit meta again.
    HRegionLocation loc = LOCATOR
        .getRegionLocation(TABLE_NAME, EMPTY_START_ROW, TimeUnit.MILLISECONDS.toNanos(500)).get();
    assertEquals(loc.getServerName(),
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName());
  }
}
