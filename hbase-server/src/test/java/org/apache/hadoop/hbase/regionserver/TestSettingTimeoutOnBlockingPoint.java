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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class})
public class TestSettingTimeoutOnBlockingPoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSettingTimeoutOnBlockingPoint.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAM = Bytes.toBytes("f");
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    // simulate queue blocking
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 2);
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void setUpAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class SleepCoprocessor implements RegionCoprocessor, RegionObserver {
    public static final int SLEEP_TIME = 10000;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Result preIncrementAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
      Threads.sleep(SLEEP_TIME);
      return null;
    }
  }

  @Test
  public void testRowLock() throws IOException {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor(tableName);
    hdt.addCoprocessor(SleepCoprocessor.class.getName());
    TEST_UTIL.createTable(hdt, new byte[][]{FAM}, TEST_UTIL.getConfiguration());

    Thread incrementThread = new Thread(() -> {
      try {
        try( Table table = TEST_UTIL.getConnection().getTable(tableName)) {
          table.incrementColumnValue(ROW1, FAM, FAM, 1);
        }
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    });
    Thread getThread = new Thread(() -> {
      try {
        try( Table table = TEST_UTIL.getConnection().getTable(tableName)) {
          table.setRpcTimeout(1000);
          Delete delete = new Delete(ROW1);
          table.delete(delete);
        }
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    });

    incrementThread.start();
    Threads.sleep(1000);
    getThread.start();
    Threads.sleep(2000);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      // We have only two handlers. The first thread will get a write lock for row1 and occupy
      // the first handler. The second thread need a read lock for row1, it should quit after 1000
      // ms and give back the handler because it can not get the lock in time.
      // So we can get the value using the second handler.
      table.setRpcTimeout(1000);
      table.get(new Get(ROW2)); // Will throw exception if the timeout checking is failed
    } finally {
      incrementThread.interrupt();
      getThread.interrupt();
    }
  }
}
