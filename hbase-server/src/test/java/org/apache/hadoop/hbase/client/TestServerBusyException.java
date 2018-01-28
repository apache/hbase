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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.ServerTooBusyException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class is for testing HBaseConnectionManager ServerBusyException.
 * Be careful adding to this class. It sets a low
 * HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD
 */
@Category({LargeTests.class})
public class TestServerBusyException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerBusyException.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");
  private static final int RPC_RETRY = 5;

  @Rule
  public TestName name = new TestName();

  public static class SleepCoprocessor implements RegionCoprocessor, RegionObserver {
    public static final int SLEEP_TIME = 5000;
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Get get, final List<Cell> results) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Put put, final WALEdit edit, final Durability durability) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
                               final Increment increment) throws IOException {
      Threads.sleep(SLEEP_TIME);
      return null;
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete,
        final WALEdit edit, final Durability durability) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }

  }

  public static class SleepLongerAtFirstCoprocessor implements RegionCoprocessor, RegionObserver {
    public static final int SLEEP_TIME = 2000;
    static final AtomicLong ct = new AtomicLong(0);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Get get, final List<Cell> results) throws IOException {
      // After first sleep, all requests are timeout except the last retry. If we handle
      // all the following requests, finally the last request is also timeout. If we drop all
      // timeout requests, we can handle the last request immediately and it will not timeout.
      if (ct.incrementAndGet() <= 1) {
        Threads.sleep(SLEEP_TIME * RPC_RETRY * 2);
      } else {
        Threads.sleep(SLEEP_TIME);
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    // Up the handlers; this test needs more than usual.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, RPC_RETRY);
    // simulate queue blocking in testDropTimeoutRequest
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 1);
    // Needed by the server busy test.
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD, 3);
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static class TestPutThread extends Thread {
    Table table;
    int getServerBusyException = 0;

    TestPutThread(Table table){
      this.table = table;
    }

    @Override
    public void run() {
      try {
        Put p = new Put(ROW);
        p.addColumn(FAM_NAM, new byte[] { 0 }, new byte[] { 0 });
        table.put(p);
      } catch (ServerTooBusyException e) {
        getServerBusyException = 1;
      } catch (IOException ignore) {
      }
    }
  }

  private static class TestGetThread extends Thread {
    Table table;
    int getServerBusyException = 0;

    TestGetThread(Table table){
      this.table = table;
    }

    @Override
    public void run() {
      try {
        Get g = new Get(ROW);
        g.addColumn(FAM_NAM, new byte[] { 0 });
        table.get(g);
      } catch (ServerTooBusyException e) {
        getServerBusyException = 1;
      } catch (IOException ignore) {
      }
    }
  }

  @Test()
  public void testServerBusyException() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor(TableName.valueOf(name.getMethodName()));
    hdt.addCoprocessor(SleepCoprocessor.class.getName());
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    TEST_UTIL.createTable(hdt, new byte[][] { FAM_NAM }, c);

    TestGetThread tg1 =
        new TestGetThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestGetThread tg2 =
        new TestGetThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestGetThread tg3 =
        new TestGetThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestGetThread tg4 =
        new TestGetThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestGetThread tg5 =
        new TestGetThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    tg1.start();
    tg2.start();
    tg3.start();
    tg4.start();
    tg5.start();
    tg1.join();
    tg2.join();
    tg3.join();
    tg4.join();
    tg5.join();
    assertEquals(2,
        tg1.getServerBusyException + tg2.getServerBusyException + tg3.getServerBusyException
            + tg4.getServerBusyException + tg5.getServerBusyException);

    // Put has its own logic in HTable, test Put alone. We use AsyncProcess for Put (use multi at
    // RPC level) and it wrap exceptions to RetriesExhaustedWithDetailsException.

    TestPutThread tp1 =
        new TestPutThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestPutThread tp2 =
        new TestPutThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestPutThread tp3 =
        new TestPutThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestPutThread tp4 =
        new TestPutThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    TestPutThread tp5 =
        new TestPutThread(TEST_UTIL.getConnection().getTable(hdt.getTableName()));
    tp1.start();
    tp2.start();
    tp3.start();
    tp4.start();
    tp5.start();
    tp1.join();
    tp2.join();
    tp3.join();
    tp4.join();
    tp5.join();
    assertEquals(2,
        tp1.getServerBusyException + tp2.getServerBusyException + tp3.getServerBusyException
            + tp4.getServerBusyException + tp5.getServerBusyException);
  }
}
