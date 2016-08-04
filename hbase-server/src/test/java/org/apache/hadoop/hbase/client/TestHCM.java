/*
 *
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class is for testing HBaseConnectionManager features
 */
@Category({LargeTests.class})
public class TestHCM {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder()
      .withTimeout(this.getClass())
      .withLookingForStuckThread(true)
      .build();
  private static final Log LOG = LogFactory.getLog(TestHCM.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME =
      TableName.valueOf("test");
  private static final TableName TABLE_NAME1 =
      TableName.valueOf("test1");
  private static final TableName TABLE_NAME2 =
      TableName.valueOf("test2");
  private static final TableName TABLE_NAME3 =
      TableName.valueOf("test3");
  private static final TableName TABLE_NAME4 =
      TableName.valueOf("test4");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");
  private static final byte[] ROW_X = Bytes.toBytes("xxx");
  private static Random _randy = new Random();
  private static final int RPC_RETRY = 5;

/**
* This copro sleeps 20 second. The first call it fails. The second time, it works.
*/
  public static class SleepAndFailFirstTime extends BaseRegionObserver {
    static final AtomicLong ct = new AtomicLong(0);
    static final String SLEEP_TIME_CONF_KEY =
        "hbase.coprocessor.SleepAndFailFirstTime.sleepTime";
    static final long DEFAULT_SLEEP_TIME = 20000;
    static final AtomicLong sleepTime = new AtomicLong(DEFAULT_SLEEP_TIME);

    public SleepAndFailFirstTime() {
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
      RegionCoprocessorEnvironment env = c.getEnvironment();
      Configuration conf = env.getConfiguration();
      sleepTime.set(conf.getLong(SLEEP_TIME_CONF_KEY, DEFAULT_SLEEP_TIME));
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
              final Get get, final List<Cell> results) throws IOException {
      Threads.sleep(sleepTime.get());
      if (ct.incrementAndGet() == 1){
        throw new IOException("first call I fail");
      }
    }
  }

  public static class SleepCoprocessor extends BaseRegionObserver {
    public static final int SLEEP_TIME = 5000;
    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Get get, final List<Cell> results) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }
  }

  public static class SleepWriteCoprocessor extends BaseRegionObserver {
    public static final int SLEEP_TIME = 5000;
    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
                               final Increment increment) throws IOException {
      Threads.sleep(SLEEP_TIME);
      return super.preIncrement(e, increment);
    }
  }

  public static class SleepLongerAtFirstCoprocessor extends BaseRegionObserver {
    public static final int SLEEP_TIME = 2000;
    static final AtomicLong ct = new AtomicLong(0);

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Get get, final List<Cell> results) throws IOException {
      // After first sleep, all requests are timeout except the last retry. If we handle
      // all the following requests, finally the last request is also timeout. If we drop all
      // timeout requests, we can handle the last request immediately and it will not timeout.
      if (ct.incrementAndGet() <= 1) {
        Threads.sleep(SLEEP_TIME * (RPC_RETRY-1) * 2);
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
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }


  private static int getHConnectionManagerCacheSize(){
    return HConnectionTestingUtility.getConnectionCount();
  }

  @Test
  public void testClusterConnection() throws IOException {
    ThreadPoolExecutor otherPool = new ThreadPoolExecutor(1, 1,
        5, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        Threads.newDaemonThreadFactory("test-hcm"));

    HConnection con1 = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
    HConnection con2 = HConnectionManager.createConnection(TEST_UTIL.getConfiguration(), otherPool);
    // make sure the internally created ExecutorService is the one passed
    assertTrue(otherPool == ((HConnectionImplementation)con2).getCurrentBatchPool());

    String tableName = "testClusterConnection";
    TEST_UTIL.createTable(tableName.getBytes(), FAM_NAM).close();
    HTable t = (HTable)con1.getTable(tableName, otherPool);
    // make sure passing a pool to the getTable does not trigger creation of an internal pool
    assertNull("Internal Thread pool should be null", ((HConnectionImplementation)con1).getCurrentBatchPool());
    // table should use the pool passed
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable)con2.getTable(tableName);
    // table should use the connectin's internal pool
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable)con2.getTable(Bytes.toBytes(tableName));
    // try other API too
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable)con2.getTable(TableName.valueOf(tableName));
    // try other API too
    assertTrue(otherPool == t.getPool());
    t.close();

    t = (HTable)con1.getTable(tableName);
    ExecutorService pool = ((HConnectionImplementation)con1).getCurrentBatchPool();
    // make sure an internal pool was created
    assertNotNull("An internal Thread pool should have been created", pool);
    // and that the table is using it
    assertTrue(t.getPool() == pool);
    t.close();

    t = (HTable)con1.getTable(tableName);
    // still using the *same* internal pool
    assertTrue(t.getPool() == pool);
    t.close();

    con1.close();
    // if the pool was created on demand it should be closed upon connection close
    assertTrue(pool.isShutdown());

    con2.close();
    // if the pool is passed, it is not closed
    assertFalse(otherPool.isShutdown());
    otherPool.shutdownNow();
  }

  /**
   * Naive test to check that HConnection#getAdmin returns a properly constructed HBaseAdmin object
   * @throws IOException Unable to construct admin
   */
  @Test
  public void testAdminFactory() throws IOException {
    Connection con1 = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    Admin admin = con1.getAdmin();
    assertTrue(admin.getConnection() == con1);
    assertTrue(admin.getConfiguration() == TEST_UTIL.getConfiguration());
    con1.close();
  }

  // Fails too often!  Needs work.  HBASE-12558
  @Ignore @Test (expected = RegionServerStoppedException.class)
  public void testClusterStatus() throws Exception {

    TableName tn =
        TableName.valueOf("testClusterStatus");
    byte[] cf = "cf".getBytes();
    byte[] rk = "rk1".getBytes();

    JVMClusterUtil.RegionServerThread rs = TEST_UTIL.getHBaseCluster().startRegionServer();
    rs.waitForServerOnline();
    final ServerName sn = rs.getRegionServer().getServerName();

    HTable t = TEST_UTIL.createTable(tn, cf);
    TEST_UTIL.waitTableAvailable(tn);
    TEST_UTIL.waitUntilNoRegionsInTransition();

    final HConnectionImplementation hci =  (HConnectionImplementation)t.getConnection();
    while (t.getRegionLocation(rk).getPort() != sn.getPort()){
      TEST_UTIL.getHBaseAdmin().move(t.getRegionLocation(rk).getRegionInfo().
          getEncodedNameAsBytes(), Bytes.toBytes(sn.toString()));
      TEST_UTIL.waitUntilNoRegionsInTransition();
      hci.clearRegionCache(tn);
    }
    Assert.assertNotNull(hci.clusterStatusListener);
    TEST_UTIL.assertRegionOnServer(t.getRegionLocation(rk).getRegionInfo(), sn, 20000);

    Put p1 = new Put(rk);
    p1.add(cf, "qual".getBytes(), "val".getBytes());
    t.put(p1);

    rs.getRegionServer().abort("I'm dead");

    // We want the status to be updated. That's a least 10 second
    TEST_UTIL.waitFor(40000, 1000, true, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getHBaseCluster().getMaster().getServerManager().
            getDeadServers().isDeadServer(sn);
      }
    });

    TEST_UTIL.waitFor(40000, 1000, true, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return hci.clusterStatusListener.isDeadServer(sn);
      }
    });

    t.close();
    hci.getClient(sn);  // will throw an exception: RegionServerStoppedException
  }

  /**
   * Test that we can handle connection close: it will trigger a retry, but the calls will
   *  finish.
   */
  @Test
  public void testConnectionCloseAllowsInterrupt() throws Exception {
    testConnectionClose(true);
  }

  @Test
  public void testConnectionNotAllowsInterrupt() throws Exception {
    testConnectionClose(false);
  }

  /**
   * Test that an operation can fail if we read the global operation timeout, even if the
   * individual timeout is fine. We do that with:
   * - client side: an operation timeout of 30 seconds
   * - server side: we sleep 20 second at each attempt. The first work fails, the second one
   * succeeds. But the client won't wait that much, because 20 + 20 > 30, so the client
   * timeouted when the server answers.
   */
  @Test
  public void testOperationTimeout() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor("HCM-testOperationTimeout");
    hdt.addCoprocessor(SleepAndFailFirstTime.class.getName());
    HTable table = TEST_UTIL.createTable(hdt, new byte[][]{FAM_NAM}, TEST_UTIL.getConfiguration());
    table.setRpcTimeout(Integer.MAX_VALUE);
    // Check that it works if the timeout is big enough
    table.setOperationTimeout(120 * 1000);
    table.get(new Get(FAM_NAM));

    // Resetting and retrying. Will fail this time, not enough time for the second try
    SleepAndFailFirstTime.ct.set(0);
    try {
      table.setOperationTimeout(30 * 1000);
      table.get(new Get(FAM_NAM));
      Assert.fail("We expect an exception here");
    } catch (SocketTimeoutException e) {
      // The client has a CallTimeout class, but it's not shared.We're not very clean today,
      //  in the general case you can expect the call to stop, but the exception may vary.
      // In this test however, we're sure that it will be a socket timeout.
      LOG.info("We received an exception, as expected ", e);
    } catch (IOException e) {
      Assert.fail("Wrong exception:" + e.getMessage());
    } finally {
      table.close();
    }
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testRpcTimeout() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor("HCM-testRpcTimeout");
    hdt.addCoprocessor(SleepCoprocessor.class.getName());
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());

    try (Table t = TEST_UTIL.createTable(hdt, new byte[][] { FAM_NAM }, c)) {
      assert t instanceof HTable;
      HTable table = (HTable) t;
      table.setRpcTimeout(SleepCoprocessor.SLEEP_TIME / 2);
      table.setOperationTimeout(SleepCoprocessor.SLEEP_TIME * 100);
      table.get(new Get(FAM_NAM));
    }
  }

  @Test
  public void testDropTimeoutRequest() throws Exception {
    // Simulate the situation that the server is slow and client retries for several times because
    // of timeout. When a request can be handled after waiting in the queue, we will drop it if
    // it has been considered as timeout at client. If we don't drop it, the server will waste time
    // on handling timeout requests and finally all requests timeout and client throws exception.
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor("HCM-testDropTimeputRequest");
    hdt.addCoprocessor(SleepLongerAtFirstCoprocessor.class.getName());
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    try (Table t = TEST_UTIL.createTable(hdt, new byte[][] { FAM_NAM }, c)) {
      t.setRpcTimeout(SleepLongerAtFirstCoprocessor.SLEEP_TIME * 2);
      t.get(new Get(FAM_NAM));
    }
  }

  /**
   * Test starting from 0 index when RpcRetryingCaller calculate the backoff time.
   */
  @Test
  public void testRpcRetryingCallerSleep() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor("HCM-testRpcRetryingCallerSleep");
    hdt.addCoprocessorWithSpec("|" + SleepAndFailFirstTime.class.getName() + "||"
        + SleepAndFailFirstTime.SLEEP_TIME_CONF_KEY + "=2000");
    TEST_UTIL.createTable(hdt, new byte[][] { FAM_NAM }).close();

    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_PAUSE, 3000);
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 4000);

    Connection connection = ConnectionFactory.createConnection(c);
    Table t = connection.getTable(TableName.valueOf("HCM-testRpcRetryingCallerSleep"));
    if (t instanceof HTable) {
      HTable table = (HTable) t;
      table.setOperationTimeout(8000);
      // Check that it works. Because 2s + 3s * RETRY_BACKOFF[0] + 2s < 8s
      table.get(new Get(FAM_NAM));

      // Resetting and retrying.
      SleepAndFailFirstTime.ct.set(0);
      try {
        table.setOperationTimeout(6000);
        // Will fail this time. After sleep, there are not enough time for second retry
        // Beacuse 2s + 3s + 2s > 6s
        table.get(new Get(FAM_NAM));
        Assert.fail("We expect an exception here");
      } catch (SocketTimeoutException e) {
        LOG.info("We received an exception, as expected ", e);
      } catch (IOException e) {
        Assert.fail("Wrong exception:" + e.getMessage());
      } finally {
        table.close();
        connection.close();
      }
    }
  }

  @Test
  public void testCallableSleep() throws Exception {
    long pauseTime;
    long baseTime = 100;
    TableName tableName = TableName.valueOf("HCM-testCallableSleep");
    HTable table = TEST_UTIL.createTable(tableName, FAM_NAM);
    RegionServerCallable<Object> regionServerCallable = new RegionServerCallable<Object>(
        TEST_UTIL.getConnection(), tableName, ROW) {
      public Object call(int timeout) throws IOException {
        return null;
      }
    };

    regionServerCallable.prepare(false);
    for (int i = 0; i < HConstants.RETRY_BACKOFF.length; i++) {
      pauseTime = regionServerCallable.sleep(baseTime, i);
      assertTrue(pauseTime >= (baseTime * HConstants.RETRY_BACKOFF[i]));
      assertTrue(pauseTime <= (baseTime * HConstants.RETRY_BACKOFF[i] * 1.01f));
    }

    RegionAdminServiceCallable<Object> regionAdminServiceCallable =
        new RegionAdminServiceCallable<Object>(
        (ClusterConnection) TEST_UTIL.getConnection(), new RpcControllerFactory(
            TEST_UTIL.getConfiguration()), tableName, ROW) {
      public Object call(int timeout) throws IOException {
        return null;
      }
    };

    regionAdminServiceCallable.prepare(false);
    for (int i = 0; i < HConstants.RETRY_BACKOFF.length; i++) {
      pauseTime = regionAdminServiceCallable.sleep(baseTime, i);
      assertTrue(pauseTime >= (baseTime * HConstants.RETRY_BACKOFF[i]));
      assertTrue(pauseTime <= (baseTime * HConstants.RETRY_BACKOFF[i] * 1.01f));
    }

    MasterCallable masterCallable = new MasterCallable((HConnection) TEST_UTIL.getConnection()) {
      public Object call(int timeout) throws IOException {
        return null;
      }
    };

    for (int i = 0; i < HConstants.RETRY_BACKOFF.length; i++) {
      pauseTime = masterCallable.sleep(baseTime, i);
      assertTrue(pauseTime >= (baseTime * HConstants.RETRY_BACKOFF[i]));
      assertTrue(pauseTime <= (baseTime * HConstants.RETRY_BACKOFF[i] * 1.01f));
    }
  }

  private void testConnectionClose(boolean allowsInterrupt) throws Exception {
    TableName tableName = TableName.valueOf("HCM-testConnectionClose" + allowsInterrupt);
    TEST_UTIL.createTable(tableName, FAM_NAM).close();

    boolean previousBalance = TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true);

    Configuration c2 = new Configuration(TEST_UTIL.getConfiguration());
    // We want to work on a separate connection.
    c2.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
    c2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 100); // retry a lot
    c2.setInt(HConstants.HBASE_CLIENT_PAUSE, 1); // don't wait between retries.
    c2.setInt(RpcClient.FAILED_SERVER_EXPIRY_KEY, 0); // Server do not really expire
    c2.setBoolean(RpcClient.SPECIFIC_WRITE_THREAD, allowsInterrupt);

    final HTable table = new HTable(c2, tableName);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    // 4 steps: ready=0; doGets=1; mustStop=2; stopped=3
    final AtomicInteger step = new AtomicInteger(0);

    final AtomicReference<Throwable> failed = new AtomicReference<Throwable>(null);
    Thread t = new Thread("testConnectionCloseThread") {
      @Override
      public void run() {
        int done = 0;
        try {
          step.set(1);
          while (step.get() == 1) {
            Get get = new Get(ROW);
            table.get(get);
            done++;
            if (done % 100 == 0)
              LOG.info("done=" + done);
          }
        } catch (Throwable t) {
          failed.set(t);
          LOG.error(t);
        }
        step.set(3);
      }
    };
    t.start();
    TEST_UTIL.waitFor(20000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return step.get() == 1;
      }
    });

    ServerName sn = table.getRegionLocation(ROW).getServerName();
    ConnectionManager.HConnectionImplementation conn =
        (ConnectionManager.HConnectionImplementation) table.getConnection();
    RpcClient rpcClient = conn.getRpcClient();

    LOG.info("Going to cancel connections. connection=" + conn.toString() + ", sn=" + sn);
    for (int i = 0; i < 5000; i++) {
      rpcClient.cancelConnections(sn);
      Thread.sleep(5);
    }

    step.compareAndSet(1, 2);
    // The test may fail here if the thread doing the gets is stuck. The way to find
    //  out what's happening is to look for the thread named 'testConnectionCloseThread'
    TEST_UTIL.waitFor(40000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return step.get() == 3;
      }
    });
    table.close();
    Assert.assertTrue("Unexpected exception is " + failed.get(), failed.get() == null);
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(previousBalance, true);
  }

  /**
   * Test that connection can become idle without breaking everything.
   */
  @Test
  public void testConnectionIdle() throws Exception {
    TableName tableName = TableName.valueOf("HCM-testConnectionIdle");
    TEST_UTIL.createTable(tableName, FAM_NAM).close();
    int idleTime =  20000;
    boolean previousBalance = TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true);

    Configuration c2 = new Configuration(TEST_UTIL.getConfiguration());
    // We want to work on a separate connection.
    c2.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
    c2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1); // Don't retry: retry = test failed
    c2.setInt(RpcClient.IDLE_TIME, idleTime);

    final Table table = new HTable(c2, tableName);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    ManualEnvironmentEdge mee = new ManualEnvironmentEdge();
    mee.setValue(System.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(mee);
    LOG.info("first get");
    table.get(new Get(ROW));

    LOG.info("first get - changing the time & sleeping");
    mee.incValue(idleTime + 1000);
    Thread.sleep(1500); // we need to wait a little for the connection to be seen as idle.
                        // 1500 = sleep time in RpcClient#waitForWork + a margin

    LOG.info("second get - connection has been marked idle in the middle");
    // To check that the connection actually became idle would need to read some private
    //  fields of RpcClient.
    table.get(new Get(ROW));
    mee.incValue(idleTime + 1000);

    LOG.info("third get - connection is idle, but the reader doesn't know yet");
    // We're testing here a special case:
    //  time limit reached BUT connection not yet reclaimed AND a new call.
    //  in this situation, we don't close the connection, instead we use it immediately.
    // If we're very unlucky we can have a race condition in the test: the connection is already
    //  under closing when we do the get, so we have an exception, and we don't retry as the
    //  retry number is 1. The probability is very very low, and seems acceptable for now. It's
    //  a test issue only.
    table.get(new Get(ROW));

    LOG.info("we're done - time will change back");

    table.close();
    EnvironmentEdgeManager.reset();
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(previousBalance, true);
  }

    /**
     * Test that the connection to the dead server is cut immediately when we receive the
     *  notification.
     * @throws Exception
     */
  @Test
  public void testConnectionCut() throws Exception {

    TableName tableName = TableName.valueOf("HCM-testConnectionCut");

    TEST_UTIL.createTable(tableName, FAM_NAM).close();
    boolean previousBalance = TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true);

    Configuration c2 = new Configuration(TEST_UTIL.getConfiguration());
    // We want to work on a separate connection.
    c2.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
    c2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    c2.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 30 * 1000);

    HTable table = new HTable(c2, tableName);

    Put p = new Put(FAM_NAM);
    p.add(FAM_NAM, FAM_NAM, FAM_NAM);
    table.put(p);

    final HConnectionImplementation hci =  (HConnectionImplementation)table.getConnection();
    final HRegionLocation loc = table.getRegionLocation(FAM_NAM);

    Get get = new Get(FAM_NAM);
    Assert.assertNotNull(table.get(get));

    get = new Get(FAM_NAM);
    get.setFilter(new BlockingFilter());

    // This thread will mark the server as dead while we're waiting during a get.
    Thread t = new Thread() {
      @Override
      public void run() {
        synchronized (syncBlockingFilter) {
          try {
            syncBlockingFilter.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        hci.clusterStatusListener.deadServerHandler.newDead(loc.getServerName());
      }
    };

    t.start();
    try {
      table.get(get);
      Assert.fail();
    } catch (IOException expected) {
      LOG.debug("Received: " + expected);
      Assert.assertFalse(expected instanceof SocketTimeoutException);
      Assert.assertFalse(syncBlockingFilter.get());
    } finally {
      syncBlockingFilter.set(true);
      t.join();
      HConnectionManager.getConnection(c2).close();
      TEST_UTIL.getHBaseAdmin().setBalancerRunning(previousBalance, true);
    }

    table.close();
  }

  protected static final AtomicBoolean syncBlockingFilter = new AtomicBoolean(false);

  public static class BlockingFilter extends FilterBase {
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
      int i = 0;
      while (i++ < 1000 && !syncBlockingFilter.get()) {
        synchronized (syncBlockingFilter) {
          syncBlockingFilter.notifyAll();
        }
        Threads.sleep(100);
      }
      syncBlockingFilter.set(true);
      return false;
    }
    @Override
    public ReturnCode filterKeyValue(Cell ignored) throws IOException {
      return ReturnCode.INCLUDE;
    }

    public static Filter parseFrom(final byte [] pbBytes) throws DeserializationException{
      return new BlockingFilter();
    }
  }

  @Test (timeout=120000)
  public void abortingHConnectionRemovesItselfFromHCM() throws Exception {
    // Save off current HConnections
    Map<HConnectionKey, HConnectionImplementation> oldHBaseInstances =
        new HashMap<HConnectionKey, HConnectionImplementation>();
    oldHBaseInstances.putAll(ConnectionManager.CONNECTION_INSTANCES);

    ConnectionManager.CONNECTION_INSTANCES.clear();

    try {
      HConnection connection = HConnectionManager.getConnection(TEST_UTIL.getConfiguration());
      connection.abort("test abortingHConnectionRemovesItselfFromHCM", new Exception(
          "test abortingHConnectionRemovesItselfFromHCM"));
      Assert.assertNotSame(connection,
        HConnectionManager.getConnection(TEST_UTIL.getConfiguration()));
    } finally {
      // Put original HConnections back
      ConnectionManager.CONNECTION_INSTANCES.clear();
      ConnectionManager.CONNECTION_INSTANCES.putAll(oldHBaseInstances);
    }
  }

  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  @Test
  public void testRegionCaching() throws Exception{
    TEST_UTIL.createMultiRegionTable(TABLE_NAME, FAM_NAM).close();
    Configuration conf =  new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HTable table = new HTable(conf, TABLE_NAME);

    TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    ConnectionManager.HConnectionImplementation conn =
      (ConnectionManager.HConnectionImplementation)table.getConnection();

    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));

    final int nextPort = conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation().getPort() + 1;
    HRegionLocation loc = conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation();
    conn.updateCachedLocation(loc.getRegionInfo(), loc.getServerName(),
        ServerName.valueOf("127.0.0.1", nextPort,
        HConstants.LATEST_TIMESTAMP), HConstants.LATEST_TIMESTAMP);
    Assert.assertEquals(conn.getCachedLocation(TABLE_NAME, ROW)
      .getRegionLocation().getPort(), nextPort);

    conn.clearRegionCache(TABLE_NAME, ROW.clone());
    RegionLocations rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);

    // We're now going to move the region and check that it works for the client
    // First a new put to add the location in the cache
    conn.clearRegionCache(TABLE_NAME);
    Assert.assertEquals(0, conn.getNumberOfCachedRegionLocations(TABLE_NAME));
    Put put2 = new Put(ROW);
    put2.add(FAM_NAM, ROW, ROW);
    table.put(put2);
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    assertNotNull(conn.getCachedLocation(TableName.valueOf(TABLE_NAME.getName()), ROW.clone()));

    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, false);
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();

    // We can wait for all regions to be online, that makes log reading easier when debugging
    TEST_UTIL.waitUntilNoRegionsInTransition();

    // Now moving the region to the second server
    HRegionLocation toMove = conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation();
    byte[] regionName = toMove.getRegionInfo().getRegionName();
    byte[] encodedRegionNameBytes = toMove.getRegionInfo().getEncodedNameAsBytes();

    // Choose the other server.
    int curServerId = TEST_UTIL.getHBaseCluster().getServerWith(regionName);
    int destServerId = (curServerId == 0 ? 1 : 0);

    HRegionServer curServer = TEST_UTIL.getHBaseCluster().getRegionServer(curServerId);
    HRegionServer destServer = TEST_UTIL.getHBaseCluster().getRegionServer(destServerId);

    ServerName destServerName = destServer.getServerName();

    // Check that we are in the expected state
    Assert.assertTrue(curServer != destServer);
    Assert.assertFalse(curServer.getServerName().equals(destServer.getServerName()));
    Assert.assertFalse( toMove.getPort() == destServerName.getPort());
    Assert.assertNotNull(curServer.getOnlineRegion(regionName));
    Assert.assertNull(destServer.getOnlineRegion(regionName));
    Assert.assertFalse(TEST_UTIL.getMiniHBaseCluster().getMaster().
        getAssignmentManager().getRegionStates().isRegionsInTransition());

    // Moving. It's possible that we don't have all the regions online at this point, so
    //  the test must depends only on the region we're looking at.
    LOG.info("Move starting region="+toMove.getRegionInfo().getRegionNameAsString());
    TEST_UTIL.getHBaseAdmin().move(
      toMove.getRegionInfo().getEncodedNameAsBytes(),
      destServerName.getServerName().getBytes()
    );

    while (destServer.getOnlineRegion(regionName) == null ||
        destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      // wait for the move to be finished
      Thread.sleep(1);
    }

    LOG.info("Move finished for region="+toMove.getRegionInfo().getRegionNameAsString());

    // Check our new state.
    Assert.assertNull(curServer.getOnlineRegion(regionName));
    Assert.assertNotNull(destServer.getOnlineRegion(regionName));
    Assert.assertFalse(destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));
    Assert.assertFalse(curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));


    // Cache was NOT updated and points to the wrong server
    Assert.assertFalse(
        conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation()
          .getPort() == destServerName.getPort());

    // This part relies on a number of tries equals to 1.
    // We do a put and expect the cache to be updated, even if we don't retry
    LOG.info("Put starting");
    Put put3 = new Put(ROW);
    put3.add(FAM_NAM, ROW, ROW);
    try {
      table.put(put3);
      Assert.fail("Unreachable point");
    } catch (RetriesExhaustedWithDetailsException e){
      LOG.info("Put done, exception caught: " + e.getClass());
      Assert.assertEquals(1, e.getNumExceptions());
      Assert.assertEquals(1, e.getCauses().size());
      Assert.assertArrayEquals(e.getRow(0).getRow(), ROW);

      // Check that we unserialized the exception as expected
      Throwable cause = ClientExceptionsUtil.findException(e.getCause(0));
      Assert.assertNotNull(cause);
      Assert.assertTrue(cause instanceof RegionMovedException);
    }
    Assert.assertNotNull("Cached connection is null", conn.getCachedLocation(TABLE_NAME, ROW));
    Assert.assertEquals(
        "Previous server was " + curServer.getServerName().getHostAndPort(),
        destServerName.getPort(),
        conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation().getPort());

    Assert.assertFalse(destServer.getRegionsInTransitionInRS()
      .containsKey(encodedRegionNameBytes));
    Assert.assertFalse(curServer.getRegionsInTransitionInRS()
      .containsKey(encodedRegionNameBytes));

    // We move it back to do another test with a scan
    LOG.info("Move starting region=" + toMove.getRegionInfo().getRegionNameAsString());
    TEST_UTIL.getHBaseAdmin().move(
      toMove.getRegionInfo().getEncodedNameAsBytes(),
      curServer.getServerName().getServerName().getBytes()
    );

    while (curServer.getOnlineRegion(regionName) == null ||
        destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      // wait for the move to be finished
      Thread.sleep(1);
    }

    // Check our new state.
    Assert.assertNotNull(curServer.getOnlineRegion(regionName));
    Assert.assertNull(destServer.getOnlineRegion(regionName));
    LOG.info("Move finished for region=" + toMove.getRegionInfo().getRegionNameAsString());

    // Cache was NOT updated and points to the wrong server
    Assert.assertFalse(conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation().getPort() ==
      curServer.getServerName().getPort());

    Scan sc = new Scan();
    sc.setStopRow(ROW);
    sc.setStartRow(ROW);

    // The scanner takes the max retries from the connection configuration, not the table as
    // the put.
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    try {
      ResultScanner rs = table.getScanner(sc);
      while (rs.next() != null) {
      }
      Assert.fail("Unreachable point");
    } catch (RetriesExhaustedException e) {
      LOG.info("Scan done, expected exception caught: " + e.getClass());
    }

    // Cache is updated with the right value.
    Assert.assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    Assert.assertEquals(
      "Previous server was "+destServer.getServerName().getHostAndPort(),
      curServer.getServerName().getPort(),
      conn.getCachedLocation(TABLE_NAME, ROW).getRegionLocation().getPort());

    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    table.close();
  }

  /**
   * Test that Connection or Pool are not closed when managed externally
   * @throws Exception
   */
  @Test
  public void testConnectionManagement() throws Exception{
    Table table0 = TEST_UTIL.createTable(TABLE_NAME1, FAM_NAM);
    Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    HTable table = (HTable) conn.getTable(TABLE_NAME1);
    table.close();
    assertFalse(conn.isClosed());
    assertFalse(table.getPool().isShutdown());
    table = (HTable) conn.getTable(TABLE_NAME1);
    table.close();
    assertFalse(table.getPool().isShutdown());
    conn.close();
    assertTrue(table.getPool().isShutdown());
    table0.close();
  }

  /**
   * Test that stale cache updates don't override newer cached values.
   */
  @Test
  public void testCacheSeqNums() throws Exception{
    HTable table = TEST_UTIL.createMultiRegionTable(TABLE_NAME2, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    ConnectionManager.HConnectionImplementation conn =
      (ConnectionManager.HConnectionImplementation)table.getConnection();

    HRegionLocation location = conn.getCachedLocation(TABLE_NAME2, ROW).getRegionLocation();
    assertNotNull(location);

    ServerName anySource = ServerName.valueOf(location.getHostname(), location.getPort() - 1, 0L);

    // Same server as already in cache reporting - overwrites any value despite seqNum.
    int nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), location.getServerName(),
        ServerName.valueOf("127.0.0.1", nextPort, 0), location.getSeqNum() - 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW).getRegionLocation();
    Assert.assertEquals(nextPort, location.getPort());

    // No source specified - same.
    nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), location.getServerName(),
        ServerName.valueOf("127.0.0.1", nextPort, 0), location.getSeqNum() - 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW).getRegionLocation();
    Assert.assertEquals(nextPort, location.getPort());

    // Higher seqNum - overwrites lower seqNum.
    nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), anySource,
        ServerName.valueOf("127.0.0.1", nextPort, 0), location.getSeqNum() + 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW).getRegionLocation();
    Assert.assertEquals(nextPort, location.getPort());

    // Lower seqNum - does not overwrite higher seqNum.
    nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), anySource,
        ServerName.valueOf("127.0.0.1", nextPort, 0), location.getSeqNum() - 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW).getRegionLocation();
    Assert.assertEquals(nextPort - 1, location.getPort());
    table.close();
  }

  /**
   * Make sure that {@link Configuration} instances that are essentially the
   * same map to the same {@link HConnection} instance.
   */
  @Test
  public void testConnectionSameness() throws Exception {
    Connection previousConnection = null;
    for (int i = 0; i < 2; i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = TEST_UTIL.getConfiguration();
      configuration.set("some_key", String.valueOf(_randy.nextInt()));
      LOG.info("The hash code of the current configuration is: "
          + configuration.hashCode());
      Connection currentConnection = HConnectionManager
          .getConnection(configuration);
      if (previousConnection != null) {
        assertTrue(
            "Did not get the same connection even though its key didn't change",
            previousConnection == currentConnection);
      }
      previousConnection = currentConnection;
      // change the configuration, so that it is no longer reachable from the
      // client's perspective. However, since its part of the LRU doubly linked
      // list, it will eventually get thrown out, at which time it should also
      // close the corresponding {@link HConnection}.
      configuration.set("other_key", String.valueOf(_randy.nextInt()));
    }
  }

  /**
   * Makes sure that there is no leaking of
   * {@link ConnectionManager.HConnectionImplementation} in the {@link HConnectionManager}
   * class.
   * @deprecated Tests deprecated functionality.  Remove in 1.0.
   */
  @Deprecated
  @Test
  public void testConnectionUniqueness() throws Exception {
    int zkmaxconnections = TEST_UTIL.getConfiguration().
      getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
          HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS);
    // Test up to a max that is < the maximum number of zk connections.  If we
    // go above zk connections, we just fall into cycle where we are failing
    // to set up a session and test runs for a long time.
    int maxConnections = Math.min(zkmaxconnections - 1, 20);
    List<HConnection> connections = new ArrayList<HConnection>(maxConnections);
    Connection previousConnection = null;
    try {
      for (int i = 0; i < maxConnections; i++) {
        // set random key to differentiate the connection from previous ones
        Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
        configuration.set("some_key", String.valueOf(_randy.nextInt()));
        configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
            String.valueOf(_randy.nextInt()));
        LOG.info("The hash code of the current configuration is: "
            + configuration.hashCode());
        HConnection currentConnection =
          HConnectionManager.getConnection(configuration);
        if (previousConnection != null) {
          assertTrue("Got the same connection even though its key changed!",
              previousConnection != currentConnection);
        }
        // change the configuration, so that it is no longer reachable from the
        // client's perspective. However, since its part of the LRU doubly linked
        // list, it will eventually get thrown out, at which time it should also
        // close the corresponding {@link HConnection}.
        configuration.set("other_key", String.valueOf(_randy.nextInt()));

        previousConnection = currentConnection;
        LOG.info("The current HConnectionManager#HBASE_INSTANCES cache size is: "
            + getHConnectionManagerCacheSize());
        Thread.sleep(50);
        connections.add(currentConnection);
      }
    } finally {
      for (Connection c: connections) {
        // Clean up connections made so we don't interfere w/ subsequent tests.
        HConnectionManager.deleteConnection(c.getConfiguration());
      }
    }
  }

  @Test
  public void testClosing() throws Exception {
    Configuration configuration =
      new Configuration(TEST_UTIL.getConfiguration());
    configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
        String.valueOf(_randy.nextInt()));

    Connection c1 = ConnectionFactory.createConnection(configuration);
    // We create two connections with the same key.
    Connection c2 = ConnectionFactory.createConnection(configuration);

    Connection c3 = HConnectionManager.getConnection(configuration);
    Connection c4 = HConnectionManager.getConnection(configuration);
    assertTrue(c3 == c4);

    c1.close();
    assertTrue(c1.isClosed());
    assertFalse(c2.isClosed());
    assertFalse(c3.isClosed());

    c3.close();
    // still a reference left
    assertFalse(c3.isClosed());
    c3.close();
    assertTrue(c3.isClosed());
    // c3 was removed from the cache
    Connection c5 = HConnectionManager.getConnection(configuration);
    assertTrue(c5 != c3);

    assertFalse(c2.isClosed());
    c2.close();
    assertTrue(c2.isClosed());
    c5.close();
    assertTrue(c5.isClosed());
  }

  /**
   * Trivial test to verify that nobody messes with
   * {@link HConnectionManager#createConnection(Configuration)}
   */
  @Test
  public void testCreateConnection() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    Connection c1 = ConnectionFactory.createConnection(configuration);
    Connection c2 = ConnectionFactory.createConnection(configuration);
    // created from the same configuration, yet they are different
    assertTrue(c1 != c2);
    assertTrue(c1.getConfiguration() == c2.getConfiguration());
    // make sure these were not cached
    Connection c3 = HConnectionManager.getConnection(configuration);
    assertTrue(c1 != c3);
    assertTrue(c2 != c3);
  }


  /**
   * This test checks that one can connect to the cluster with only the
   *  ZooKeeper quorum set. Other stuff like master address will be read
   *  from ZK by the client.
   */
  @Test
  public void testConnection() throws Exception{
    // We create an empty config and add the ZK address.
    Configuration c = new Configuration();
    c.set(HConstants.ZOOKEEPER_QUORUM,
      TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    c.set(HConstants.ZOOKEEPER_CLIENT_PORT ,
      TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));

    // This should be enough to connect
    HConnection conn = HConnectionManager.getConnection(c);
    assertTrue( conn.isMasterRunning() );
    conn.close();
  }

  private int setNumTries(HConnectionImplementation hci, int newVal) throws Exception {
    Field numTries = hci.getClass().getDeclaredField("numTries");
    numTries.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(numTries, numTries.getModifiers() & ~Modifier.FINAL);
    final int prevNumRetriesVal = (Integer)numTries.get(hci);
    numTries.set(hci, newVal);

    return prevNumRetriesVal;
  }

  @Test
  public void testMulti() throws Exception {
    HTable table = TEST_UTIL.createMultiRegionTable(TABLE_NAME3, FAM_NAM);
    try {
       ConnectionManager.HConnectionImplementation conn =
           ( ConnectionManager.HConnectionImplementation)table.getConnection();

       // We're now going to move the region and check that it works for the client
       // First a new put to add the location in the cache
       conn.clearRegionCache(TABLE_NAME3);
       Assert.assertEquals(0, conn.getNumberOfCachedRegionLocations(TABLE_NAME3));

       TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, false);
       HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();

       // We can wait for all regions to be online, that makes log reading easier when debugging
       TEST_UTIL.waitUntilNoRegionsInTransition();

       Put put = new Put(ROW_X);
       put.add(FAM_NAM, ROW_X, ROW_X);
       table.put(put);

       // Now moving the region to the second server
       HRegionLocation toMove = conn.getCachedLocation(TABLE_NAME3, ROW_X).getRegionLocation();
       byte[] regionName = toMove.getRegionInfo().getRegionName();
       byte[] encodedRegionNameBytes = toMove.getRegionInfo().getEncodedNameAsBytes();

       // Choose the other server.
       int curServerId = TEST_UTIL.getHBaseCluster().getServerWith(regionName);
       int destServerId = (curServerId == 0 ? 1 : 0);

       HRegionServer curServer = TEST_UTIL.getHBaseCluster().getRegionServer(curServerId);
       HRegionServer destServer = TEST_UTIL.getHBaseCluster().getRegionServer(destServerId);

       ServerName destServerName = destServer.getServerName();

       //find another row in the cur server that is less than ROW_X
       List<Region> regions = curServer.getOnlineRegions(TABLE_NAME3);
       byte[] otherRow = null;
       for (Region region : regions) {
         if (!region.getRegionInfo().getEncodedName().equals(toMove.getRegionInfo().getEncodedName())
             && Bytes.BYTES_COMPARATOR.compare(region.getRegionInfo().getStartKey(), ROW_X) < 0) {
           otherRow = region.getRegionInfo().getStartKey();
           break;
         }
       }
       assertNotNull(otherRow);
       // If empty row, set it to first row.-f
       if (otherRow.length <= 0) otherRow = Bytes.toBytes("aaa");
       Put put2 = new Put(otherRow);
       put2.add(FAM_NAM, otherRow, otherRow);
       table.put(put2); //cache put2's location

       // Check that we are in the expected state
       Assert.assertTrue(curServer != destServer);
       Assert.assertNotEquals(curServer.getServerName(), destServer.getServerName());
       Assert.assertNotEquals(toMove.getPort(), destServerName.getPort());
       Assert.assertNotNull(curServer.getOnlineRegion(regionName));
       Assert.assertNull(destServer.getOnlineRegion(regionName));
       Assert.assertFalse(TEST_UTIL.getMiniHBaseCluster().getMaster().
           getAssignmentManager().getRegionStates().isRegionsInTransition());

       // Moving. It's possible that we don't have all the regions online at this point, so
       //  the test must depends only on the region we're looking at.
       LOG.info("Move starting region="+toMove.getRegionInfo().getRegionNameAsString());
       TEST_UTIL.getHBaseAdmin().move(
           toMove.getRegionInfo().getEncodedNameAsBytes(),
           destServerName.getServerName().getBytes()
           );

       while (destServer.getOnlineRegion(regionName) == null ||
           destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
           curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
           master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
         // wait for the move to be finished
         Thread.sleep(1);
        }

       LOG.info("Move finished for region="+toMove.getRegionInfo().getRegionNameAsString());

       // Check our new state.
       Assert.assertNull(curServer.getOnlineRegion(regionName));
       Assert.assertNotNull(destServer.getOnlineRegion(regionName));
       Assert.assertFalse(destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));
       Assert.assertFalse(curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));


       // Cache was NOT updated and points to the wrong server
       Assert.assertFalse(
           conn.getCachedLocation(TABLE_NAME3, ROW_X).getRegionLocation()
            .getPort() == destServerName.getPort());

       // Hijack the number of retry to fail after 2 tries
       final int prevNumRetriesVal = setNumTries(conn, 2);

       Put put3 = new Put(ROW_X);
       put3.add(FAM_NAM, ROW_X, ROW_X);
       Put put4 = new Put(otherRow);
       put4.add(FAM_NAM, otherRow, otherRow);

       // do multi
       table.batch(Lists.newArrayList(put4, put3)); // first should be a valid row,
       // second we get RegionMovedException.

       setNumTries(conn, prevNumRetriesVal);
     } finally {
       table.close();
     }
  }

  @Test
  public void testErrorBackoffTimeCalculation() throws Exception {
    // TODO: This test would seem to presume hardcoded RETRY_BACKOFF which it should not.
    final long ANY_PAUSE = 100;
    ServerName location = ServerName.valueOf("127.0.0.1", 1, 0);
    ServerName diffLocation = ServerName.valueOf("127.0.0.1", 2, 0);

    ManualEnvironmentEdge timeMachine = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(timeMachine);
    try {
      long timeBase = timeMachine.currentTime();
      long largeAmountOfTime = ANY_PAUSE * 1000;
      ConnectionManager.ServerErrorTracker tracker =
          new ConnectionManager.ServerErrorTracker(largeAmountOfTime, 100);

      // The default backoff is 0.
      assertEquals(0, tracker.calculateBackoffTime(location, ANY_PAUSE));

      // Check some backoff values from HConstants sequence.
      tracker.reportServerError(location);
      assertEqualsWithJitter(ANY_PAUSE * HConstants.RETRY_BACKOFF[0],
        tracker.calculateBackoffTime(location, ANY_PAUSE));
      tracker.reportServerError(location);
      tracker.reportServerError(location);
      tracker.reportServerError(location);
      assertEqualsWithJitter(ANY_PAUSE * HConstants.RETRY_BACKOFF[3],
        tracker.calculateBackoffTime(location, ANY_PAUSE));

      // All of this shouldn't affect backoff for different location.
      assertEquals(0, tracker.calculateBackoffTime(diffLocation, ANY_PAUSE));
      tracker.reportServerError(diffLocation);
      assertEqualsWithJitter(ANY_PAUSE * HConstants.RETRY_BACKOFF[0],
        tracker.calculateBackoffTime(diffLocation, ANY_PAUSE));

      // Check with different base.
      assertEqualsWithJitter(ANY_PAUSE * 2 * HConstants.RETRY_BACKOFF[3],
          tracker.calculateBackoffTime(location, ANY_PAUSE * 2));
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private static void assertEqualsWithJitter(long expected, long actual) {
    assertEqualsWithJitter(expected, actual, expected);
  }

  private static void assertEqualsWithJitter(long expected, long actual, long jitterBase) {
    assertTrue("Value not within jitter: " + expected + " vs " + actual,
        Math.abs(actual - expected) <= (0.01f * jitterBase));
  }

  /**
   * Tests that a destroyed connection does not have a live zookeeper.
   * Below is timing based.  We put up a connection to a table and then close the connection while
   * having a background thread running that is forcing close of the connection to try and
   * provoke a close catastrophe; we are hoping for a car crash so we can see if we are leaking
   * zk connections.
   * @throws Exception
   */
  @Ignore ("Flakey test: See HBASE-8996")@Test
  public void testDeleteForZKConnLeak() throws Exception {
    TEST_UTIL.createTable(TABLE_NAME4, FAM_NAM);
    final Configuration config = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    config.setInt("zookeeper.recovery.retry", 1);
    config.setInt("zookeeper.recovery.retry.intervalmill", 1000);
    config.setInt("hbase.rpc.timeout", 2000);
    config.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 10,
      5, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>(),
      Threads.newDaemonThreadFactory("test-hcm-delete"));

    pool.submit(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          try {
            HConnection conn = HConnectionManager.getConnection(config);
            LOG.info("Connection " + conn);
            HConnectionManager.deleteStaleConnection(conn);
            LOG.info("Connection closed " + conn);
            // TODO: This sleep time should be less than the time that it takes to open and close
            // a table.  Ideally we would do a few runs first to measure.  For now this is
            // timing based; hopefully we hit the bad condition.
            Threads.sleep(10);
          } catch (Exception e) {
          }
        }
      }
    });

    // Use connection multiple times.
    for (int i = 0; i < 30; i++) {
      Connection c1 = null;
      try {
        c1 = ConnectionManager.getConnectionInternal(config);
        LOG.info("HTable connection " + i + " " + c1);
        Table table = new HTable(config, TABLE_NAME4, pool);
        table.close();
        LOG.info("HTable connection " + i + " closed " + c1);
      } catch (Exception e) {
        LOG.info("We actually want this to happen!!!!  So we can see if we are leaking zk", e);
      } finally {
        if (c1 != null) {
          if (c1.isClosed()) {
            // cannot use getZooKeeper as method instantiates watcher if null
            Field zkwField = c1.getClass().getDeclaredField("keepAliveZookeeper");
            zkwField.setAccessible(true);
            Object watcher = zkwField.get(c1);

            if (watcher != null) {
              if (((ZooKeeperWatcher)watcher).getRecoverableZooKeeper().getState().isAlive()) {
                // non-synchronized access to watcher; sleep and check again in case zk connection
                // hasn't been cleaned up yet.
                Thread.sleep(1000);
                if (((ZooKeeperWatcher) watcher).getRecoverableZooKeeper().getState().isAlive()) {
                  pool.shutdownNow();
                  fail("Live zookeeper in closed connection");
                }
              }
            }
          }
          c1.close();
        }
      }
    }
    pool.shutdownNow();
  }

  @Test
  public void testConnectionRideOverClusterRestart() throws IOException, InterruptedException {
    Configuration config = new Configuration(TEST_UTIL.getConfiguration());

    TableName tableName = TableName.valueOf("testConnectionRideOverClusterRestart");
    TEST_UTIL.createTable(tableName.getName(), new byte[][] {FAM_NAM}, config).close();

    Connection connection = ConnectionFactory.createConnection(config);
    Table table = connection.getTable(tableName);

    // this will cache the meta location and table's region location
    table.get(new Get(Bytes.toBytes("foo")));

    // restart HBase
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.restartHBaseCluster(2);
    // this should be able to discover new locations for meta and table's region
    table.get(new Get(Bytes.toBytes("foo")));
    TEST_UTIL.deleteTable(tableName);
    table.close();
    connection.close();
  }
}
