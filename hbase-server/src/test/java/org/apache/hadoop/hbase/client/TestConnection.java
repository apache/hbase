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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.ResourceLeakDetector;
import org.apache.hbase.thirdparty.io.netty.util.ResourceLeakDetector.Level;

/**
 * This class is for testing {@link Connection}.
 */
@Category({ LargeTests.class })
public class TestConnection {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnection.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestConnection.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");
  private static final int RPC_RETRY = 5;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ResourceLeakDetector.setLevel(Level.PARANOID);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    // Up the handlers; this test needs more than usual.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, RPC_RETRY);
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 3);
    TEST_UTIL.startMiniCluster(2);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.getAdmin().balancerSwitch(true, true);
  }

  /**
   * Naive test to check that Connection#getAdmin returns a properly constructed HBaseAdmin object
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

  /**
   * Test that we can handle connection close: it will trigger a retry, but the calls will finish.
   */
  @Test
  public void testConnectionCloseAllowsInterrupt() throws Exception {
    testConnectionClose(true);
  }

  @Test
  public void testConnectionNotAllowsInterrupt() throws Exception {
    testConnectionClose(false);
  }

  private void testConnectionClose(boolean allowsInterrupt) throws Exception {
    TableName tableName = TableName.valueOf("HCM-testConnectionClose" + allowsInterrupt);
    TEST_UTIL.createTable(tableName, FAM_NAM).close();

    TEST_UTIL.getAdmin().balancerSwitch(false, true);

    Configuration c2 = new Configuration(TEST_UTIL.getConfiguration());
    // We want to work on a separate connection.
    c2.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
    c2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 100); // retry a lot
    c2.setInt(HConstants.HBASE_CLIENT_PAUSE, 1); // don't wait between retries.
    c2.setInt(RpcClient.FAILED_SERVER_EXPIRY_KEY, 0); // Server do not really expire
    c2.setBoolean(RpcClient.SPECIFIC_WRITE_THREAD, allowsInterrupt);
    // to avoid the client to be stuck when do the Get
    c2.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 10000);
    c2.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 10000);
    c2.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5000);

    Connection connection = ConnectionFactory.createConnection(c2);
    final Table table = connection.getTable(tableName);

    Put put = new Put(ROW);
    put.addColumn(FAM_NAM, ROW, ROW);
    table.put(put);

    // 4 steps: ready=0; doGets=1; mustStop=2; stopped=3
    final AtomicInteger step = new AtomicInteger(0);

    final AtomicReference<Throwable> failed = new AtomicReference<>(null);
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
            if (done % 100 == 0) {
              LOG.info("done=" + done);
            }
            // without the sleep, will cause the exception for too many files in
            // org.apache.hadoop.hdfs.server.datanode.DataXceiver
            Thread.sleep(100);
          }
        } catch (Throwable t) {
          failed.set(t);
          LOG.error(t.toString(), t);
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

    ServerName sn;
    try (RegionLocator rl = connection.getRegionLocator(tableName)) {
      sn = rl.getRegionLocation(ROW).getServerName();
    }

    RpcClient rpcClient = ((AsyncConnectionImpl) connection.toAsyncConnection()).rpcClient;

    LOG.info("Going to cancel connections. connection=" + connection.toString() + ", sn=" + sn);
    for (int i = 0; i < 500; i++) {
      rpcClient.cancelConnections(sn);
      Thread.sleep(50);
    }

    step.compareAndSet(1, 2);
    // The test may fail here if the thread doing the gets is stuck. The way to find
    // out what's happening is to look for the thread named 'testConnectionCloseThread'
    TEST_UTIL.waitFor(40000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return step.get() == 3;
      }
    });
    table.close();
    connection.close();
    Assert.assertTrue("Unexpected exception is " + failed.get(), failed.get() == null);
  }

  /**
   * Test that connection can become idle without breaking everything.
   */
  @Test
  public void testConnectionIdle() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, FAM_NAM).close();
    int idleTime = 20000;
    boolean previousBalance = TEST_UTIL.getAdmin().balancerSwitch(false, true);

    Configuration c2 = new Configuration(TEST_UTIL.getConfiguration());
    // We want to work on a separate connection.
    c2.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
    c2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1); // Don't retry: retry = test failed
    c2.setInt(RpcClient.IDLE_TIME, idleTime);

    Connection connection = ConnectionFactory.createConnection(c2);
    final Table table = connection.getTable(tableName);

    Put put = new Put(ROW);
    put.addColumn(FAM_NAM, ROW, ROW);
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
    // fields of RpcClient.
    table.get(new Get(ROW));
    mee.incValue(idleTime + 1000);

    LOG.info("third get - connection is idle, but the reader doesn't know yet");
    // We're testing here a special case:
    // time limit reached BUT connection not yet reclaimed AND a new call.
    // in this situation, we don't close the connection, instead we use it immediately.
    // If we're very unlucky we can have a race condition in the test: the connection is already
    // under closing when we do the get, so we have an exception, and we don't retry as the
    // retry number is 1. The probability is very very low, and seems acceptable for now. It's
    // a test issue only.
    table.get(new Get(ROW));

    LOG.info("we're done - time will change back");

    table.close();

    connection.close();
    EnvironmentEdgeManager.reset();
    TEST_UTIL.getAdmin().balancerSwitch(previousBalance, true);
  }

  @Test
  public void testClosing() throws Exception {
    Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
    configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
      String.valueOf(ThreadLocalRandom.current().nextInt()));

    // as connection caching is going away, now we're just testing
    // that closed connection does actually get closed.

    Connection c1 = ConnectionFactory.createConnection(configuration);
    Connection c2 = ConnectionFactory.createConnection(configuration);
    // no caching, different connections
    assertTrue(c1 != c2);

    // closing independently
    c1.close();
    assertTrue(c1.isClosed());
    assertFalse(c2.isClosed());

    c2.close();
    assertTrue(c2.isClosed());
  }

  /**
   * Trivial test to verify that nobody messes with
   * {@link ConnectionFactory#createConnection(Configuration)}
   */
  @Test
  public void testCreateConnection() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    Connection c1 = ConnectionFactory.createConnection(configuration);
    Connection c2 = ConnectionFactory.createConnection(configuration);
    // created from the same configuration, yet they are different
    assertTrue(c1 != c2);
    assertTrue(c1.getConfiguration() == c2.getConfiguration());
  }

  /*
  ====> With MasterRegistry, connections cannot outlast the masters' lifetime.
  @Test
  public void testConnectionRideOverClusterRestart() throws IOException, InterruptedException {
    Configuration config = new Configuration(TEST_UTIL.getConfiguration());

    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, new byte[][] { FAM_NAM }).close();

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
   */

  @Test
  public void testLocateRegionsWithRegionReplicas() throws IOException {
    int regionReplication = 3;
    byte[] family = Bytes.toBytes("cf");
    TableName tableName = TableName.valueOf(name.getMethodName());

    // Create a table with region replicas
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(regionReplication)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    TEST_UTIL.getAdmin().createTable(builder.build());

    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      RegionLocator locator = conn.getRegionLocator(tableName)) {
      // Get locations of the regions of the table
      List<HRegionLocation> locations = locator.getAllRegionLocations();

      // The size of the returned locations should be 3
      assertEquals(regionReplication, locations.size());

      // The replicaIds of the returned locations should be 0, 1 and 2
      Set<Integer> expectedReplicaIds =
        IntStream.range(0, regionReplication).boxed().collect(Collectors.toSet());
      for (HRegionLocation location : locations) {
        assertTrue(expectedReplicaIds.remove(location.getRegion().getReplicaId()));
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testClosedConnection() throws ServiceException, Throwable {
    byte[] family = Bytes.toBytes("cf");
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName)
      .setCoprocessor(MultiRowMutationEndpoint.class.getName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    TEST_UTIL.getAdmin().createTable(builder.build());

    Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    // cache the location
    try (Table table = conn.getTable(tableName)) {
      table.get(new Get(Bytes.toBytes(0)));
    } finally {
      conn.close();
    }
    Batch.Call<MultiRowMutationService, MutateRowsResponse> callable = service -> {
      throw new RuntimeException("Should not arrive here");
    };
    conn.getTable(tableName).coprocessorService(MultiRowMutationService.class,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, callable);
  }

  // There is no assertion, but you need to confirm that there is no resource leak output from netty
  @Test
  public void testCancelConnectionMemoryLeak() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, FAM_NAM).close();
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table table = connection.getTable(tableName)) {
      table.get(new Get(Bytes.toBytes("1")));
      ServerName sn = TEST_UTIL.getRSForFirstRegionInTable(tableName).getServerName();
      RpcClient rpcClient = ((AsyncConnectionImpl) connection.toAsyncConnection()).rpcClient;
      rpcClient.cancelConnections(sn);
      Thread.sleep(1000);
      System.gc();
      Thread.sleep(1000);
    }
  }
}
