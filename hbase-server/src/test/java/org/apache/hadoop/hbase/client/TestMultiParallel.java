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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class, FlakeyTests.class})
public class TestMultiParallel {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiParallel.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiParallel.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");
  private static final String FAMILY = "family";
  private static final TableName TEST_TABLE = TableName.valueOf("multi_test_table");
  private static final byte[] BYTES_FAMILY = Bytes.toBytes(FAMILY);
  private static final byte[] ONE_ROW = Bytes.toBytes("xxx");
  private static final byte [][] KEYS = makeKeys();

  private static final int slaves = 5; // also used for testing HTable pool size
  private static Connection CONNECTION;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    UTIL.getConfiguration().set(HConstants.RPC_CODEC_CONF_KEY,
        KeyValueCodec.class.getCanonicalName());
    // Disable table on master for now as the feature is broken
    //UTIL.getConfiguration().setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    // We used to ask for system tables on Master exclusively but not needed by test and doesn't
    // work anyways -- so commented out.
    // UTIL.getConfiguration().setBoolean(LoadBalancer.SYSTEM_TABLES_ON_MASTER, true);
    UTIL.getConfiguration()
        .set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, MyMasterObserver.class.getName());
    UTIL.startMiniCluster(slaves);
    Table t = UTIL.createMultiRegionTable(TEST_TABLE, Bytes.toBytes(FAMILY));
    UTIL.waitTableEnabled(TEST_TABLE);
    t.close();
    CONNECTION = ConnectionFactory.createConnection(UTIL.getConfiguration());
    assertTrue(MyMasterObserver.start.get());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    CONNECTION.close();
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    final int balanceCount = MyMasterObserver.postBalanceCount.get();
    LOG.info("before");
    if (UTIL.ensureSomeRegionServersAvailable(slaves)) {
      // Distribute regions
      UTIL.getMiniHBaseCluster().getMaster().balance();
      // Some plans are created.
      if (MyMasterObserver.postBalanceCount.get() > balanceCount) {
        // It is necessary to wait the move procedure to start.
        // Otherwise, the next wait may pass immediately.
        UTIL.waitFor(3 * 1000, 100, false, () ->
            UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().hasRegionsInTransition()
        );
      }

      // Wait until completing balance
      UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);
    }
    LOG.info("before done");
  }

  private static byte[][] makeKeys() {
    byte [][] starterKeys = HBaseTestingUtility.KEYS;
    // Create a "non-uniform" test set with the following characteristics:
    // a) Unequal number of keys per region

    // Don't use integer as a multiple, so that we have a number of keys that is
    // not a multiple of the number of regions
    int numKeys = (int) (starterKeys.length * 10.33F);

    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      int kIdx = i % starterKeys.length;
      byte[] k = starterKeys[kIdx];
      byte[] cp = new byte[k.length + 1];
      System.arraycopy(k, 0, cp, 0, k.length);
      cp[k.length] = new Integer(i % 256).byteValue();
      keys.add(cp);
    }

    // b) Same duplicate keys (showing multiple Gets/Puts to the same row, which
    // should work)
    // c) keys are not in sorted order (within a region), to ensure that the
    // sorting code and index mapping doesn't break the functionality
    for (int i = 0; i < 100; i++) {
      int kIdx = i % starterKeys.length;
      byte[] k = starterKeys[kIdx];
      byte[] cp = new byte[k.length + 1];
      System.arraycopy(k, 0, cp, 0, k.length);
      cp[k.length] = new Integer(i % 256).byteValue();
      keys.add(cp);
    }
    return keys.toArray(new byte [][] {new byte [] {}});
  }


  /**
   * This is for testing the active number of threads that were used while
   * doing a batch operation. It inserts one row per region via the batch
   * operation, and then checks the number of active threads.
   * <p/>
   * For HBASE-3553
   */
  @Test
  public void testActiveThreadsCount() throws Exception {
    UTIL.getConfiguration().setLong("hbase.htable.threads.coresize", slaves + 1);
    // Make sure max is at least as big as coresize; can be smaller in test context where
    // we tune down thread sizes -- max could be < slaves + 1.
    UTIL.getConfiguration().setLong("hbase.htable.threads.max", slaves + 1);
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration())) {
      ThreadPoolExecutor executor = HTable.getDefaultExecutor(UTIL.getConfiguration());
      try {
        try (Table t = connection.getTable(TEST_TABLE, executor)) {
          List<Put> puts = constructPutRequests(); // creates a Put for every region
          t.batch(puts, null);
          HashSet<ServerName> regionservers = new HashSet<>();
          try (RegionLocator locator = connection.getRegionLocator(TEST_TABLE)) {
            for (Row r : puts) {
              HRegionLocation location = locator.getRegionLocation(r.getRow());
              regionservers.add(location.getServerName());
            }
          }
          assertEquals(regionservers.size(), executor.getLargestPoolSize());
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void testBatchWithGet() throws Exception {
    LOG.info("test=testBatchWithGet");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    // load test data
    List<Put> puts = constructPutRequests();
    table.batch(puts, null);

    // create a list of gets and run it
    List<Row> gets = new ArrayList<>();
    for (byte[] k : KEYS) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      gets.add(get);
    }
    Result[] multiRes = new Result[gets.size()];
    table.batch(gets, multiRes);

    // Same gets using individual call API
    List<Result> singleRes = new ArrayList<>();
    for (Row get : gets) {
      singleRes.add(table.get((Get) get));
    }
    // Compare results
    Assert.assertEquals(singleRes.size(), multiRes.length);
    for (int i = 0; i < singleRes.size(); i++) {
      Assert.assertTrue(singleRes.get(i).containsColumn(BYTES_FAMILY, QUALIFIER));
      Cell[] singleKvs = singleRes.get(i).rawCells();
      Cell[] multiKvs = multiRes[i].rawCells();
      for (int j = 0; j < singleKvs.length; j++) {
        Assert.assertEquals(singleKvs[j], multiKvs[j]);
        Assert.assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(singleKvs[j]),
            CellUtil.cloneValue(multiKvs[j])));
      }
    }
    table.close();
  }

  @Test
  public void testBadFam() throws Exception {
    LOG.info("test=testBadFam");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    List<Row> actions = new ArrayList<>();
    Put p = new Put(Bytes.toBytes("row1"));
    p.addColumn(Bytes.toBytes("bad_family"), Bytes.toBytes("qual"), Bytes.toBytes("value"));
    actions.add(p);
    p = new Put(Bytes.toBytes("row2"));
    p.addColumn(BYTES_FAMILY, Bytes.toBytes("qual"), Bytes.toBytes("value"));
    actions.add(p);

    // row1 and row2 should be in the same region.

    Object [] r = new Object[actions.size()];
    try {
      table.batch(actions, r);
      fail();
    } catch (RetriesExhaustedWithDetailsException ex) {
      LOG.debug(ex.toString(), ex);
      // good!
      assertFalse(ex.mayHaveClusterIssues());
    }
    assertEquals(2, r.length);
    assertTrue(r[0] instanceof Throwable);
    assertTrue(r[1] instanceof Result);
    table.close();
  }

  @Test
  public void testFlushCommitsNoAbort() throws Exception {
    LOG.info("test=testFlushCommitsNoAbort");
    doTestFlushCommits(false);
  }

  /**
   * Only run one Multi test with a forced RegionServer abort. Otherwise, the
   * unit tests will take an unnecessarily long time to run.
   */
  @Test
  public void testFlushCommitsWithAbort() throws Exception {
    LOG.info("test=testFlushCommitsWithAbort");
    doTestFlushCommits(true);
  }

  /**
   * Set table auto flush to false and test flushing commits
   * @param doAbort true if abort one regionserver in the testing
   */
  private void doTestFlushCommits(boolean doAbort) throws Exception {
    // Load the data
    LOG.info("get new table");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    LOG.info("constructPutRequests");
    List<Put> puts = constructPutRequests();
    table.put(puts);
    LOG.info("puts");
    final int liveRScount = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()
        .size();
    assert liveRScount > 0;
    JVMClusterUtil.RegionServerThread liveRS = UTIL.getMiniHBaseCluster()
        .getLiveRegionServerThreads().get(0);
    if (doAbort) {
      liveRS.getRegionServer().abort("Aborting for tests",
          new Exception("doTestFlushCommits"));
      // If we wait for no regions being online after we abort the server, we
      // could ensure the master has re-assigned the regions on killed server
      // after writing successfully. It means the server we aborted is dead
      // and detected by matser
      while (liveRS.getRegionServer().getNumberOfOnlineRegions() != 0) {
        Thread.sleep(100);
      }
      // try putting more keys after the abort. same key/qual... just validating
      // no exceptions thrown
      puts = constructPutRequests();
      table.put(puts);
    }

    LOG.info("validating loaded data");
    validateLoadedData(table);

    // Validate server and region count
    List<JVMClusterUtil.RegionServerThread> liveRSs = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();
    int count = 0;
    for (JVMClusterUtil.RegionServerThread t: liveRSs) {
      count++;
      LOG.info("Count=" + count + ", Alive=" + t.getRegionServer());
    }
    LOG.info("Count=" + count);
    Assert.assertEquals("Server count=" + count + ", abort=" + doAbort,
        (doAbort ? (liveRScount - 1) : liveRScount), count);
    if (doAbort) {
      UTIL.getMiniHBaseCluster().waitOnRegionServer(0);
      UTIL.waitFor(15 * 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          // We disable regions on master so the count should be liveRScount - 1
          return UTIL.getMiniHBaseCluster().getMaster()
              .getClusterMetrics().getLiveServerMetrics().size() == liveRScount - 1;
        }
      });
      UTIL.waitFor(15 * 1000, UTIL.predicateNoRegionsInTransition());
    }

    table.close();
    LOG.info("done");
  }

  @Test
  public void testBatchWithPut() throws Exception {
    LOG.info("test=testBatchWithPut");
    Table table = CONNECTION.getTable(TEST_TABLE);
    // put multiple rows using a batch
    List<Put> puts = constructPutRequests();

    Object[] results = new Object[puts.size()];
    table.batch(puts, results);
    validateSizeAndEmpty(results, KEYS.length);

    if (true) {
      int liveRScount = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size();
      assert liveRScount > 0;
      JVMClusterUtil.RegionServerThread liveRS =
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(0);
      liveRS.getRegionServer().abort("Aborting for tests", new Exception("testBatchWithPut"));
      puts = constructPutRequests();
      try {
        results = new Object[puts.size()];
        table.batch(puts, results);
      } catch (RetriesExhaustedWithDetailsException ree) {
        LOG.info(ree.getExhaustiveDescription());
        table.close();
        throw ree;
      }
      validateSizeAndEmpty(results, KEYS.length);
    }

    validateLoadedData(table);
    table.close();
  }

  @Test
  public void testBatchWithDelete() throws Exception {
    LOG.info("test=testBatchWithDelete");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    // Load some data
    List<Put> puts = constructPutRequests();
    Object[] results = new Object[puts.size()];
    table.batch(puts, results);
    validateSizeAndEmpty(results, KEYS.length);

    // Deletes
    List<Row> deletes = new ArrayList<>();
    for (int i = 0; i < KEYS.length; i++) {
      Delete delete = new Delete(KEYS[i]);
      delete.addFamily(BYTES_FAMILY);
      deletes.add(delete);
    }
    results= new Object[deletes.size()];
    table.batch(deletes, results);
    validateSizeAndEmpty(results, KEYS.length);

    // Get to make sure ...
    for (byte[] k : KEYS) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      Assert.assertFalse(table.exists(get));
    }
    table.close();
  }

  @Test
  public void testHTableDeleteWithList() throws Exception {
    LOG.info("test=testHTableDeleteWithList");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    // Load some data
    List<Put> puts = constructPutRequests();
    Object[] results = new Object[puts.size()];
    table.batch(puts, results);
    validateSizeAndEmpty(results, KEYS.length);

    // Deletes
    ArrayList<Delete> deletes = new ArrayList<>();
    for (int i = 0; i < KEYS.length; i++) {
      Delete delete = new Delete(KEYS[i]);
      delete.addFamily(BYTES_FAMILY);
      deletes.add(delete);
    }
    table.delete(deletes);
    Assert.assertTrue(deletes.isEmpty());

    // Get to make sure ...
    for (byte[] k : KEYS) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      Assert.assertFalse(table.exists(get));
    }
    table.close();
  }

  @Test
  public void testBatchWithManyColsInOneRowGetAndPut() throws Exception {
    LOG.info("test=testBatchWithManyColsInOneRowGetAndPut");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    List<Row> puts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Put put = new Put(ONE_ROW);
      byte[] qual = Bytes.toBytes("column" + i);
      put.addColumn(BYTES_FAMILY, qual, VALUE);
      puts.add(put);
    }
    Object[] results = new Object[puts.size()];
    table.batch(puts, results);

    // validate
    validateSizeAndEmpty(results, 100);

    // get the data back and validate that it is correct
    List<Row> gets = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Get get = new Get(ONE_ROW);
      byte[] qual = Bytes.toBytes("column" + i);
      get.addColumn(BYTES_FAMILY, qual);
      gets.add(get);
    }

    Object[] multiRes = new Object[gets.size()];
    table.batch(gets, multiRes);

    int idx = 0;
    for (Object r : multiRes) {
      byte[] qual = Bytes.toBytes("column" + idx);
      validateResult(r, qual, VALUE);
      idx++;
    }
    table.close();
  }

  @Test
  public void testBatchWithIncrementAndAppend() throws Exception {
    LOG.info("test=testBatchWithIncrementAndAppend");
    final byte[] QUAL1 = Bytes.toBytes("qual1");
    final byte[] QUAL2 = Bytes.toBytes("qual2");
    final byte[] QUAL3 = Bytes.toBytes("qual3");
    final byte[] QUAL4 = Bytes.toBytes("qual4");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);
    Delete d = new Delete(ONE_ROW);
    table.delete(d);
    Put put = new Put(ONE_ROW);
    put.addColumn(BYTES_FAMILY, QUAL1, Bytes.toBytes("abc"));
    put.addColumn(BYTES_FAMILY, QUAL2, Bytes.toBytes(1L));
    table.put(put);

    Increment inc = new Increment(ONE_ROW);
    inc.addColumn(BYTES_FAMILY, QUAL2, 1);
    inc.addColumn(BYTES_FAMILY, QUAL3, 1);

    Append a = new Append(ONE_ROW);
    a.addColumn(BYTES_FAMILY, QUAL1, Bytes.toBytes("def"));
    a.addColumn(BYTES_FAMILY, QUAL4, Bytes.toBytes("xyz"));
    List<Row> actions = new ArrayList<>();
    actions.add(inc);
    actions.add(a);

    Object[] multiRes = new Object[actions.size()];
    table.batch(actions, multiRes);
    validateResult(multiRes[1], QUAL1, Bytes.toBytes("abcdef"));
    validateResult(multiRes[1], QUAL4, Bytes.toBytes("xyz"));
    validateResult(multiRes[0], QUAL2, Bytes.toBytes(2L));
    validateResult(multiRes[0], QUAL3, Bytes.toBytes(1L));
    table.close();
  }

  @Test
  public void testNonceCollision() throws Exception {
    LOG.info("test=testNonceCollision");
    final Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
    Table table = connection.getTable(TEST_TABLE);
    Put put = new Put(ONE_ROW);
    put.addColumn(BYTES_FAMILY, QUALIFIER, Bytes.toBytes(0L));

    // Replace nonce manager with the one that returns each nonce twice.
    NonceGenerator cnm = new NonceGenerator() {

      private final PerClientRandomNonceGenerator delegate = PerClientRandomNonceGenerator.get();

      private long lastNonce = -1;

      @Override
      public synchronized long newNonce() {
        long nonce = 0;
        if (lastNonce == -1) {
          lastNonce = nonce = delegate.newNonce();
        } else {
          nonce = lastNonce;
          lastNonce = -1L;
        }
        return nonce;
      }

      @Override
      public long getNonceGroup() {
        return delegate.getNonceGroup();
      }
    };

    NonceGenerator oldCnm =
      ConnectionUtils.injectNonceGeneratorForTesting((ClusterConnection)connection, cnm);

    // First test sequential requests.
    try {
      Increment inc = new Increment(ONE_ROW);
      inc.addColumn(BYTES_FAMILY, QUALIFIER, 1L);
      table.increment(inc);

      // duplicate increment
      inc = new Increment(ONE_ROW);
      inc.addColumn(BYTES_FAMILY, QUALIFIER, 1L);
      Result result = table.increment(inc);
      validateResult(result, QUALIFIER, Bytes.toBytes(1L));

      Get get = new Get(ONE_ROW);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      result = table.get(get);
      validateResult(result, QUALIFIER, Bytes.toBytes(1L));

      // Now run a bunch of requests in parallel, exactly half should succeed.
      int numRequests = 40;
      final CountDownLatch startedLatch = new CountDownLatch(numRequests);
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(numRequests);
      for (int i = 0; i < numRequests; ++i) {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            Table table = null;
            try {
              table = connection.getTable(TEST_TABLE);
            } catch (IOException e) {
              fail("Not expected");
            }
            Increment inc = new Increment(ONE_ROW);
            inc.addColumn(BYTES_FAMILY, QUALIFIER, 1L);
            startedLatch.countDown();
            try {
              startLatch.await();
            } catch (InterruptedException e) {
              fail("Not expected");
            }
            try {
              table.increment(inc);
            } catch (IOException ioEx) {
              fail("Not expected");
            }
            doneLatch.countDown();
          }
        };
        Threads.setDaemonThreadRunning(new Thread(r));
      }
      startedLatch.await(); // Wait until all threads are ready...
      startLatch.countDown(); // ...and unleash the herd!
      doneLatch.await();
      // Now verify
      get = new Get(ONE_ROW);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      result = table.get(get);
      validateResult(result, QUALIFIER, Bytes.toBytes((numRequests / 2) + 1L));
      table.close();
    } finally {
      ConnectionImplementation.injectNonceGeneratorForTesting((ClusterConnection) connection, oldCnm);
    }
  }

  @Test
  public void testBatchWithMixedActions() throws Exception {
    LOG.info("test=testBatchWithMixedActions");
    Table table = UTIL.getConnection().getTable(TEST_TABLE);

    // Load some data to start
    List<Put> puts = constructPutRequests();
    Object[] results = new Object[puts.size()];
    table.batch(puts, results);
    validateSizeAndEmpty(results, KEYS.length);

    // Batch: get, get, put(new col), delete, get, get of put, get of deleted,
    // put
    List<Row> actions = new ArrayList<>();

    byte[] qual2 = Bytes.toBytes("qual2");
    byte[] val2 = Bytes.toBytes("putvalue2");

    // 0 get
    Get get = new Get(KEYS[10]);
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // 1 get
    get = new Get(KEYS[11]);
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // 2 put of new column
    Put put = new Put(KEYS[10]);
    put.addColumn(BYTES_FAMILY, qual2, val2);
    actions.add(put);

    // 3 delete
    Delete delete = new Delete(KEYS[20]);
    delete.addFamily(BYTES_FAMILY);
    actions.add(delete);

    // 4 get
    get = new Get(KEYS[30]);
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // There used to be a 'get' of a previous put here, but removed
    // since this API really cannot guarantee order in terms of mixed
    // get/puts.

    // 5 put of new column
    put = new Put(KEYS[40]);
    put.addColumn(BYTES_FAMILY, qual2, val2);
    actions.add(put);

    // 6 RowMutations
    RowMutations rm = new RowMutations(KEYS[50]);
    put = new Put(KEYS[50]);
    put.addColumn(BYTES_FAMILY, qual2, val2);
    rm.add((Mutation) put);
    byte[] qual3 = Bytes.toBytes("qual3");
    byte[] val3 = Bytes.toBytes("putvalue3");
    put = new Put(KEYS[50]);
    put.addColumn(BYTES_FAMILY, qual3, val3);
    rm.add((Mutation) put);
    actions.add(rm);

    // 7 Add another Get to the mixed sequence after RowMutations
    get = new Get(KEYS[10]);
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    results = new Object[actions.size()];
    table.batch(actions, results);

    // Validation

    validateResult(results[0]);
    validateResult(results[1]);
    validateEmpty(results[3]);
    validateResult(results[4]);
    validateEmpty(results[5]);
    validateEmpty(results[6]);
    validateResult(results[7]);

    // validate last put, externally from the batch
    get = new Get(KEYS[40]);
    get.addColumn(BYTES_FAMILY, qual2);
    Result r = table.get(get);
    validateResult(r, qual2, val2);

    // validate last RowMutations, externally from the batch
    get = new Get(KEYS[50]);
    get.addColumn(BYTES_FAMILY, qual2);
    r = table.get(get);
    validateResult(r, qual2, val2);

    get = new Get(KEYS[50]);
    get.addColumn(BYTES_FAMILY, qual3);
    r = table.get(get);
    validateResult(r, qual3, val3);

    table.close();
  }

  // // Helper methods ////

  private void validateResult(Object r) {
    validateResult(r, QUALIFIER, VALUE);
  }

  private void validateResult(Object r1, byte[] qual, byte[] val) {
    Result r = (Result)r1;
    Assert.assertTrue(r.containsColumn(BYTES_FAMILY, qual));
    byte[] value = r.getValue(BYTES_FAMILY, qual);
    if (0 != Bytes.compareTo(val, value)) {
      fail("Expected [" + Bytes.toStringBinary(val)
          + "] but got [" + Bytes.toStringBinary(value) + "]");
    }
  }

  private List<Put> constructPutRequests() {
    List<Put> puts = new ArrayList<>();
    for (byte[] k : KEYS) {
      Put put = new Put(k);
      put.addColumn(BYTES_FAMILY, QUALIFIER, VALUE);
      puts.add(put);
    }
    return puts;
  }

  private void validateLoadedData(Table table) throws IOException {
    // get the data back and validate that it is correct
    LOG.info("Validating data on " + table);
    List<Get> gets = new ArrayList<>();
    for (byte[] k : KEYS) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      gets.add(get);
    }
    int retryNum = 10;
    Result[] results = null;
    do  {
      results = table.get(gets);
      boolean finished = true;
      for (Result result : results) {
        if (result.isEmpty()) {
          finished = false;
          break;
        }
      }
      if (finished) {
        break;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
      retryNum--;
    } while (retryNum > 0);

    if (retryNum == 0) {
      fail("Timeout for validate data");
    } else {
      if (results != null) {
        for (Result r : results) {
          Assert.assertTrue(r.containsColumn(BYTES_FAMILY, QUALIFIER));
          Assert.assertEquals(0, Bytes.compareTo(VALUE, r
            .getValue(BYTES_FAMILY, QUALIFIER)));
        }
        LOG.info("Validating data on " + table + " successfully!");
      }
    }
  }

  private void validateEmpty(Object r1) {
    Result result = (Result)r1;
    Assert.assertTrue(result != null);
    Assert.assertTrue(result.isEmpty());
  }

  private void validateSizeAndEmpty(Object[] results, int expectedSize) {
    // Validate got back the same number of Result objects, all empty
    Assert.assertEquals(expectedSize, results.length);
    for (Object result : results) {
      validateEmpty(result);
    }
  }

  public static class MyMasterObserver implements MasterObserver, MasterCoprocessor {
    private static final AtomicInteger postBalanceCount = new AtomicInteger(0);
    private static final AtomicBoolean start = new AtomicBoolean(false);

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      start.set(true);
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void postBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<RegionPlan> plans) throws IOException {
      if (!plans.isEmpty()) {
        postBalanceCount.incrementAndGet();
      }
    }
  }
}
