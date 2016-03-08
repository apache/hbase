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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.ipc.SimpleRpcScheduler;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.test.LoadTestKVGenerator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestFastFail {
  private static final Log LOG = LogFactory.getLog(TestFastFail.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final Random random = new Random();
  private static int SLAVES = 1;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final int SLEEPTIME = 5000;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    MyPreemptiveFastFailInterceptor.numBraveSouls.set(0);
    CallQueueTooBigPffeInterceptor.numCallQueueTooBig.set(0);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Ignore ("Can go zombie -- see HBASE-14421; FIX") @Test
  public void testFastFail() throws IOException, InterruptedException {
    Admin admin = TEST_UTIL.getHBaseAdmin();

    final String tableName = "testClientRelearningExperiment";
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes
        .toBytes(tableName)));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc, Bytes.toBytes("aaaa"), Bytes.toBytes("zzzz"), 32);
    final long numRows = 1000;

    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, SLEEPTIME * 100);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, SLEEPTIME / 10);
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS, 0);
    conf.setClass(HConstants.HBASE_CLIENT_FAST_FAIL_INTERCEPTOR_IMPL,
        MyPreemptiveFastFailInterceptor.class,
        PreemptiveFastFailInterceptor.class);

    final Connection connection = ConnectionFactory.createConnection(conf);

    /**
     * Write numRows worth of data, so that the workers can arbitrarily read.
     */
    List<Put> puts = new ArrayList<>();
    for (long i = 0; i < numRows; i++) {
      byte[] rowKey = longToByteArrayKey(i);
      Put put = new Put(rowKey);
      byte[] value = rowKey; // value is the same as the row key
      put.add(FAMILY, QUALIFIER, value);
      puts.add(put);
    }
    try (Table table = connection.getTable(TableName.valueOf(tableName))) {
      table.put(puts);
      LOG.info("Written all puts.");
    }

    /**
     * The number of threads that are going to perform actions against the test
     * table.
     */
    int nThreads = 100;
    ExecutorService service = Executors.newFixedThreadPool(nThreads);
    final CountDownLatch continueOtherHalf = new CountDownLatch(1);
    final CountDownLatch doneHalfway = new CountDownLatch(nThreads);

    final AtomicInteger numSuccessfullThreads = new AtomicInteger(0);
    final AtomicInteger numFailedThreads = new AtomicInteger(0);

    // The total time taken for the threads to perform the second put;
    final AtomicLong totalTimeTaken = new AtomicLong(0);
    final AtomicInteger numBlockedWorkers = new AtomicInteger(0);
    final AtomicInteger numPreemptiveFastFailExceptions = new AtomicInteger(0);

    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < nThreads; i++) {
      futures.add(service.submit(new Callable<Boolean>() {
        /**
         * The workers are going to perform a couple of reads. The second read
         * will follow the killing of a regionserver so that we make sure that
         * some of threads go into PreemptiveFastFailExcception
         */
        public Boolean call() throws Exception {
          try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Thread.sleep(Math.abs(random.nextInt()) % 250); // Add some jitter here
            byte[] row = longToByteArrayKey(Math.abs(random.nextLong())
                % numRows);
            Get g = new Get(row);
            g.addColumn(FAMILY, QUALIFIER);
            try {
              table.get(g);
            } catch (Exception e) {
              LOG.debug("Get failed : ", e);
              doneHalfway.countDown();
              return false;
            }

            // Done with one get, proceeding to do the next one.
            doneHalfway.countDown();
            continueOtherHalf.await();

            long startTime = System.currentTimeMillis();
            g = new Get(row);
            g.addColumn(FAMILY, QUALIFIER);
            try {
              table.get(g);
              // The get was successful
              numSuccessfullThreads.addAndGet(1);
            } catch (Exception e) {
              if (e instanceof PreemptiveFastFailException) {
                // We were issued a PreemptiveFastFailException
                numPreemptiveFastFailExceptions.addAndGet(1);
              }
              // Irrespective of PFFE, the request failed.
              numFailedThreads.addAndGet(1);
              return false;
            } finally {
              long enTime = System.currentTimeMillis();
              totalTimeTaken.addAndGet(enTime - startTime);
              if ((enTime - startTime) >= SLEEPTIME) {
                // Considering the slow workers as the blockedWorkers.
                // This assumes that the threads go full throttle at performing
                // actions. In case the thread scheduling itself is as slow as
                // SLEEPTIME, then this test might fail and so, we might have
                // set it to a higher number on slower machines.
                numBlockedWorkers.addAndGet(1);
              }
            }
            return true;
          } catch (Exception e) {
            LOG.error("Caught unknown exception", e);
            doneHalfway.countDown();
            return false;
          }
        }
      }));
    }

    doneHalfway.await();

    ClusterStatus status = TEST_UTIL.getHBaseCluster().getClusterStatus();

    // Kill a regionserver
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(i);
      List<Region> regions = server.getOnlineRegions(TableName.META_TABLE_NAME);
      if (regions.size() > 0) continue; // We don't want to kill META table because that adds extra
                                        // latencies which can't be tested very easily.
      server.getRpcServer().stop();
      server.stop("Testing");
    }

    // Let the threads continue going
    continueOtherHalf.countDown();

    Thread.sleep(2 * SLEEPTIME);
    // Restore the cluster
    TEST_UTIL.getHBaseCluster().restoreClusterStatus(status);

    int numThreadsReturnedFalse = 0;
    int numThreadsReturnedTrue = 0;
    int numThreadsThrewExceptions = 0;
    for (Future<Boolean> f : futures) {
      try {
        numThreadsReturnedTrue += f.get() ? 1 : 0;
        numThreadsReturnedFalse += f.get() ? 0 : 1;
      } catch (Exception e) {
        numThreadsThrewExceptions++;
      }
    }
    LOG.debug("numThreadsReturnedFalse:"
        + numThreadsReturnedFalse
        + " numThreadsReturnedTrue:"
        + numThreadsReturnedTrue
        + " numThreadsThrewExceptions:"
        + numThreadsThrewExceptions
        + " numFailedThreads:"
        + numFailedThreads.get()
        + " numSuccessfullThreads:"
        + numSuccessfullThreads.get()
        + " numBlockedWorkers:"
        + numBlockedWorkers.get()
        + " totalTimeWaited: "
        + totalTimeTaken.get()
        / (numBlockedWorkers.get() == 0 ? Long.MAX_VALUE : numBlockedWorkers
            .get()) + " numPFFEs: " + numPreemptiveFastFailExceptions.get());

    assertEquals("The expected number of all the successfull and the failed "
        + "threads should equal the total number of threads that we spawned",
        nThreads, numFailedThreads.get() + numSuccessfullThreads.get());
    assertEquals(
        "All the failures should be coming from the secondput failure",
        numFailedThreads.get(), numThreadsReturnedFalse);
    assertEquals("Number of threads that threw execution exceptions "
        + "otherwise should be 0", numThreadsThrewExceptions, 0);
    assertEquals("The regionservers that returned true should equal to the"
        + " number of successful threads", numThreadsReturnedTrue,
        numSuccessfullThreads.get());
    /* 'should' is not worthy of an assert. Disabling because randomly this seems to randomly
     * not but true. St.Ack 20151012
     *
    assertTrue(
        "There should be atleast one thread that retried instead of failing",
        MyPreemptiveFastFailInterceptor.numBraveSouls.get() > 0);
    assertTrue(
        "There should be atleast one PreemptiveFastFail exception,"
            + " otherwise, the test makes little sense."
            + "numPreemptiveFastFailExceptions: "
            + numPreemptiveFastFailExceptions.get(),
        numPreemptiveFastFailExceptions.get() > 0);
    */
    assertTrue(
        "Only few thread should ideally be waiting for the dead "
            + "regionserver to be coming back. numBlockedWorkers:"
            + numBlockedWorkers.get() + " threads that retried : "
            + MyPreemptiveFastFailInterceptor.numBraveSouls.get(),
        numBlockedWorkers.get() <= MyPreemptiveFastFailInterceptor.numBraveSouls
            .get());
  }

  @Test
  public void testCallQueueTooBigException() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();

    final String tableName = "testCallQueueTooBigException";
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes
      .toBytes(tableName)));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc, Bytes.toBytes("aaaa"), Bytes.toBytes("zzzz"), 3);

    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 100);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 500);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS, 0);
    conf.setClass(HConstants.HBASE_CLIENT_FAST_FAIL_INTERCEPTOR_IMPL,
      CallQueueTooBigPffeInterceptor.class, PreemptiveFastFailInterceptor.class);

    final Connection connection = ConnectionFactory.createConnection(conf);

    //Set max call queues size to 0
    SimpleRpcScheduler srs = (SimpleRpcScheduler)
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getRpcServer().getScheduler();
    Configuration newConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    newConf.setInt("hbase.ipc.server.max.callqueue.length", 0);
    srs.onConfigurationChange(newConf);

    try (Table table = connection.getTable(TableName.valueOf(tableName))) {
      Get get = new Get(new byte[1]);
      table.get(get);
    } catch (Throwable ex) {
    }

    assertEquals("There should have been 1 hit", 1,
      CallQueueTooBigPffeInterceptor.numCallQueueTooBig.get());

    newConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    newConf.setInt("hbase.ipc.server.max.callqueue.length", 250);
    srs.onConfigurationChange(newConf);
  }

  public static class MyPreemptiveFastFailInterceptor extends
      PreemptiveFastFailInterceptor {
    public static AtomicInteger numBraveSouls = new AtomicInteger();

    @Override
    protected boolean shouldRetryInspiteOfFastFail(FailureInfo fInfo) {
      boolean ret = super.shouldRetryInspiteOfFastFail(fInfo);
      if (ret)
        numBraveSouls.addAndGet(1);
      return ret;
    }

    public MyPreemptiveFastFailInterceptor(Configuration conf) {
      super(conf);
    }
  }

  private byte[] longToByteArrayKey(long rowKey) {
    return LoadTestKVGenerator.md5PrefixedKey(rowKey).getBytes();
  }

  public static class CallQueueTooBigPffeInterceptor extends
    PreemptiveFastFailInterceptor {
    public static AtomicInteger numCallQueueTooBig = new AtomicInteger();

    @Override
    protected void handleFailureToServer(ServerName serverName, Throwable t) {
      super.handleFailureToServer(serverName, t);
      numCallQueueTooBig.incrementAndGet();
    }

    public CallQueueTooBigPffeInterceptor(Configuration conf) {
      super(conf);
    }
  }
}
