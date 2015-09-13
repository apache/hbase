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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.test.LoadTestKVGenerator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, ClientTests.class})
public class TestFastFail {
  private static final Log LOG = LogFactory.getLog(TestFastFail.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final Random random = new Random();
  private static int SLAVES = 3;
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
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
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
    TEST_UTIL.getHBaseCluster().getRegionServer(0).getRpcServer().stop();
    TEST_UTIL.getHBaseCluster().getRegionServer(0).stop("Testing");

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
     * assertTrue(
        "There should be atleast one thread that retried instead of failing",
        MyPreemptiveFastFailInterceptor.numBraveSouls.get() > 0);*/
    assertTrue(
        "There should be atleast one PreemptiveFastFail exception,"
            + " otherwise, the test makes little sense."
            + "numPreemptiveFastFailExceptions: "
            + numPreemptiveFastFailExceptions.get(),
        numPreemptiveFastFailExceptions.get() > 0);
    assertTrue(
        "Only few thread should ideally be waiting for the dead "
            + "regionserver to be coming back. numBlockedWorkers:"
            + numBlockedWorkers.get() + " threads that retried : "
            + MyPreemptiveFastFailInterceptor.numBraveSouls.get(),
        numBlockedWorkers.get() <= MyPreemptiveFastFailInterceptor.numBraveSouls
            .get());
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
}
