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

package org.apache.hadoop.hbase.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.IntegrationTestIngest;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.factories.MonkeyFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.MultiThreadedReader;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * An IntegrationTest for doing reads with a timeout, to a read-only table with region
 * replicas. ChaosMonkey is run which kills the region servers and master, but ensures
 * that meta region server is not killed, and at most 2 region servers are dead at any point
 * in time. The expected behavior is that all reads with stale mode true will return
 * before the timeout (5 sec by default). The test fails if the read requests does not finish
 * in time.
 *
 * <p> This test uses LoadTestTool to read and write the data from a single client but
 * multiple threads. The data is written first, then we allow the region replicas to catch
 * up. Then we start the reader threads doing get requests with stale mode true. Chaos Monkey is
 * started after some delay (20 sec by default) after the reader threads are started so that
 * there is enough time to fully cache meta.
 *
 * These parameters (and some other parameters from LoadTestTool) can be used to
 * control behavior, given values are default:
 * <pre>
 * -Dhbase.DIntegrationTestTimeBoundedRequestsWithRegionReplicas.runtime=600000
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_regions_per_server=5
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.get_timeout_ms=5000
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_keys_per_server=2500
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.region_replication=3
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_read_threads=20
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_write_threads=20
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_regions_per_server=5
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.chaos_monkey_delay=20000
 * </pre>
 * Use this test with "serverKilling" ChaosMonkey. Sample usage:
 * <pre>
 * hbase org.apache.hadoop.hbase.test.IntegrationTestTimeBoundedRequestsWithRegionReplicas
 * -Dhbase.IntegrationTestTimeBoundedRequestsWithRegionReplicas.runtime=600000
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_write_threads=40
 * -DIntegrationTestTimeBoundedRequestsWithRegionReplicas.num_read_threads=40
 * -Dhbase.ipc.client.allowsInterrupt=true --monkey serverKilling
 * </pre>
 */
@Category(IntegrationTests.class)
public class IntegrationTestTimeBoundedRequestsWithRegionReplicas extends IntegrationTestIngest {

  private static final Log LOG = LogFactory.getLog(
    IntegrationTestTimeBoundedRequestsWithRegionReplicas.class);

  private static final String TEST_NAME
    = IntegrationTestTimeBoundedRequestsWithRegionReplicas.class.getSimpleName();

  protected static final long DEFAULT_GET_TIMEOUT = 5000; // 5 sec
  protected static final String GET_TIMEOUT_KEY = "get_timeout_ms";

  protected static final long DEFAUL_CHAOS_MONKEY_DELAY = 20 * 1000; // 20 sec
  protected static final String CHAOS_MONKEY_DELAY_KEY = "chaos_monkey_delay";

  protected static final int DEFAULT_REGION_REPLICATION = 3;

  @Override
  protected void startMonkey() throws Exception {
    // we do not want to start the monkey at the start of the test.
  }

  @Override
  protected MonkeyFactory getDefaultMonkeyFactory() {
    return MonkeyFactory.getFactory(MonkeyFactory.CALM);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    // default replication for this test is 3
    String clazz = this.getClass().getSimpleName();
    conf.setIfUnset(String.format("%s.%s", clazz, LoadTestTool.OPT_REGION_REPLICATION),
      Integer.toString(DEFAULT_REGION_REPLICATION));
  }

  protected void writeData(int colsPerKey, int recordSize, int writeThreads,
      long startKey, long numKeys) throws IOException {
    int ret = loadTool.run(getArgsForLoadTestTool("-write",
      String.format("%d:%d:%d", colsPerKey, recordSize, writeThreads), startKey, numKeys));
    if (0 != ret) {
      String errorMsg = "Load failed with error code " + ret;
      LOG.error(errorMsg);
      Assert.fail(errorMsg);
    }
  }

  @Override
  protected void runIngestTest(long defaultRunTime, long keysPerServerPerIter, int colsPerKey,
      int recordSize, int writeThreads, int readThreads) throws Exception {
    LOG.info("Cluster size:"+
      util.getHBaseClusterInterface().getClusterStatus().getServersSize());

    long start = System.currentTimeMillis();
    String runtimeKey = String.format(RUN_TIME_KEY, this.getClass().getSimpleName());
    long runtime = util.getConfiguration().getLong(runtimeKey, defaultRunTime);
    long startKey = 0;

    long numKeys = getNumKeys(keysPerServerPerIter);


    // write data once
    LOG.info("Writing some data to the table");
    writeData(colsPerKey, recordSize, writeThreads, startKey, numKeys);

    // flush the table
    LOG.info("Flushing the table");
    Admin admin = util.getHBaseAdmin();
    admin.flush(getTablename());

    // re-open the regions to make sure that the replicas are up to date
    long refreshTime = conf.getLong(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 0);
    if (refreshTime > 0 && refreshTime <= 10000) {
      LOG.info("Sleeping " + refreshTime + "ms to ensure that the data is replicated");
      Threads.sleep(refreshTime*3);
    } else {
      LOG.info("Reopening the table");
      admin.disableTable(getTablename());
      admin.enableTable(getTablename());
    }

    // We should only start the ChaosMonkey after the readers are started and have cached
    // all of the region locations. Because the meta is not replicated, the timebounded reads
    // will timeout if meta server is killed.
    // We will start the chaos monkey after 1 minute, and since the readers are reading random
    // keys, it should be enough to cache every region entry.
    long chaosMonkeyDelay = conf.getLong(String.format("%s.%s", TEST_NAME, CHAOS_MONKEY_DELAY_KEY)
      , DEFAUL_CHAOS_MONKEY_DELAY);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    LOG.info(String.format("ChaosMonkey delay is : %d seconds. Will start %s " +
        "ChaosMonkey after delay", chaosMonkeyDelay / 1000, monkeyToUse));
    ScheduledFuture<?> result = executorService.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting ChaosMonkey");
          monkey.start();
          monkey.waitForStop();
        } catch (Exception e) {
          LOG.warn(StringUtils.stringifyException(e));
        }

      }
    }, chaosMonkeyDelay, TimeUnit.MILLISECONDS);

    // set the intended run time for the reader. The reader will do read requests
    // to random keys for this amount of time.
    long remainingTime = runtime - (System.currentTimeMillis() - start);
    LOG.info("Reading random keys from the table for " + remainingTime/60000 + " min");
    this.conf.setLong(
      String.format(RUN_TIME_KEY, TimeBoundedMultiThreadedReader.class.getSimpleName())
      , remainingTime); // load tool shares the same conf

    // now start the readers which will run for configured run time
    try {
      int ret = loadTool.run(getArgsForLoadTestTool("-read", String.format("100:%d", readThreads)
        , startKey, numKeys));
      if (0 != ret) {
        String errorMsg = "Verification failed with error code " + ret;
        LOG.error(errorMsg);
        Assert.fail(errorMsg);
      }
    } finally {
      if (result != null) result.cancel(false);
      monkey.stop("Stopping the test");
      monkey.waitForStop();
      executorService.shutdown();
    }
  }

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    List<String> args = Lists.newArrayList(super.getArgsForLoadTestTool(
      mode, modeSpecificArg, startKey, numKeys));
    args.add("-reader");
    args.add(TimeBoundedMultiThreadedReader.class.getName());
    return args.toArray(new String[args.size()]);
  }

  public static class TimeBoundedMultiThreadedReader extends MultiThreadedReader {
    protected long timeoutNano;
    protected AtomicLong timedOutReads = new AtomicLong();
    protected long runTime;
    protected Thread timeoutThread;
    protected AtomicLong staleReads = new AtomicLong();

    public TimeBoundedMultiThreadedReader(LoadTestDataGenerator dataGen, Configuration conf,
        TableName tableName, double verifyPercent) throws IOException {
      super(dataGen, conf, tableName, verifyPercent);
      long timeoutMs = conf.getLong(
        String.format("%s.%s", TEST_NAME, GET_TIMEOUT_KEY), DEFAULT_GET_TIMEOUT);
      timeoutNano = timeoutMs * 1000000;
      LOG.info("Timeout for gets: " + timeoutMs);
      String runTimeKey = String.format(RUN_TIME_KEY, this.getClass().getSimpleName());
      this.runTime = conf.getLong(runTimeKey, -1);
      if (this.runTime <= 0) {
        throw new IllegalArgumentException("Please configure " + runTimeKey);
      }
    }

    @Override
    public void waitForFinish() {
      try {
        this.timeoutThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      this.aborted = true;
      super.waitForFinish();
    }

    @Override
    protected String progressInfo() {
      StringBuilder builder = new StringBuilder(super.progressInfo());
      appendToStatus(builder, "stale_reads", staleReads.get());
      appendToStatus(builder, "get_timeouts", timedOutReads.get());
      return builder.toString();
    }

    @Override
    public void start(long startKey, long endKey, int numThreads) throws IOException {
      super.start(startKey, endKey, numThreads);
      this.timeoutThread = new TimeoutThread(this.runTime);
      this.timeoutThread.start();
    }

    @Override
    protected HBaseReaderThread createReaderThread(int readerId) throws IOException {
      return new TimeBoundedMultiThreadedReaderThread(readerId);
    }

    private class TimeoutThread extends Thread {
      long timeout;
      long reportInterval = 60000;
      public TimeoutThread(long timeout) {
        this.timeout = timeout;
      }

      @Override
      public void run() {
        while (true) {
          long rem = Math.min(timeout, reportInterval);
          if (rem <= 0) {
            break;
          }
          LOG.info("Remaining execution time:" + timeout / 60000 + " min");
          Threads.sleep(rem);
          timeout -= rem;
        }
      }
    }

    public class TimeBoundedMultiThreadedReaderThread
      extends MultiThreadedReader.HBaseReaderThread {

      public TimeBoundedMultiThreadedReaderThread(int readerId) throws IOException {
        super(readerId);
      }

      @Override
      protected Get createGet(long keyToRead) throws IOException {
        Get get = super.createGet(keyToRead);
        get.setConsistency(Consistency.TIMELINE);
        return get;
      }

      @Override
      protected long getNextKeyToRead() {
        // always read a random key, assuming that the writer has finished writing all keys
        long key = startKey + Math.abs(RandomUtils.nextLong())
            % (endKey - startKey);
        return key;
      }

      @Override
      protected void verifyResultsAndUpdateMetrics(boolean verify, Get[] gets, long elapsedNano,
          Result[] results, Table table, boolean isNullExpected)
          throws IOException {
        super.verifyResultsAndUpdateMetrics(verify, gets, elapsedNano, results, table, isNullExpected);
        for (Result r : results) {
          if (r.isStale()) staleReads.incrementAndGet();
        }
        // we actually do not timeout and cancel the reads after timeout. We just wait for the RPC
        // to complete, but if the request took longer than timeout, we treat that as error.
        if (elapsedNano > timeoutNano) {
          timedOutReads.incrementAndGet();
          numReadFailures.addAndGet(1); // fail the test
          for (Result r : results) {
            LOG.error("FAILED FOR " + r);
            RegionLocations rl = ((ClusterConnection)connection).
                locateRegion(tableName, r.getRow(), true, true);
            HRegionLocation locations[] = rl.getRegionLocations();
            for (HRegionLocation h : locations) {
              LOG.error("LOCATION " + h);
            }
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestTimeBoundedRequestsWithRegionReplicas(), args);
    System.exit(ret);
  }
}
