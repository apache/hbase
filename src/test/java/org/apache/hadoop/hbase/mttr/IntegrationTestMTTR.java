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

package org.apache.hadoop.hbase.mttr;

import com.google.common.base.Objects;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * Integration test that should benchmark how fast HBase can recover from failures. This test starts
 * different threads:
 * <ol>
 * <li>
 * Load Test Tool.<br/>
 * This runs so that all RegionServers will have some load and HLogs will be full.
 * </li>
 * <li>
 * Scan thread.<br/>
 * This thread runs a very short scan over and over again recording how log it takes to respond.
 * The longest response is assumed to be the time it took to recover.
 * </li>
 * <li>
 * Put thread.<br/>
 * This thread just like the scan thread except it does a very small put.
 * </li>
 * <li>
 * Admin thread. <br/>
 * This thread will continually go to the master to try and get the cluster status.  Just like the
 * put and scan threads, the time to respond is recorded.
 * </li>
 * <li>
 * Chaos Monkey thread.<br/>
 * This thread runs a ChaosMonkey.Action.
 * </li>
 * </ol>
 * <p/>
 * The ChaosMonkey actions currently run are:
 * <ul>
 * <li>Restart the RegionServer holding meta.</li>
 * <li>Restart the RegionServer holding the table the scan and put threads are targeting.</li>
 * <li>Move the Regions of the table used by the scan and put threads.</li>
 * <li>Restart the master.</li>
 * </ul>
 * <p/>
 * At the end of the test a log line is output on the INFO level containing the timing data that was
 * collected.
 */

@Category(IntegrationTests.class)
public class IntegrationTestMTTR {
  /**
   * Constants.
   */
  private static final byte[] FAMILY = Bytes.toBytes("d");
  private static final Log LOG = LogFactory.getLog(IntegrationTestMTTR.class);
  private static final long SLEEP_TIME = 60 * 1000l;

  /**
   * Configurable table names.
   */
  private static String tableName;
  private static byte[] tableNameBytes;
  private static String loadTableName;
  private static byte[] loadTableNameBytes;

  /**
   * Util to get at the cluster.
   */
  private static IntegrationTestingUtility util;

  /**
   * Executor for test threads.
   */
  private static ExecutorService executorService;

  /**
   * All of the chaos monkey actions used.
   */
  private static ChaosMonkey.Action restartRSAction;
  private static ChaosMonkey.Action restartMetaAction;
  private static ChaosMonkey.Action moveRegionAction;
  private static ChaosMonkey.Action restartMasterAction;

  /**
   * The load test tool used to create load and make sure that HLogs aren't empty.
   */
  private static LoadTestTool loadTool;


  @BeforeClass
  public static void setUp() throws Exception {
    // Set up the integration test util
    if (util == null) {
      util = new IntegrationTestingUtility();
    }

    // Make sure there are three servers.
    util.initializeCluster(3);

    // Set up the load test tool.
    loadTool = new LoadTestTool();
    loadTool.setConf(util.getConfiguration());

    // Create executor with enough threads to restart rs's,
    // run scans, puts, admin ops and load test tool.
    executorService = Executors.newFixedThreadPool(8);

    // Set up the tables needed.
    setupTables();

    // Set up the actions.
    setupActions();
  }

  private static void setupActions() throws IOException {
    // Set up the action that will restart a region server holding a region from our table
    // because this table should only have one region we should be good.
    restartRSAction = new ChaosMonkey.RestartRsHoldingTable(SLEEP_TIME, tableName);

    // Set up the action that will kill the region holding meta.
    restartMetaAction = new ChaosMonkey.RestartRsHoldingMeta(SLEEP_TIME);

    // Set up the action that will move the regions of our table.
    moveRegionAction = new ChaosMonkey.MoveRegionsOfTable(SLEEP_TIME, tableName);

    // Kill the master
    restartMasterAction = new ChaosMonkey.RestartActiveMaster(1000);

    // Give the action the access to the cluster.
    ChaosMonkey.ActionContext actionContext = new ChaosMonkey.ActionContext(util);
    restartRSAction.init(actionContext);
    restartMetaAction.init(actionContext);
    moveRegionAction.init(actionContext);
    restartMasterAction.init(actionContext);
  }

  private static void setupTables() throws IOException {
    // Get the table name.
    tableName = util.getConfiguration()
        .get("hbase.IntegrationTestMTTR.tableName", "IntegrationTestMTTR");
    tableNameBytes = Bytes.toBytes(tableName);

    loadTableName = util.getConfiguration()
        .get("hbase.IntegrationTestMTTR.loadTableName", "IntegrationTestMTTRLoadTestTool");
    loadTableNameBytes = Bytes.toBytes(loadTableName);

    if (util.getHBaseAdmin().tableExists(tableNameBytes)) {
      util.deleteTable(tableNameBytes);
    }

    if (util.getHBaseAdmin().tableExists(loadTableName)) {
      util.deleteTable(loadTableNameBytes);
    }

    // Create the table.  If this fails then fail everything.
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableNameBytes);

    // Make the max file size huge so that splits don't happen during the test.
    tableDescriptor.setMaxFileSize(Long.MAX_VALUE);

    HColumnDescriptor descriptor = new HColumnDescriptor(FAMILY);
    descriptor.setMaxVersions(1);
    tableDescriptor.addFamily(descriptor);
    util.getHBaseAdmin().createTable(tableDescriptor);

    // Setup the table for LoadTestTool
    int ret = loadTool.run(new String[]{"-tn", loadTableName, "-init_only"});
    assertEquals("Failed to initialize LoadTestTool", 0, ret);
  }

  @AfterClass
  public static void after() throws IOException {
    // Clean everything up.
    util.restoreCluster();
    util = null;

    // Stop the threads so that we know everything is complete.
    executorService.shutdown();
    executorService = null;

    // Clean up the actions.
    moveRegionAction = null;
    restartMetaAction = null;
    restartRSAction = null;
    restartMasterAction = null;

    loadTool = null;
  }

  @Test
  public void testRestartRsHoldingTable() throws Exception {
    run(new ActionCallable(restartRSAction), "RestartRsHoldingTable");
  }

  @Test
  public void testKillRsHoldingMeta() throws Exception {
    run(new ActionCallable(restartMetaAction), "KillRsHoldingMeta");
  }

  @Test
  public void testMoveRegion() throws Exception {
    run(new ActionCallable(moveRegionAction), "MoveRegion");
  }

  @Test
  public void testRestartMaster() throws Exception {
    run(new ActionCallable(restartMasterAction), "RestartMaster");
  }

  public void run(Callable<Boolean> monkeyCallable, String testName) throws Exception {
    int maxIters = util.getHBaseClusterInterface().isDistributedCluster() ? 10 : 3;

    // Array to keep track of times.
    ArrayList<TimingResult> resultPuts = new ArrayList<TimingResult>(maxIters);
    ArrayList<TimingResult> resultScan = new ArrayList<TimingResult>(maxIters);
    ArrayList<TimingResult> resultAdmin = new ArrayList<TimingResult>(maxIters);
    long start = System.nanoTime();

    // We're going to try this multiple times
    for (int fullIterations = 0; fullIterations < maxIters; fullIterations++) {
      // Create and start executing a callable that will kill the servers
      Future<Boolean> monkeyFuture = executorService.submit(monkeyCallable);

      // Pass that future to the timing Callables.
      Future<TimingResult> putFuture = executorService.submit(new PutCallable(monkeyFuture));
      Future<TimingResult> scanFuture = executorService.submit(new ScanCallable(monkeyFuture));
      Future<TimingResult> adminFuture = executorService.submit(new AdminCallable(monkeyFuture));

      Future<Boolean> loadFuture = executorService.submit(new LoadCallable(monkeyFuture));

      monkeyFuture.get();
      loadFuture.get();

      // Get the values from the futures.
      TimingResult putTime = putFuture.get();
      TimingResult scanTime = scanFuture.get();
      TimingResult adminTime = adminFuture.get();

      // Store the times to display later.
      resultPuts.add(putTime);
      resultScan.add(scanTime);
      resultAdmin.add(adminTime);

      // Wait some time for everything to settle down.
      Thread.sleep(5000l);
    }

    long runtimeMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

    Objects.ToStringHelper helper = Objects.toStringHelper("MTTRResults")
        .add("putResults", resultPuts)
        .add("scanResults", resultScan)
        .add("adminResults", resultAdmin)
        .add("totalRuntimeMs", runtimeMs)
        .add("name", testName);

    // Log the info
    LOG.info(helper.toString());
  }

  /**
   * Class to store results of TimingCallable.
   *
   * Stores times and trace id.
   */
  private class TimingResult {
    DescriptiveStatistics stats = new DescriptiveStatistics();

    /**
     * Add a result to this aggregate result.
     * @param time Time in nanoseconds
     * @param span Span.  To be kept if the time taken was over 1 second
     */
    public void addResult(long time) {
      stats.addValue(TimeUnit.MILLISECONDS.convert(time, TimeUnit.NANOSECONDS));
    }

    public String toString() {
      Objects.ToStringHelper helper = Objects.toStringHelper(this)
          .add("numResults", stats.getN())
          .add("minTime", stats.getMin())
          .add("meanTime", stats.getMean())
          .add("maxTime", stats.getMax())
          .add("25th", stats.getPercentile(25))
          .add("50th", stats.getPercentile(50))
          .add("75th", stats.getPercentile(75))
          .add("90th", stats.getPercentile(90))
          .add("95th", stats.getPercentile(95))
          .add("99th", stats.getPercentile(99))
          .add("99.9th", stats.getPercentile(99.9))
          .add("99.99th", stats.getPercentile(99.99));
      return helper.toString();
    }
  }

  /**
   * Base class for actions that need to record the time needed to recover from a failure.
   */
  public abstract class TimingCallable implements Callable<TimingResult> {
    protected final Future future;

    public TimingCallable(Future f) {
      future = f;
    }

    @Override
    public TimingResult call() throws Exception {
      TimingResult result = new TimingResult();
      int numAfterDone = 0;
      // Keep trying until the rs is back up and we've gotten a put through
      while (numAfterDone < 10) {
        long start = System.nanoTime();
        try {
          boolean actionResult = doAction();
          if (actionResult && future.isDone()) {
            numAfterDone ++;
          }
        } catch (Exception e) {
          numAfterDone = 0;
        }
        result.addResult(System.nanoTime() - start);
      }
      return result;
    }

    protected abstract boolean doAction() throws Exception;

    protected String getSpanName() {
      return this.getClass().getSimpleName();
    }
  }

  /**
   * Callable that will keep putting small amounts of data into a table
   * until  the future supplied returns.  It keeps track of the max time.
   */
  public class PutCallable extends TimingCallable {

    private final HTable table;

    public PutCallable(Future f) throws IOException {
      super(f);
      this.table = new HTable(util.getConfiguration(), tableNameBytes);
    }

    @Override
    protected boolean doAction() throws Exception {
      Put p = new Put(Bytes.toBytes(RandomStringUtils.randomAlphanumeric(5)));
      p.add(FAMILY, Bytes.toBytes("\0"), Bytes.toBytes(RandomStringUtils.randomAscii(5)));
      table.put(p);
      table.flushCommits();
      return true;
    }

    @Override
    protected String getSpanName() {
      return "MTTR Put Test";
    }
  }

  /**
   * Callable that will keep scanning for small amounts of data until the
   * supplied future returns.  Returns the max time taken to scan.
   */
  public class ScanCallable extends TimingCallable {
    private final HTable table;

    public ScanCallable(Future f) throws IOException {
      super(f);
      this.table = new HTable(util.getConfiguration(), tableNameBytes);
    }

    @Override
    protected boolean doAction() throws Exception {
      ResultScanner rs = null;
      try {
      Scan s = new Scan();
      s.setBatch(2);
      s.addFamily(FAMILY);
      s.setFilter(new KeyOnlyFilter());
      s.setMaxVersions(1);

      rs = table.getScanner(s);
      Result result = rs.next();
      return rs != null && result != null && result.size() > 0;
      } finally {
        if (rs != null) {
          rs.close();
        }
      }
    }
    @Override
    protected String getSpanName() {
      return "MTTR Scan Test";
    }
  }

  /**
   * Callable that will keep going to the master for cluster status.  Returns the max time taken.
   */
  public class AdminCallable extends TimingCallable {

    public AdminCallable(Future f) throws IOException {
      super(f);
    }

    @Override
    protected boolean doAction() throws Exception {
      HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
      ClusterStatus status = admin.getClusterStatus();
      return status != null;
    }

    @Override
    protected String getSpanName() {
      return "MTTR Admin Test";
    }
  }


  public class ActionCallable implements Callable<Boolean> {
    private final ChaosMonkey.Action action;

    public ActionCallable(ChaosMonkey.Action action) {
      this.action = action;
    }

    @Override
    public Boolean call() throws Exception {
      this.action.perform();
      return true;
    }
  }

  /**
   * Callable used to make sure the cluster has some load on it.
   * This callable uses LoadTest tool to
   */
  public class LoadCallable implements Callable<Boolean> {

    private final Future future;

    public LoadCallable(Future f) {
      future = f;
    }

    @Override
    public Boolean call() throws Exception {
      int colsPerKey = 10;
      int recordSize = 500;
      int numServers = util.getHBaseClusterInterface().getInitialClusterStatus().getServersSize();
      int numKeys = numServers * 5000;
      int writeThreads = 10;


      // Loop until the chaos monkey future is done.
      // But always go in just in case some action completes quickly
      do {
        int ret = loadTool.run(new String[]{
            "-tn", loadTableName,
            "-write", String.format("%d:%d:%d", colsPerKey, recordSize, writeThreads),
            "-num_keys", String.valueOf(numKeys),
            "-skip_init"
        });
        assertEquals("Load failed", 0, ret);
      } while (!future.isDone());

      return true;
    }
  }
}
