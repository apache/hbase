/**
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
package org.apache.hadoop.hbase;

import com.google.common.base.Objects;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.chaos.actions.MoveRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRsHoldingTableAction;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;
import org.apache.hadoop.hbase.chaos.policies.Policy;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for comparing the performance impact of region replicas. Uses
 * components of {@link PerformanceEvaluation}. Does not run from
 * {@code IntegrationTestsDriver} because IntegrationTestBase is incompatible
 * with the JUnit runner. Hence no @Test annotations either. See {@code -help}
 * for full list of options.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRegionReplicaPerf extends IntegrationTestBase {

  private static final Log LOG = LogFactory.getLog(IntegrationTestRegionReplicaPerf.class);

  private static final String SLEEP_TIME_KEY = "sleeptime";
  // short default interval because tests don't run very long.
  private static final String SLEEP_TIME_DEFAULT = "" + (10 * 1000l);
  private static final String TABLE_NAME_KEY = "tableName";
  private static final String TABLE_NAME_DEFAULT = "IntegrationTestRegionReplicaPerf";
  private static final String NOMAPRED_KEY = "nomapred";
  private static final boolean NOMAPRED_DEFAULT = false;
  private static final String REPLICA_COUNT_KEY = "replicas";
  private static final String REPLICA_COUNT_DEFAULT = "" + 3;
  private static final String PRIMARY_TIMEOUT_KEY = "timeout";
  private static final String PRIMARY_TIMEOUT_DEFAULT = "" + 10 * 1000; // 10 ms
  private static final String NUM_RS_KEY = "numRs";
  private static final String NUM_RS_DEFAULT = "" + 3;

  private TableName tableName;
  private long sleepTime;
  private boolean nomapred = NOMAPRED_DEFAULT;
  private int replicaCount;
  private int primaryTimeout;
  private int clusterSize;

  /**
   * Wraps the invocation of {@link PerformanceEvaluation} in a {@code Callable}.
   */
  static class PerfEvalCallable implements Callable<TimingResult> {
    private final Queue<String> argv = new LinkedList<String>();
    private final Admin admin;

    public PerfEvalCallable(Admin admin, String argv) {
      // TODO: this API is awkward, should take HConnection, not HBaseAdmin
      this.admin = admin;
      this.argv.addAll(Arrays.asList(argv.split(" ")));
      LOG.debug("Created PerformanceEvaluationCallable with args: " + argv);
    }

    @Override
    public TimingResult call() throws Exception {
      PerformanceEvaluation.TestOptions opts = PerformanceEvaluation.parseOpts(argv);
      PerformanceEvaluation.checkTable(admin, opts);
      long numRows = opts.totalRows;
      long elapsedTime;
      if (opts.nomapred) {
        elapsedTime = PerformanceEvaluation.doLocalClients(opts, admin.getConfiguration());
      } else {
        Job job = PerformanceEvaluation.doMapReduce(opts, admin.getConfiguration());
        Counters counters = job.getCounters();
        numRows = counters.findCounter(PerformanceEvaluation.Counter.ROWS).getValue();
        elapsedTime = counters.findCounter(PerformanceEvaluation.Counter.ELAPSED_TIME).getValue();
      }
      return new TimingResult(numRows, elapsedTime);
    }
  }

  /**
   * Record the results from a single {@link PerformanceEvaluation} job run.
   */
  static class TimingResult {
    public long numRows;
    public long elapsedTime;

    public TimingResult(long numRows, long elapsedTime) {
      this.numRows = numRows;
      this.elapsedTime = elapsedTime;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("numRows", numRows)
        .add("elapsedTime", elapsedTime)
        .toString();
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = util.getConfiguration();

    // sanity check cluster
    // TODO: this should reach out to master and verify online state instead
    assertEquals("Master must be configured with StochasticLoadBalancer",
      "org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer",
      conf.get("hbase.master.loadbalancer.class"));
    // TODO: this should reach out to master and verify online state instead
    assertTrue("hbase.regionserver.storefile.refresh.period must be greater than zero.",
      conf.getLong("hbase.regionserver.storefile.refresh.period", 0) > 0);

    // enable client-side settings
    conf.setBoolean(RpcClient.SPECIFIC_WRITE_THREAD, true);
    // TODO: expose these settings to CLI override
    conf.setLong("hbase.client.primaryCallTimeout.get", primaryTimeout);
    conf.setLong("hbase.client.primaryCallTimeout.multiget", primaryTimeout);
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    util.initializeCluster(clusterSize);
  }

  @Override
  public void setUpMonkey() throws Exception {
    Policy p = new PeriodicRandomActionPolicy(sleepTime,
      new RestartRsHoldingTableAction(sleepTime, tableName.getNameAsString()),
      new MoveRandomRegionOfTableAction(tableName));
    this.monkey = new PolicyBasedChaosMonkey(util, p);
    // don't start monkey right away
  }

  @Override
  protected void addOptions() {
    addOptWithArg(TABLE_NAME_KEY, "Alternate table name. Default: '"
      + TABLE_NAME_DEFAULT + "'");
    addOptWithArg(SLEEP_TIME_KEY, "How long the monkey sleeps between actions. Default: "
      + SLEEP_TIME_DEFAULT);
    addOptNoArg(NOMAPRED_KEY,
      "Run multiple clients using threads (rather than use mapreduce)");
    addOptWithArg(REPLICA_COUNT_KEY, "Number of region replicas. Default: "
      + REPLICA_COUNT_DEFAULT);
    addOptWithArg(PRIMARY_TIMEOUT_KEY, "Overrides hbase.client.primaryCallTimeout. Default: "
      + PRIMARY_TIMEOUT_DEFAULT + " (10ms)");
    addOptWithArg(NUM_RS_KEY, "Specify the number of RegionServers to use. Default: "
      + NUM_RS_DEFAULT);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    tableName = TableName.valueOf(cmd.getOptionValue(TABLE_NAME_KEY, TABLE_NAME_DEFAULT));
    sleepTime = Long.parseLong(cmd.getOptionValue(SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT));
    nomapred = cmd.hasOption(NOMAPRED_KEY);
    replicaCount = Integer.parseInt(cmd.getOptionValue(REPLICA_COUNT_KEY, REPLICA_COUNT_DEFAULT));
    primaryTimeout =
      Integer.parseInt(cmd.getOptionValue(PRIMARY_TIMEOUT_KEY, PRIMARY_TIMEOUT_DEFAULT));
    clusterSize = Integer.parseInt(cmd.getOptionValue(NUM_RS_KEY, NUM_RS_DEFAULT));
    LOG.debug(Objects.toStringHelper("Parsed Options")
      .add(TABLE_NAME_KEY, tableName)
      .add(SLEEP_TIME_KEY, sleepTime)
      .add(NOMAPRED_KEY, nomapred)
      .add(REPLICA_COUNT_KEY, replicaCount)
      .add(PRIMARY_TIMEOUT_KEY, primaryTimeout)
      .add(NUM_RS_KEY, clusterSize)
      .toString());
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    test();
    return 0;
  }

  @Override
  public TableName getTablename() {
    return tableName;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return null;
  }

  public void test() throws Exception {
    int maxIters = 3;
    String mr = nomapred ? "--nomapred" : "";
    String replicas = "--replicas=" + replicaCount;
    // TODO: splits disabled until "phase 2" is complete.
    String splitPolicy = "--splitPolicy=" + DisabledRegionSplitPolicy.class.getName();
    String writeOpts = format("%s %s --table=%s --presplit=16 sequentialWrite 4",
      mr, splitPolicy, tableName);
    String readOpts =
      format("%s --table=%s --latency --sampleRate=0.1 randomRead 4", mr, tableName);
    String replicaReadOpts = format("%s %s", replicas, readOpts);

    ArrayList<TimingResult> resultsWithoutReplica = new ArrayList<TimingResult>(maxIters);
    ArrayList<TimingResult> resultsWithReplica = new ArrayList<TimingResult>(maxIters);

    // create/populate the table, replicas disabled
    LOG.debug("Populating table.");
    new PerfEvalCallable(util.getHBaseAdmin(), writeOpts).call();

    // one last sanity check, then send in the clowns!
    assertEquals("Table must be created with DisabledRegionSplitPolicy. Broken test.",
      DisabledRegionSplitPolicy.class.getName(),
      util.getHBaseAdmin().getTableDescriptor(tableName).getRegionSplitPolicyClassName());
    startMonkey();

    // collect a baseline without region replicas.
    for (int i = 0; i < maxIters; i++) {
      LOG.debug("Launching non-replica job " + (i + 1) + "/" + maxIters);
      resultsWithoutReplica.add(new PerfEvalCallable(util.getHBaseAdmin(), readOpts).call());
      // TODO: sleep to let cluster stabilize, though monkey continues. is it necessary?
      Thread.sleep(5000l);
    }

    // disable monkey, enable region replicas, enable monkey
    cleanUpMonkey("Altering table.");
    LOG.debug("Altering " + tableName + " replica count to " + replicaCount);
    util.setReplicas(util.getHBaseAdmin(), tableName, replicaCount);
    setUpMonkey();
    startMonkey();

    // run test with region replicas.
    for (int i = 0; i < maxIters; i++) {
      LOG.debug("Launching replica job " + (i + 1) + "/" + maxIters);
      resultsWithReplica.add(new PerfEvalCallable(util.getHBaseAdmin(), replicaReadOpts).call());
      // TODO: sleep to let cluster stabilize, though monkey continues. is it necessary?
      Thread.sleep(5000l);
    }

    DescriptiveStatistics withoutReplicaStats = new DescriptiveStatistics();
    for (TimingResult tr : resultsWithoutReplica) {
      withoutReplicaStats.addValue(tr.elapsedTime);
    }
    DescriptiveStatistics withReplicaStats = new DescriptiveStatistics();
    for (TimingResult tr : resultsWithReplica) {
      withReplicaStats.addValue(tr.elapsedTime);
    }

    LOG.info(Objects.toStringHelper("testName")
      .add("withoutReplicas", resultsWithoutReplica)
      .add("withReplicas", resultsWithReplica)
      .add("withoutReplicasMean", withoutReplicaStats.getMean())
      .add("withReplicasMean", withReplicaStats.getMean())
      .toString());

    assertTrue(
      "Running with region replicas under chaos should be as fast or faster than without. "
      + "withReplicas.mean: " + withReplicaStats.getMean() + "ms "
      + "withoutReplicas.mean: " + withoutReplicaStats.getMean() + "ms.",
      withReplicaStats.getMean() <= withoutReplicaStats.getMean());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestRegionReplicaPerf(), args);
    System.exit(status);
  }
}
