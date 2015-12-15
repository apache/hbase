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
import com.google.common.collect.Sets;
import com.codahale.metrics.Histogram;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.chaos.actions.MoveRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;
import org.apache.hadoop.hbase.chaos.policies.Policy;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.YammerHistogramUtils;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
  private static final String REPLICA_COUNT_KEY = "replicas";
  private static final String REPLICA_COUNT_DEFAULT = "" + 3;
  private static final String PRIMARY_TIMEOUT_KEY = "timeout";
  private static final String PRIMARY_TIMEOUT_DEFAULT = "" + 10 * 1000; // 10 ms
  private static final String NUM_RS_KEY = "numRs";
  private static final String NUM_RS_DEFAULT = "" + 3;

  /** Extract a descriptive statistic from a {@link com.codahale.metrics.Histogram}. */
  private enum Stat {
    STDEV {
      @Override
      double apply(Histogram hist) {
        return hist.getSnapshot().getStdDev();
      }
    },
    FOUR_9S {
      @Override
      double apply(Histogram hist) {
        return hist.getSnapshot().getValue(0.9999);
      }
    };

    abstract double apply(Histogram hist);
  }

  private TableName tableName;
  private long sleepTime;
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
      PerformanceEvaluation.RunResult results[] = null;
      long numRows = opts.totalRows;
      long elapsedTime = 0;
      if (opts.nomapred) {
        results = PerformanceEvaluation.doLocalClients(opts, admin.getConfiguration());
        for (PerformanceEvaluation.RunResult r : results) {
          elapsedTime = Math.max(elapsedTime, r.duration);
        }
      } else {
        Job job = PerformanceEvaluation.doMapReduce(opts, admin.getConfiguration());
        Counters counters = job.getCounters();
        numRows = counters.findCounter(PerformanceEvaluation.Counter.ROWS).getValue();
        elapsedTime = counters.findCounter(PerformanceEvaluation.Counter.ELAPSED_TIME).getValue();
      }
      return new TimingResult(numRows, elapsedTime, results);
    }
  }

  /**
   * Record the results from a single {@link PerformanceEvaluation} job run.
   */
  static class TimingResult {
    public final long numRows;
    public final long elapsedTime;
    public final PerformanceEvaluation.RunResult results[];

    public TimingResult(long numRows, long elapsedTime, PerformanceEvaluation.RunResult results[]) {
      this.numRows = numRows;
      this.elapsedTime = elapsedTime;
      this.results = results;
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
      new RestartRandomRsExceptMetaAction(sleepTime),
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
    replicaCount = Integer.parseInt(cmd.getOptionValue(REPLICA_COUNT_KEY, REPLICA_COUNT_DEFAULT));
    primaryTimeout =
      Integer.parseInt(cmd.getOptionValue(PRIMARY_TIMEOUT_KEY, PRIMARY_TIMEOUT_DEFAULT));
    clusterSize = Integer.parseInt(cmd.getOptionValue(NUM_RS_KEY, NUM_RS_DEFAULT));
    LOG.debug(Objects.toStringHelper("Parsed Options")
      .add(TABLE_NAME_KEY, tableName)
      .add(SLEEP_TIME_KEY, sleepTime)
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
    return Sets.newHashSet(Bytes.toString(PerformanceEvaluation.FAMILY_NAME));
  }

  /** Compute the mean of the given {@code stat} from a timing results. */
  private static double calcMean(String desc, Stat stat, List<TimingResult> results) {
    double sum = 0;
    int count = 0;

    for (TimingResult tr : results) {
      for (PerformanceEvaluation.RunResult r : tr.results) {
        assertNotNull("One of the run results is missing detailed run data.", r.hist);
        sum += stat.apply(r.hist);
        count += 1;
        LOG.debug(desc + "{" + YammerHistogramUtils.getHistogramReport(r.hist) + "}");
      }
    }
    return sum / count;
  }

  public void test() throws Exception {
    int maxIters = 3;
    String replicas = "--replicas=" + replicaCount;
    // TODO: splits disabled until "phase 2" is complete.
    String splitPolicy = "--splitPolicy=" + DisabledRegionSplitPolicy.class.getName();
    String writeOpts = format("%s --nomapred --table=%s --presplit=16 sequentialWrite 4",
      splitPolicy, tableName);
    String readOpts =
      format("--nomapred --table=%s --latency --sampleRate=0.1 randomRead 4", tableName);
    String replicaReadOpts = format("%s %s", replicas, readOpts);

    ArrayList<TimingResult> resultsWithoutReplicas = new ArrayList<TimingResult>(maxIters);
    ArrayList<TimingResult> resultsWithReplicas = new ArrayList<TimingResult>(maxIters);

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
      resultsWithoutReplicas.add(new PerfEvalCallable(util.getHBaseAdmin(), readOpts).call());
      // TODO: sleep to let cluster stabilize, though monkey continues. is it necessary?
      Thread.sleep(5000l);
    }

    // disable monkey, enable region replicas, enable monkey
    cleanUpMonkey("Altering table.");
    LOG.debug("Altering " + tableName + " replica count to " + replicaCount);
    IntegrationTestingUtility.setReplicas(util.getHBaseAdmin(), tableName, replicaCount);
    setUpMonkey();
    startMonkey();

    // run test with region replicas.
    for (int i = 0; i < maxIters; i++) {
      LOG.debug("Launching replica job " + (i + 1) + "/" + maxIters);
      resultsWithReplicas.add(new PerfEvalCallable(util.getHBaseAdmin(), replicaReadOpts).call());
      // TODO: sleep to let cluster stabilize, though monkey continues. is it necessary?
      Thread.sleep(5000l);
    }

    // compare the average of the stdev and 99.99pct across runs to determine if region replicas
    // are having an overall improvement on response variance experienced by clients.
    double withoutReplicasStdevMean =
        calcMean("withoutReplicas", Stat.STDEV, resultsWithoutReplicas);
    double withoutReplicas9999Mean =
        calcMean("withoutReplicas", Stat.FOUR_9S, resultsWithoutReplicas);
    double withReplicasStdevMean =
        calcMean("withReplicas", Stat.STDEV, resultsWithReplicas);
    double withReplicas9999Mean =
        calcMean("withReplicas", Stat.FOUR_9S, resultsWithReplicas);

    LOG.info(Objects.toStringHelper(this)
      .add("withoutReplicas", resultsWithoutReplicas)
      .add("withReplicas", resultsWithReplicas)
      .add("withoutReplicasStdevMean", withoutReplicasStdevMean)
      .add("withoutReplicas99.99Mean", withoutReplicas9999Mean)
      .add("withReplicasStdevMean", withReplicasStdevMean)
      .add("withReplicas99.99Mean", withReplicas9999Mean)
      .toString());

    assertTrue(
      "Running with region replicas under chaos should have less request variance than without. "
      + "withReplicas.stdev.mean: " + withReplicasStdevMean + "ms "
      + "withoutReplicas.stdev.mean: " + withoutReplicasStdevMean + "ms.",
      withReplicasStdevMean <= withoutReplicasStdevMean);
    assertTrue(
        "Running with region replicas under chaos should improve 99.99pct latency. "
            + "withReplicas.99.99.mean: " + withReplicas9999Mean + "ms "
            + "withoutReplicas.99.99.mean: " + withoutReplicas9999Mean + "ms.",
        withReplicas9999Mean <= withoutReplicas9999Mean);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestRegionReplicaPerf(), args);
    System.exit(status);
  }
}
