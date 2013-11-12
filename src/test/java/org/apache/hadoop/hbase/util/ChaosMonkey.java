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

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTestDataIngestWithChaosMonkey;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A utility to injects faults in a running cluster.
 * <p>
 * ChaosMonkey defines Action's and Policy's. Actions are sequences of events, like
 *  - Select a random server to kill
 *  - Sleep for 5 sec
 *  - Start the server on the same host
 * Actions can also be complex events, like rolling restart of all of the servers.
 * <p>
 * Policies on the other hand are responsible for executing the actions based on a strategy.
 * The default policy is to execute a random action every minute based on predefined action
 * weights. ChaosMonkey executes predefined named policies until it is stopped. More than one
 * policy can be active at any time.
 * <p>
 * Chaos monkey can be run from the command line, or can be invoked from integration tests.
 * See {@link IntegrationTestDataIngestWithChaosMonkey} or other integration tests that use
 * chaos monkey for code examples.
 * <p>
 * ChaosMonkey class is indeed inspired by the Netflix's same-named tool:
 * http://techblog.netflix.com/2012/07/chaos-monkey-released-into-wild.html
 */
public class ChaosMonkey extends AbstractHBaseTool implements Stoppable {

  private static final Log LOG = LogFactory.getLog(ChaosMonkey.class);

  private static final long ONE_SEC = 1000;
  private static final long FIVE_SEC = 5 * ONE_SEC;
  private static final long ONE_MIN = 60 * ONE_SEC;
  private static final long TIMEOUT = ONE_MIN;

  final IntegrationTestingUtility util;

  /**
   * Construct a new ChaosMonkey
   * @param util the HBaseIntegrationTestingUtility already configured
   * @param policies names of pre-defined policies to use
   */
  public ChaosMonkey(IntegrationTestingUtility util, String... policies) {
    this.util = util;
    setPoliciesByName(policies);
  }

  /**
   * Construct a new ChaosMonkey
   * @param util the HBaseIntegrationTestingUtility already configured
   * @param policies custom policies to use
   */
  public ChaosMonkey(IntegrationTestingUtility util, Policy... policies) {
    this.util = util;
    this.policies = policies;
  }

  private void setPoliciesByName(String... policies) {
    this.policies = new Policy[policies.length];
    for (int i=0; i < policies.length; i++) {
      this.policies[i] = NAMED_POLICIES.get(policies[i]);
    }
  }

  /**
   * Context for Action's
   */
  public static class ActionContext {
    private IntegrationTestingUtility util;

    public ActionContext(IntegrationTestingUtility util) {
      this.util = util;
    }

    public IntegrationTestingUtility getHBaseIntegrationTestingUtility() {
      return util;
    }

    public HBaseCluster getHBaseCluster() {
      return util.getHBaseClusterInterface();
    }
  }

  /**
   * A (possibly mischievous) action that the ChaosMonkey can perform.
   */
  public static class Action {
    // TODO: interesting question - should actions be implemented inside
    //       ChaosMonkey, or outside? If they are inside (initial), the class becomes
    //       huge and all-encompassing; if they are outside ChaosMonkey becomes just
    //       a random task scheduler. For now, keep inside.

    protected ActionContext context;
    protected HBaseCluster cluster;
    protected ClusterStatus initialStatus;
    protected ServerName[] initialServers;

    public void init(ActionContext context) throws IOException {
      this.context = context;
      cluster = context.getHBaseCluster();
      initialStatus = cluster.getInitialClusterStatus();
      Collection<ServerName> regionServers = initialStatus.getServers();
      initialServers = regionServers.toArray(new ServerName[regionServers.size()]);
    }

    public void perform() throws Exception { };

    // TODO: perhaps these methods should be elsewhere?
    /** Returns current region servers */
    protected ServerName[] getCurrentServers() throws IOException {
      Collection<ServerName> regionServers = cluster.getClusterStatus().getServers();
      return regionServers.toArray(new ServerName[regionServers.size()]);
    }

    protected void killMaster(ServerName server) throws IOException {
      LOG.info("Killing master:" + server);
      cluster.killMaster(server);
      cluster.waitForMasterToStop(server, TIMEOUT);
      LOG.info("Killed master server:" + server);
    }

    protected void startMaster(ServerName server) throws IOException {
      LOG.info("Starting master:" + server.getHostname());
      cluster.startMaster(server.getHostname());
      cluster.waitForActiveAndReadyMaster(TIMEOUT);
      LOG.info("Started master: " + server);
    }

    protected void killRs(ServerName server) throws IOException {
      LOG.info("Killing region server:" + server);
      cluster.killRegionServer(server);
      cluster.waitForRegionServerToStop(server, TIMEOUT);
      LOG.info("Killed region server:" + server + ". Reported num of rs:"
          + cluster.getClusterStatus().getServersSize());
    }

    protected void startRs(ServerName server) throws IOException {
      LOG.info("Starting region server:" + server.getHostname());
      cluster.startRegionServer(server.getHostname());
      cluster.waitForRegionServerToStart(server.getHostname(), TIMEOUT);
      LOG.info("Started region server:" + server + ". Reported num of rs:"
          + cluster.getClusterStatus().getServersSize());
    }
  }

  private static class RestartActionBase extends Action {
    long sleepTime; // how long should we sleep

    public RestartActionBase(long sleepTime) {
      this.sleepTime = sleepTime;
    }

    void sleep(long sleepTime) {
      LOG.info("Sleeping for:" + sleepTime);
      Threads.sleep(sleepTime);
    }

    void restartMaster(ServerName server, long sleepTime) throws IOException {
      sleepTime = Math.max(sleepTime, 1000);
      killMaster(server);
      sleep(sleepTime);
      startMaster(server);
    }

    void restartRs(ServerName server, long sleepTime) throws IOException {
      sleepTime = Math.max(sleepTime, 1000);
      killRs(server);
      sleep(sleepTime);
      startRs(server);
    }
  }

  public static class RestartActiveMaster extends RestartActionBase {
    public RestartActiveMaster(long sleepTime) {
      super(sleepTime);
    }
    @Override
    public void perform() throws Exception {
      LOG.info("Performing action: Restart active master");

      ServerName master = cluster.getClusterStatus().getMaster();
      restartMaster(master, sleepTime);
    }
  }

  public static class RestartRandomRs extends RestartActionBase {
    public RestartRandomRs(long sleepTime) {
      super(sleepTime);
    }

    @Override
    public void perform() throws Exception {
      LOG.info("Performing action: Restart random region server");
      ServerName server = selectRandomItem(getCurrentServers());

      restartRs(server, sleepTime);
    }
  }

  public static class RestartRsHoldingMeta extends RestartActionBase {
    public RestartRsHoldingMeta(long sleepTime) {
      super(sleepTime);
    }
    @Override
    public void perform() throws Exception {
      LOG.info("Performing action: Restart region server holding META");
      ServerName server = cluster.getServerHoldingMeta();
      if (server == null) {
        LOG.warn("No server is holding .META. right now.");
        return;
      }
      restartRs(server, sleepTime);
    }
  }

  public static class RestartRsHoldingRoot extends RestartRandomRs {
    public RestartRsHoldingRoot(long sleepTime) {
      super(sleepTime);
    }
    @Override
    public void perform() throws Exception {
      LOG.info("Performing action: Restart region server holding ROOT");
      ServerName server = cluster.getServerHoldingRoot();
      if (server == null) {
        LOG.warn("No server is holding -ROOT- right now.");
        return;
      }
      restartRs(server, sleepTime);
    }
  }

  public static class RestartRsHoldingTable extends RestartActionBase {

    private final String tableName;

    public RestartRsHoldingTable(long sleepTime, String tableName) {
      super(sleepTime);
      this.tableName = tableName;
    }

    @Override
    public void perform() throws Exception {
      HTable table = null;
      Collection<ServerName> serverNames;
      try {
        Configuration conf = context.getHBaseIntegrationTestingUtility().getConfiguration();
        table = new HTable(conf, tableName);
        serverNames = table.getRegionLocations().values();
      } catch (IOException e) {
        LOG.debug("Error creating HTable used to get list of region locations.", e);
        return;
      } finally {
        if (table != null) {
          table.close();
        }
      }
      Random random = new Random();
      ServerName[] nameArray = serverNames.toArray(new ServerName[serverNames.size()]);
      restartRs(nameArray[random.nextInt(nameArray.length)], sleepTime);
    }
  }

  public static class MoveRegionsOfTable extends Action {
    private final long sleepTime;
    private final byte[] tableNameBytes;

    public MoveRegionsOfTable(long sleepTime, String tableName) {
      this.sleepTime = sleepTime;
      this.tableNameBytes = Bytes.toBytes(tableName);
    }

    @Override
    public void perform() throws Exception {
      try {
        HBaseAdmin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
        List<HRegionInfo> regions = admin.getTableRegions(tableNameBytes);
        Collection<ServerName> serversList = admin.getClusterStatus().getServers();
        ServerName[] servers = serversList.toArray(new ServerName[serversList.size()]);
        Random random = new Random();
        for (HRegionInfo regionInfo:regions) {
          try {
            byte[] destServerName =
              Bytes.toBytes(servers[random.nextInt(servers.length)].getServerName());
            admin.move(regionInfo.getRegionName(), destServerName);
          } catch (Exception e) {
            LOG.debug("Error moving region", e);
          }
        }
        Thread.sleep(sleepTime);
      } catch (Exception e) {
        LOG.debug("Error performing MoveRegionsOfTable", e);
      }
    }
  }

  /**
   * Restarts a ratio of the running regionservers at the same time
   */
  public static class BatchRestartRs extends RestartActionBase {
    float ratio; //ratio of regionservers to restart

    public BatchRestartRs(long sleepTime, float ratio) {
      super(sleepTime);
      this.ratio = ratio;
    }

    @Override
    public void perform() throws Exception {
      LOG.info(String.format("Performing action: Batch restarting %d%% of region servers",
          (int)(ratio * 100)));
      List<ServerName> selectedServers = selectRandomItems(getCurrentServers(), ratio);

      for (ServerName server : selectedServers) {
        LOG.info("Killing region server:" + server);
        cluster.killRegionServer(server);
      }

      for (ServerName server : selectedServers) {
        cluster.waitForRegionServerToStop(server, TIMEOUT);
      }

      LOG.info("Killed " + selectedServers.size() + " region servers. Reported num of rs:"
          + cluster.getClusterStatus().getServersSize());

      sleep(sleepTime);

      for (ServerName server : selectedServers) {
        LOG.info("Starting region server:" + server.getHostname());
        cluster.startRegionServer(server.getHostname());

      }
      for (ServerName server : selectedServers) {
        cluster.waitForRegionServerToStart(server.getHostname(), TIMEOUT);
      }
      LOG.info("Started " + selectedServers.size() +" region servers. Reported num of rs:"
          + cluster.getClusterStatus().getServersSize());
    }
  }

  /**
   * Restarts a ratio of the regionservers in a rolling fashion. At each step, either kills a
   * server, or starts one, sleeping randomly (0-sleepTime) in between steps.
   */
  public static class RollingBatchRestartRs extends BatchRestartRs {
    public RollingBatchRestartRs(long sleepTime, float ratio) {
      super(sleepTime, ratio);
    }

    @Override
    public void perform() throws Exception {
      Random random = new Random();
      LOG.info(String.format("Performing action: Rolling batch restarting %d%% of region servers",
          (int)(ratio * 100)));
      List<ServerName> selectedServers = selectRandomItems(getCurrentServers(), ratio);

      Queue<ServerName> serversToBeKilled = new LinkedList<ServerName>(selectedServers);
      Queue<ServerName> deadServers = new LinkedList<ServerName>();

      //
      while (!serversToBeKilled.isEmpty() || !deadServers.isEmpty()) {
        boolean action = true; //action true = kill server, false = start server

        if (serversToBeKilled.isEmpty() || deadServers.isEmpty()) {
          action = deadServers.isEmpty();
        } else {
          action = random.nextBoolean();
        }

        if (action) {
          ServerName server = serversToBeKilled.remove();
          killRs(server);
          deadServers.add(server);
        } else {
          ServerName server = deadServers.remove();
          startRs(server);
        }

        sleep(random.nextInt((int)sleepTime));
      }
    }
  }

  public static class UnbalanceRegionsAction extends Action {
    private double fractionOfRegions;
    private double fractionOfServers;
    private Random random = new Random();

    /**
     * Unbalances the regions on the cluster by choosing "target" servers, and moving
     * some regions from each of the non-target servers to random target servers.
     * @param fractionOfRegions Fraction of regions to move from each server.
     * @param fractionOfServers Fraction of servers to be chosen as targets.
     */
    public UnbalanceRegionsAction(double fractionOfRegions, double fractionOfServers) {
      this.fractionOfRegions = fractionOfRegions;
      this.fractionOfServers = fractionOfServers;
    }

    @Override
    public void perform() throws Exception {
      LOG.info("Unbalancing regions");
      ClusterStatus status = this.cluster.getClusterStatus();
      List<ServerName> victimServers = new LinkedList<ServerName>(status.getServers());
      int targetServerCount = (int)Math.ceil(fractionOfServers * victimServers.size());
      List<byte[]> targetServers = new ArrayList<byte[]>(targetServerCount);
      for (int i = 0; i < targetServerCount; ++i) {
        int victimIx = random.nextInt(victimServers.size());
        String serverName = victimServers.remove(victimIx).getServerName();
        targetServers.add(Bytes.toBytes(serverName));
      }

      List<byte[]> victimRegions = new LinkedList<byte[]>();
      for (ServerName server : victimServers) {
        HServerLoad serverLoad = status.getLoad(server);
        // Ugh.
        List<byte[]> regions = new LinkedList<byte[]>(serverLoad.getRegionsLoad().keySet());
        int victimRegionCount = (int)Math.ceil(fractionOfRegions * regions.size());
        LOG.debug("Removing " + victimRegionCount + " regions from " + server.getServerName());
        for (int i = 0; i < victimRegionCount; ++i) {
          int victimIx = random.nextInt(regions.size());
          String regionId = HRegionInfo.encodeRegionName(regions.remove(victimIx));
          victimRegions.add(Bytes.toBytes(regionId));
        }
      }

      LOG.info("Moving " + victimRegions.size() + " regions from " + victimServers.size()
          + " servers to " + targetServers.size() + " different servers");
      HBaseAdmin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
      for (byte[] victimRegion : victimRegions) {
        int targetIx = random.nextInt(targetServers.size());
        admin.move(victimRegion, targetServers.get(targetIx));
      }
    }
  }

  public static class ForceBalancerAction extends Action {
    @Override
    public void perform() throws Exception {
      LOG.info("Balancing regions");
      HBaseAdmin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
      boolean result = admin.balancer();
      if (!result) {
        LOG.error("Balancer didn't succeed");
      }
    }
  }

  /**
   * A context for a Policy
   */
  public static class PolicyContext extends ActionContext {
    public PolicyContext(IntegrationTestingUtility util) {
      super(util);
    }
  }

  /**
   * A policy to introduce chaos to the cluster
   */
  public static abstract class Policy extends StoppableImplementation implements Runnable {
    protected PolicyContext context;
    public void init(PolicyContext context) throws Exception {
      this.context = context;
    }
  }

  /** A policy that runs multiple other policies one after the other */
  public static class CompositeSequentialPolicy extends Policy {
    private List<Policy> policies;
    public CompositeSequentialPolicy(Policy... policies) {
      this.policies = Arrays.asList(policies);
    }

    @Override
    public void stop(String why) {
      super.stop(why);
      for (Policy p : policies) {
        p.stop(why);
      }
    }

    @Override
    public void run() {
      for (Policy p : policies) {
        p.run();
      }
    }

    @Override
    public void init(PolicyContext context) throws Exception {
      super.init(context);
      for (Policy p : policies) {
        p.init(context);
      }
    }
  }

  /** A policy which does stuff every time interval. */
  public static abstract class PeriodicPolicy extends Policy {
    private long periodMs;

    public PeriodicPolicy(long periodMs) {
      this.periodMs = periodMs;
    }

    @Override
    public void run() {
      // Add some jitter.
      int jitter = new Random().nextInt((int)periodMs);
      LOG.info("Sleeping for " + jitter + " to add jitter");
      Threads.sleep(jitter);

      while (!isStopped()) {
        long start = System.currentTimeMillis();
        runOneIteration();

        if (isStopped()) return;
        long sleepTime = periodMs - (System.currentTimeMillis() - start);
        if (sleepTime > 0) {
          LOG.info("Sleeping for: " + sleepTime);
          Threads.sleep(sleepTime);
        }
      }
    }

    protected abstract void runOneIteration();

    @Override
    public void init(PolicyContext context) throws Exception {
      super.init(context);
      LOG.info("Using ChaosMonkey Policy: " + this.getClass() + ", period: " + periodMs);
    }
  }

  /** A policy which performs a sequence of actions deterministically. */
  public static class DoActionsOncePolicy extends PeriodicPolicy {
    private List<Action> actions;

    public DoActionsOncePolicy(long periodMs, List<Action> actions) {
      super(periodMs);
      this.actions = new ArrayList<ChaosMonkey.Action>(actions);
    }

    public DoActionsOncePolicy(long periodMs, Action... actions) {
      this(periodMs, Arrays.asList(actions));
    }

    @Override
    protected void runOneIteration() {
      if (actions.isEmpty()) {
        this.stop("done");
        return;
      }
      Action action = actions.remove(0);

      try {
        action.perform();
      } catch (Exception ex) {
        LOG.warn("Exception occured during performing action: "
            + StringUtils.stringifyException(ex));
      }
    }

    @Override
    public void init(PolicyContext context) throws Exception {
      super.init(context);
      for (Action action : actions) {
        action.init(this.context);
      }
    }
  }

  /**
   * A policy, which picks a random action according to the given weights,
   * and performs it every configurable period.
   */
  public static class PeriodicRandomActionPolicy extends PeriodicPolicy {
    private List<Pair<Action, Integer>> actions;

    public PeriodicRandomActionPolicy(long periodMs, List<Pair<Action, Integer>> actions) {
      super(periodMs);
      this.actions = actions;
    }

    public PeriodicRandomActionPolicy(long periodMs, Pair<Action, Integer>... actions) {
      // We don't expect it to be modified.
      this(periodMs, Arrays.asList(actions));
    }

    public PeriodicRandomActionPolicy(long periodMs, Action... actions) {
      super(periodMs);
      this.actions = new ArrayList<Pair<Action, Integer>>(actions.length);
      for (Action action : actions) {
        this.actions.add(new Pair<Action, Integer>(action, 1));
      }
    }

    @Override
    protected void runOneIteration() {
      Action action = selectWeightedRandomItem(actions);
      try {
        action.perform();
      } catch (Exception ex) {
        LOG.warn("Exception occured during performing action: "
            + StringUtils.stringifyException(ex));
      }
    }

    @Override
    public void init(PolicyContext context) throws Exception {
      super.init(context);
      for (Pair<Action, Integer> action : actions) {
        action.getFirst().init(this.context);
      }
    }
  }

  /** Selects a random item from the given items */
  static <T> T selectRandomItem(T[] items) {
    Random random = new Random();
    return items[random.nextInt(items.length)];
  }

  /** Selects a random item from the given items with weights*/
  static <T> T selectWeightedRandomItem(List<Pair<T, Integer>> items) {
    Random random = new Random();
    int totalWeight = 0;
    for (Pair<T, Integer> pair : items) {
      totalWeight += pair.getSecond();
    }

    int cutoff = random.nextInt(totalWeight);
    int cummulative = 0;
    T item = null;

    //warn: O(n)
    for (int i=0; i<items.size(); i++) {
      int curWeight = items.get(i).getSecond();
      if ( cutoff < cummulative + curWeight) {
        item = items.get(i).getFirst();
        break;
      }
      cummulative += curWeight;
    }

    return item;
  }

  /** Selects and returns ceil(ratio * items.length) random items from the given array */
  static <T> List<T> selectRandomItems(T[] items, float ratio) {
    Random random = new Random();
    int remaining = (int)Math.ceil(items.length * ratio);

    List<T> selectedItems = new ArrayList<T>(remaining);

    for (int i=0; i<items.length && remaining > 0; i++) {
      if (random.nextFloat() < ((float)remaining/(items.length-i))) {
        selectedItems.add(items[i]);
        remaining--;
      }
    }

    return selectedItems;
  }

  /**
   * All actions that deal with RS's with the following weights (relative probabilities):
   *  - Restart active master (sleep 5 sec)                    : 2
   *  - Restart random regionserver (sleep 5 sec)              : 2
   *  - Restart random regionserver (sleep 60 sec)             : 2
   *  - Restart META regionserver (sleep 5 sec)                : 1
   *  - Restart ROOT regionserver (sleep 5 sec)                : 1
   *  - Batch restart of 50% of regionservers (sleep 5 sec)    : 2
   *  - Rolling restart of 100% of regionservers (sleep 5 sec) : 2
   */
  @SuppressWarnings("unchecked")
  private static final List<Pair<Action, Integer>> ALL_ACTIONS = Lists.newArrayList(
      new Pair<Action,Integer>(new RestartActiveMaster(FIVE_SEC), 2),
      new Pair<Action,Integer>(new RestartRandomRs(FIVE_SEC), 2),
      new Pair<Action,Integer>(new RestartRandomRs(ONE_MIN), 2),
      new Pair<Action,Integer>(new RestartRsHoldingMeta(FIVE_SEC), 1),
      new Pair<Action,Integer>(new RestartRsHoldingRoot(FIVE_SEC), 1),
      new Pair<Action,Integer>(new BatchRestartRs(FIVE_SEC, 0.5f), 2),
      new Pair<Action,Integer>(new RollingBatchRestartRs(FIVE_SEC, 1.0f), 2)
  );

  public static final String EVERY_MINUTE_RANDOM_ACTION_POLICY = "EVERY_MINUTE_RANDOM_ACTION_POLICY";

  private Policy[] policies;
  private Thread[] monkeyThreads;

  public void start() throws Exception {
    monkeyThreads = new Thread[policies.length];

    for (int i=0; i<policies.length; i++) {
      policies[i].init(new PolicyContext(this.util));
      Thread monkeyThread = new Thread(policies[i]);
      monkeyThread.start();
      monkeyThreads[i] = monkeyThread;
    }
  }

  @Override
  public void stop(String why) {
    for (Policy policy : policies) {
      policy.stop(why);
    }
  }

  @Override
  public boolean isStopped() {
    return policies[0].isStopped();
  }

  /**
   * Wait for ChaosMonkey to stop.
   * @throws InterruptedException
   */
  public void waitForStop() throws InterruptedException {
    for (Thread monkeyThread : monkeyThreads) {
      monkeyThread.join();
    }
  }

  private static final Map<String, Policy> NAMED_POLICIES = Maps.newHashMap();
  static {
    NAMED_POLICIES.put(EVERY_MINUTE_RANDOM_ACTION_POLICY,
        new PeriodicRandomActionPolicy(ONE_MIN, ALL_ACTIONS));
  }

  @Override
  protected void addOptions() {
    addOptWithArg("policy", "a named policy defined in ChaosMonkey.java. Possible values: "
        + NAMED_POLICIES.keySet());
    //we can add more options, and make policies more configurable
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    String[] policies = cmd.getOptionValues("policy");
    if (policies != null) {
      setPoliciesByName(policies);
    }
  }

  @Override
  protected int doWork() throws Exception {
    start();
    waitForStop();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    IntegrationTestingUtility util = new IntegrationTestingUtility(conf);
    util.initializeCluster(1);

    ChaosMonkey monkey = new ChaosMonkey(util, EVERY_MINUTE_RANDOM_ACTION_POLICY);
    int ret = ToolRunner.run(conf, monkey, args);
    System.exit(ret);
  }

}
