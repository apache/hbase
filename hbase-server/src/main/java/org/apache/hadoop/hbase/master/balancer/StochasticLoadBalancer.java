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
package org.apache.hadoop.hbase.master.balancer;

import com.google.errorprone.annotations.RestrictedApi;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.namequeues.BalancerDecisionDetails;
import org.apache.hadoop.hbase.namequeues.BalancerRejectionDetails;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * <p>This is a best effort load balancer. Given a Cost function F(C) =&gt; x It will
 * randomly try and mutate the cluster to Cprime. If F(Cprime) &lt; F(C) then the
 * new cluster state becomes the plan. It includes costs functions to compute the cost of:</p>
 * <ul>
 * <li>Region Load</li>
 * <li>Table Load</li>
 * <li>Data Locality</li>
 * <li>Memstore Sizes</li>
 * <li>Storefile Sizes</li>
 * </ul>
 *
 *
 * <p>Every cost function returns a number between 0 and 1 inclusive; where 0 is the lowest cost
 * best solution, and 1 is the highest possible cost and the worst solution.  The computed costs are
 * scaled by their respective multipliers:</p>
 *
 * <ul>
 *   <li>hbase.master.balancer.stochastic.regionLoadCost</li>
 *   <li>hbase.master.balancer.stochastic.moveCost</li>
 *   <li>hbase.master.balancer.stochastic.tableLoadCost</li>
 *   <li>hbase.master.balancer.stochastic.localityCost</li>
 *   <li>hbase.master.balancer.stochastic.memstoreSizeCost</li>
 *   <li>hbase.master.balancer.stochastic.storefileSizeCost</li>
 * </ul>
 *
 * <p>You can also add custom Cost function by setting the the following configuration value:</p>
 * <ul>
 *     <li>hbase.master.balancer.stochastic.additionalCostFunctions</li>
 * </ul>
 *
 * <p>All custom Cost Functions needs to extends {@link CostFunction}</p>
 *
 * <p>In addition to the above configurations, the balancer can be tuned by the following
 * configuration values:</p>
 * <ul>
 *   <li>hbase.master.balancer.stochastic.maxMoveRegions which
 *   controls what the max number of regions that can be moved in a single invocation of this
 *   balancer.</li>
 *   <li>hbase.master.balancer.stochastic.stepsPerRegion is the coefficient by which the number of
 *   regions is multiplied to try and get the number of times the balancer will
 *   mutate all servers.</li>
 *   <li>hbase.master.balancer.stochastic.maxSteps which controls the maximum number of times that
 *   the balancer will try and mutate all the servers. The balancer will use the minimum of this
 *   value and the above computation.</li>
 * </ul>
 *
 * <p>This balancer is best used with hbase.master.loadbalance.bytable set to false
 * so that the balancer gets the full picture of all loads on the cluster.</p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
  justification="Complaint is about costFunctions not being synchronized; not end of the world")
public class StochasticLoadBalancer extends BaseLoadBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(StochasticLoadBalancer.class);

  protected static final String STEPS_PER_REGION_KEY =
      "hbase.master.balancer.stochastic.stepsPerRegion";
  protected static final String MAX_STEPS_KEY =
      "hbase.master.balancer.stochastic.maxSteps";
  protected static final String RUN_MAX_STEPS_KEY =
      "hbase.master.balancer.stochastic.runMaxSteps";
  protected static final String MAX_RUNNING_TIME_KEY =
      "hbase.master.balancer.stochastic.maxRunningTime";
  protected static final String KEEP_REGION_LOADS =
      "hbase.master.balancer.stochastic.numRegionLoadsToRemember";
  private static final String TABLE_FUNCTION_SEP = "_";
  protected static final String MIN_COST_NEED_BALANCE_KEY =
      "hbase.master.balancer.stochastic.minCostNeedBalance";
  protected static final String COST_FUNCTIONS_COST_FUNCTIONS_KEY =
          "hbase.master.balancer.stochastic.additionalCostFunctions";

  Map<String, Deque<BalancerRegionLoad>> loads = new HashMap<>();

  // values are defaults
  private int maxSteps = 1000000;
  private boolean runMaxSteps = false;
  private int stepsPerRegion = 800;
  private long maxRunningTime = 30 * 1000 * 1; // 30 seconds.
  private int numRegionLoadsToRemember = 15;
  private float minCostNeedBalance = 0.05f;
  private boolean isBalancerDecisionRecording = false;
  private boolean isBalancerRejectionRecording = false;

  private List<CandidateGenerator> candidateGenerators;
  private CostFromRegionLoadFunction[] regionLoadFunctions;
  private List<CostFunction> costFunctions; // FindBugs: Wants this protected; IS2_INCONSISTENT_SYNC

  // to save and report costs to JMX
  private Double curOverallCost = 0d;
  private Double[] tempFunctionCosts;
  private Double[] curFunctionCosts;

  // Keep locality based picker and cost function to alert them
  // when new services are offered
  private LocalityBasedCandidateGenerator localityCandidateGenerator;
  private ServerLocalityCostFunction localityCost;
  private RackLocalityCostFunction rackLocalityCost;
  private RegionReplicaHostCostFunction regionReplicaHostCostFunction;
  private RegionReplicaRackCostFunction regionReplicaRackCostFunction;

  /**
   * Use to add balancer decision history to ring-buffer
   */
  NamedQueueRecorder namedQueueRecorder;

  /**
   * The constructor that pass a MetricsStochasticBalancer to BaseLoadBalancer to replace its
   * default MetricsBalancer
   */
  public StochasticLoadBalancer() {
    super(new MetricsStochasticBalancer());
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    maxSteps = conf.getInt(MAX_STEPS_KEY, maxSteps);
    stepsPerRegion = conf.getInt(STEPS_PER_REGION_KEY, stepsPerRegion);
    maxRunningTime = conf.getLong(MAX_RUNNING_TIME_KEY, maxRunningTime);
    runMaxSteps = conf.getBoolean(RUN_MAX_STEPS_KEY, runMaxSteps);

    numRegionLoadsToRemember = conf.getInt(KEEP_REGION_LOADS, numRegionLoadsToRemember);
    minCostNeedBalance = conf.getFloat(MIN_COST_NEED_BALANCE_KEY, minCostNeedBalance);
    if (localityCandidateGenerator == null) {
      localityCandidateGenerator = new LocalityBasedCandidateGenerator();
    }
    localityCost = new ServerLocalityCostFunction(conf);
    rackLocalityCost = new RackLocalityCostFunction(conf);

    if (this.candidateGenerators == null) {
      candidateGenerators = Lists.newArrayList();
      candidateGenerators.add(new RandomCandidateGenerator());
      candidateGenerators.add(new LoadCandidateGenerator());
      candidateGenerators.add(localityCandidateGenerator);
      candidateGenerators.add(new RegionReplicaRackCandidateGenerator());
    }
    regionLoadFunctions = new CostFromRegionLoadFunction[] {
      new ReadRequestCostFunction(conf),
      new CPRequestCostFunction(conf),
      new WriteRequestCostFunction(conf),
      new MemStoreSizeCostFunction(conf),
      new StoreFileCostFunction(conf)
    };
    regionReplicaHostCostFunction = new RegionReplicaHostCostFunction(conf);
    regionReplicaRackCostFunction = new RegionReplicaRackCostFunction(conf);

    costFunctions = new ArrayList<>();
    addCostFunction(new RegionCountSkewCostFunction(conf));
    addCostFunction(new PrimaryRegionCountSkewCostFunction(conf));
    addCostFunction(new MoveCostFunction(conf));
    addCostFunction(localityCost);
    addCostFunction(rackLocalityCost);
    addCostFunction(new TableSkewCostFunction(conf));
    addCostFunction(regionReplicaHostCostFunction);
    addCostFunction(regionReplicaRackCostFunction);
    addCostFunction(regionLoadFunctions[0]);
    addCostFunction(regionLoadFunctions[1]);
    addCostFunction(regionLoadFunctions[2]);
    addCostFunction(regionLoadFunctions[3]);
    addCostFunction(regionLoadFunctions[4]);
    loadCustomCostFunctions(conf);

    curFunctionCosts= new Double[costFunctions.size()];
    tempFunctionCosts= new Double[costFunctions.size()];

    isBalancerDecisionRecording = getConf()
      .getBoolean(BaseLoadBalancer.BALANCER_DECISION_BUFFER_ENABLED,
        BaseLoadBalancer.DEFAULT_BALANCER_DECISION_BUFFER_ENABLED);
    isBalancerRejectionRecording = getConf()
      .getBoolean(BaseLoadBalancer.BALANCER_REJECTION_BUFFER_ENABLED,
        BaseLoadBalancer.DEFAULT_BALANCER_REJECTION_BUFFER_ENABLED);

    if (this.namedQueueRecorder == null && (isBalancerDecisionRecording
      || isBalancerRejectionRecording)) {
      this.namedQueueRecorder = NamedQueueRecorder.getInstance(getConf());
    }

    LOG.info("Loaded config; maxSteps=" + maxSteps + ", stepsPerRegion=" + stepsPerRegion +
            ", maxRunningTime=" + maxRunningTime + ", isByTable=" + isByTable + ", CostFunctions=" +
            Arrays.toString(getCostFunctionNames()) + " etc.");
  }

  private static CostFunction createCostFunction(Class<? extends CostFunction> clazz,
    Configuration conf) {
    try {
      Constructor<? extends CostFunction> ctor = clazz.getDeclaredConstructor(Configuration.class);
      return ReflectionUtils.instantiate(clazz.getName(), ctor, conf);
    } catch (NoSuchMethodException e) {
      // will try construct with no parameter
    }
    return ReflectionUtils.newInstance(clazz);
  }

  private void loadCustomCostFunctions(Configuration conf) {
    String[] functionsNames = conf.getStrings(COST_FUNCTIONS_COST_FUNCTIONS_KEY);

    if (null == functionsNames) {
      return;
    }
    for (String className : functionsNames) {
      Class<? extends CostFunction> clazz;
      try {
        clazz = Class.forName(className).asSubclass(CostFunction.class);
      } catch (ClassNotFoundException e) {
        LOG.warn("Cannot load class '{}': {}", className, e.getMessage());
        continue;
      }
      CostFunction func = createCostFunction(clazz, conf);
      LOG.info("Successfully loaded custom CostFunction '{}'", func.getClass().getSimpleName());
      costFunctions.add(func);
    }
  }

  protected void setCandidateGenerators(List<CandidateGenerator> customCandidateGenerators) {
    this.candidateGenerators = customCandidateGenerators;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  List<CandidateGenerator> getCandidateGenerators() {
    return this.candidateGenerators;
  }

  @Override
  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", 0.001F);
  }

  @Override
  public synchronized void setClusterMetrics(ClusterMetrics st) {
    super.setClusterMetrics(st);
    updateRegionLoad();
    for (CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setClusterMetrics(st);
    }

    // update metrics size
    try {
      // by-table or ensemble mode
      int tablesCount = isByTable ? provider.getNumberOfTables() : 1;
      int functionsCount = getCostFunctionNames().length;

      updateMetricsSize(tablesCount * (functionsCount + 1)); // +1 for overall
    } catch (Exception e) {
      LOG.error("failed to get the size of all tables", e);
    }
  }

  /**
   * Update the number of metrics that are reported to JMX
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void updateMetricsSize(int size) {
    if (metricsBalancer instanceof MetricsStochasticBalancer) {
      ((MetricsStochasticBalancer) metricsBalancer).updateMetricsSize(size);
    }
  }

  @Override
  protected synchronized boolean areSomeRegionReplicasColocated(BalancerClusterState c) {
    regionReplicaHostCostFunction.init(c);
    if (regionReplicaHostCostFunction.cost() > 0) {
      return true;
    }
    regionReplicaRackCostFunction.init(c);
    if (regionReplicaRackCostFunction.cost() > 0) {
      return true;
    }
    return false;
  }

  @Override
  protected boolean needsBalance(TableName tableName, BalancerClusterState cluster) {
    ClusterLoadState cs = new ClusterLoadState(cluster.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      if (this.isBalancerRejectionRecording) {
        sendRejectionReasonToRingBuffer("The number of RegionServers " +
          cs.getNumServers() + " < MIN_SERVER_BALANCE(" + MIN_SERVER_BALANCE + ")", null);
      }
      return false;
    }
    if (areSomeRegionReplicasColocated(cluster)) {
      return true;
    }

    if (idleRegionServerExist(cluster)){
      return true;
    }

    double total = 0.0;
    float sumMultiplier = 0.0f;
    for (CostFunction c : costFunctions) {
      float multiplier = c.getMultiplier();
      if (multiplier <= 0) {
        LOG.trace("{} not needed because multiplier is <= 0", c.getClass().getSimpleName());
        continue;
      }
      if (!c.isNeeded()) {
        LOG.trace("{} not needed", c.getClass().getSimpleName());
        continue;
      }
      sumMultiplier += multiplier;
      total += c.cost() * multiplier;
    }

    boolean balanced = total <= 0 || sumMultiplier <= 0 ||
        (sumMultiplier > 0 && (total / sumMultiplier) < minCostNeedBalance);
    if(balanced && isBalancerRejectionRecording){
      String reason = "";
      if (total <= 0) {
        reason = "(cost1*multiplier1)+(cost2*multiplier2)+...+(costn*multipliern) = " + total + " <= 0";
      } else if (sumMultiplier <= 0) {
        reason = "sumMultiplier = " + sumMultiplier + " <= 0";
      } else if ((total / sumMultiplier) < minCostNeedBalance) {
        reason =
          "[(cost1*multiplier1)+(cost2*multiplier2)+...+(costn*multipliern)]/sumMultiplier = " + (total
            / sumMultiplier) + " <= minCostNeedBalance(" + minCostNeedBalance + ")";
      }
      sendRejectionReasonToRingBuffer(reason, costFunctions);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} {}; total cost={}, sum multiplier={}; cost/multiplier to need a balance is {}",
          balanced ? "Skipping load balancing because balanced" : "We need to load balance",
          isByTable ? String.format("table (%s)", tableName) : "cluster",
          total, sumMultiplier, minCostNeedBalance);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Balance decision detailed function costs={}", functionCost());
      }
    }
    return !balanced;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  BalanceAction nextAction(BalancerClusterState cluster) {
    return candidateGenerators.get(ThreadLocalRandom.current().nextInt(candidateGenerators.size()))
      .generate(cluster);
  }

  /**
   * Given the cluster state this will try and approach an optimal balance. This
   * should always approach the optimal state given enough steps.
   */
  @Override
  public synchronized List<RegionPlan> balanceTable(TableName tableName, Map<ServerName,
    List<RegionInfo>> loadOfOneTable) {
    // On clusters with lots of HFileLinks or lots of reference files,
    // instantiating the storefile infos can be quite expensive.
    // Allow turning this feature off if the locality cost is not going to
    // be used in any computations.
    RegionHDFSBlockLocationFinder finder = null;
    if ((this.localityCost != null && this.localityCost.getMultiplier() > 0)
        || (this.rackLocalityCost != null && this.rackLocalityCost.getMultiplier() > 0)) {
      finder = this.regionFinder;
    }

    //The clusterState that is given to this method contains the state
    //of all the regions in the table(s) (that's true today)
    // Keep track of servers to iterate through them.
    BalancerClusterState cluster =
      new BalancerClusterState(loadOfOneTable, loads, finder, rackManager);

    long startTime = EnvironmentEdgeManager.currentTime();

    initCosts(cluster);

    if (!needsBalance(tableName, cluster)) {
      return null;
    }

    double currentCost = computeCost(cluster, Double.MAX_VALUE);
    curOverallCost = currentCost;
    System.arraycopy(tempFunctionCosts, 0, curFunctionCosts, 0, curFunctionCosts.length);
    double initCost = currentCost;
    double newCost;

    long computedMaxSteps;
    if (runMaxSteps) {
      computedMaxSteps = Math.max(this.maxSteps,
          ((long)cluster.numRegions * (long)this.stepsPerRegion * (long)cluster.numServers));
    } else {
      long calculatedMaxSteps = (long)cluster.numRegions * (long)this.stepsPerRegion *
          (long)cluster.numServers;
      computedMaxSteps = Math.min(this.maxSteps, calculatedMaxSteps);
      if (calculatedMaxSteps > maxSteps) {
        LOG.warn("calculatedMaxSteps:{} for loadbalancer's stochastic walk is larger than "
            + "maxSteps:{}. Hence load balancing may not work well. Setting parameter "
            + "\"hbase.master.balancer.stochastic.runMaxSteps\" to true can overcome this issue."
            + "(This config change does not require service restart)", calculatedMaxSteps,
            maxSteps);
      }
    }
    LOG.info("start StochasticLoadBalancer.balancer, initCost=" + currentCost + ", functionCost="
        + functionCost() + " computedMaxSteps: " + computedMaxSteps);

    final String initFunctionTotalCosts = totalCostsPerFunc();
    // Perform a stochastic walk to see if we can get a good fit.
    long step;

    for (step = 0; step < computedMaxSteps; step++) {
      BalanceAction action = nextAction(cluster);

      if (action.getType() == BalanceAction.Type.NULL) {
        continue;
      }

      cluster.doAction(action);
      updateCostsWithAction(cluster, action);

      newCost = computeCost(cluster, currentCost);

      // Should this be kept?
      if (newCost < currentCost) {
        currentCost = newCost;

        // save for JMX
        curOverallCost = currentCost;
        System.arraycopy(tempFunctionCosts, 0, curFunctionCosts, 0, curFunctionCosts.length);
      } else {
        // Put things back the way they were before.
        // TODO: undo by remembering old values
        BalanceAction undoAction = action.undoAction();
        cluster.doAction(undoAction);
        updateCostsWithAction(cluster, undoAction);
      }

      if (EnvironmentEdgeManager.currentTime() - startTime >
          maxRunningTime) {
        break;
      }
    }
    long endTime = EnvironmentEdgeManager.currentTime();

    metricsBalancer.balanceCluster(endTime - startTime);

    // update costs metrics
    updateStochasticCosts(tableName, curOverallCost, curFunctionCosts);
    if (initCost > currentCost) {
      List<RegionPlan> plans = createRegionPlans(cluster);
      LOG.info("Finished computing new load balance plan. Computation took {}" +
        " to try {} different iterations.  Found a solution that moves " +
        "{} regions; Going from a computed cost of {}" +
        " to a new cost of {}", java.time.Duration.ofMillis(endTime - startTime),
        step, plans.size(), initCost, currentCost);
      sendRegionPlansToRingBuffer(plans, currentCost, initCost, initFunctionTotalCosts, step);
      return plans;
    }
    LOG.info("Could not find a better load balance plan.  Tried {} different configurations in " +
      "{}, and did not find anything with a computed cost less than {}", step,
      java.time.Duration.ofMillis(endTime - startTime), initCost);
    return null;
  }

  private void sendRejectionReasonToRingBuffer(String reason, List<CostFunction> costFunctions){
    if (this.isBalancerRejectionRecording){
      BalancerRejection.Builder builder =
        new BalancerRejection.Builder()
        .setReason(reason);
      if (costFunctions != null) {
        for (CostFunction c : costFunctions) {
          float multiplier = c.getMultiplier();
          if (multiplier <= 0 || !c.isNeeded()) {
            continue;
          }
          builder.addCostFuncInfo(c.getClass().getName(), c.cost(), c.getMultiplier());
        }
      }
      namedQueueRecorder.addRecord(new BalancerRejectionDetails(builder.build()));
    }
  }

  private void sendRegionPlansToRingBuffer(List<RegionPlan> plans, double currentCost,
      double initCost, String initFunctionTotalCosts, long step) {
    if (this.isBalancerDecisionRecording) {
      List<String> regionPlans = new ArrayList<>();
      for (RegionPlan plan : plans) {
        regionPlans.add(
          "table: " + plan.getRegionInfo().getTable() + " , region: " + plan.getRegionName()
            + " , source: " + plan.getSource() + " , destination: " + plan.getDestination());
      }
      BalancerDecision balancerDecision =
        new BalancerDecision.Builder()
          .setInitTotalCost(initCost)
          .setInitialFunctionCosts(initFunctionTotalCosts)
          .setComputedTotalCost(currentCost)
          .setFinalFunctionCosts(totalCostsPerFunc())
          .setComputedSteps(step)
          .setRegionPlans(regionPlans).build();
      namedQueueRecorder.addRecord(new BalancerDecisionDetails(balancerDecision));
    }
  }

  /**
   * update costs to JMX
   */
  private void updateStochasticCosts(TableName tableName, Double overall, Double[] subCosts) {
    if (tableName == null) {
      return;
    }

    // check if the metricsBalancer is MetricsStochasticBalancer before casting
    if (metricsBalancer instanceof MetricsStochasticBalancer) {
      MetricsStochasticBalancer balancer = (MetricsStochasticBalancer) metricsBalancer;
      // overall cost
      balancer.updateStochasticCost(tableName.getNameAsString(),
        "Overall", "Overall cost", overall);

      // each cost function
      for (int i = 0; i < costFunctions.size(); i++) {
        CostFunction costFunction = costFunctions.get(i);
        String costFunctionName = costFunction.getClass().getSimpleName();
        Double costPercent = (overall == 0) ? 0 : (subCosts[i] / overall);
        // TODO: cost function may need a specific description
        balancer.updateStochasticCost(tableName.getNameAsString(), costFunctionName,
          "The percent of " + costFunctionName, costPercent);
      }
    }
  }

  private void addCostFunction(CostFunction costFunction) {
    if (costFunction.getMultiplier() > 0) {
      costFunctions.add(costFunction);
    }
  }

  private String functionCost() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c:costFunctions) {
      builder.append(c.getClass().getSimpleName());
      builder.append(" : (");
      builder.append(c.getMultiplier());
      builder.append(", ");
      builder.append(c.cost());
      builder.append("); ");
    }
    return builder.toString();
  }

  private String totalCostsPerFunc() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c : costFunctions) {
      if (c.getMultiplier() * c.cost() > 0.0) {
        builder.append(" ");
        builder.append(c.getClass().getSimpleName());
        builder.append(" : ");
        builder.append(c.getMultiplier() * c.cost());
        builder.append(";");
      }
    }
    if (builder.length() > 0) {
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  /**
   * Create all of the RegionPlan's needed to move from the initial cluster state to the desired
   * state.
   *
   * @param cluster The state of the cluster
   * @return List of RegionPlan's that represent the moves needed to get to desired final state.
   */
  private List<RegionPlan> createRegionPlans(BalancerClusterState cluster) {
    List<RegionPlan> plans = new ArrayList<>();
    for (int regionIndex = 0;
         regionIndex < cluster.regionIndexToServerIndex.length; regionIndex++) {
      int initialServerIndex = cluster.initialRegionIndexToServerIndex[regionIndex];
      int newServerIndex = cluster.regionIndexToServerIndex[regionIndex];

      if (initialServerIndex != newServerIndex) {
        RegionInfo region = cluster.regions[regionIndex];
        ServerName initialServer = cluster.servers[initialServerIndex];
        ServerName newServer = cluster.servers[newServerIndex];

        if (LOG.isTraceEnabled()) {
          LOG.trace("Moving Region " + region.getEncodedName() + " from server "
              + initialServer.getHostname() + " to " + newServer.getHostname());
        }
        RegionPlan rp = new RegionPlan(region, initialServer, newServer);
        plans.add(rp);
      }
    }
    return plans;
  }

  /**
   * Store the current region loads.
   */
  private synchronized void updateRegionLoad() {
    // We create a new hashmap so that regions that are no longer there are removed.
    // However we temporarily need the old loads so we can use them to keep the rolling average.
    Map<String, Deque<BalancerRegionLoad>> oldLoads = loads;
    loads = new HashMap<>();

    clusterStatus.getLiveServerMetrics().forEach((ServerName sn, ServerMetrics sm) -> {
      sm.getRegionMetrics().forEach((byte[] regionName, RegionMetrics rm) -> {
        String regionNameAsString = RegionInfo.getRegionNameAsString(regionName);
        Deque<BalancerRegionLoad> rLoads = oldLoads.get(regionNameAsString);
        if (rLoads == null) {
          rLoads = new ArrayDeque<>(numRegionLoadsToRemember + 1);
        } else if (rLoads.size() >= numRegionLoadsToRemember) {
          rLoads.remove();
        }
        rLoads.add(new BalancerRegionLoad(rm));
        loads.put(regionNameAsString, rLoads);
      });
    });

    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setLoads(loads);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void initCosts(BalancerClusterState cluster) {
    for (CostFunction c:costFunctions) {
      c.init(cluster);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void updateCostsWithAction(BalancerClusterState cluster, BalanceAction action) {
    for (CostFunction c : costFunctions) {
      c.postAction(action);
    }
  }

  /**
   * Get the names of the cost functions
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  String[] getCostFunctionNames() {
    if (costFunctions == null) {
      return null;
    }
    String[] ret = new String[costFunctions.size()];
    for (int i = 0; i < costFunctions.size(); i++) {
      CostFunction c = costFunctions.get(i);
      ret[i] = c.getClass().getSimpleName();
    }

    return ret;
  }

  /**
   * This is the main cost function.  It will compute a cost associated with a proposed cluster
   * state.  All different costs will be combined with their multipliers to produce a double cost.
   *
   * @param cluster The state of the cluster
   * @param previousCost the previous cost. This is used as an early out.
   * @return a double of a cost associated with the proposed cluster state.  This cost is an
   *         aggregate of all individual cost functions.
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  double computeCost(BalancerClusterState cluster, double previousCost) {
    double total = 0;

    for (int i = 0; i < costFunctions.size(); i++) {
      CostFunction c = costFunctions.get(i);
      this.tempFunctionCosts[i] = 0.0;

      if (c.getMultiplier() <= 0) {
        continue;
      }

      Float multiplier = c.getMultiplier();
      Double cost = c.cost();

      this.tempFunctionCosts[i] = multiplier*cost;
      total += this.tempFunctionCosts[i];

      if (total > previousCost) {
        break;
      }
    }

    return total;
  }

  /**
   * A helper function to compose the attribute name from tablename and costfunction name
   */
  static String composeAttributeName(String tableName, String costFunctionName) {
    return tableName + TABLE_FUNCTION_SEP + costFunctionName;
  }
}
