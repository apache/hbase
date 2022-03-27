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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action.Type;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.AssignRegionAction;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.LocalityType;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.MoveRegionAction;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.SwapRegionsAction;
import org.apache.hadoop.hbase.namequeues.BalancerDecisionDetails;
import org.apache.hadoop.hbase.namequeues.BalancerRejectionDetails;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
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
 * <p>All custom Cost Functions needs to extends {@link StochasticLoadBalancer.CostFunction}</p>
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

  protected static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Logger LOG = LoggerFactory.getLogger(StochasticLoadBalancer.class);
  public static final double COST_EPSILON = 0.0001;

  Map<String, Deque<BalancerRegionLoad>> loads = new HashMap<>();

  // values are defaults
  private int maxSteps = 1000000;
  private boolean runMaxSteps = false;
  private int stepsPerRegion = 800;
  private long maxRunningTime = 30 * 1000 * 1; // 30 seconds.
  private int numRegionLoadsToRemember = 15;
  private float minCostNeedBalance = 0.025f;
  private boolean isBalancerDecisionRecording = false;
  private boolean isBalancerRejectionRecording = false;

  protected List<CandidateGenerator> candidateGenerators;

  public enum GeneratorType {
    RANDOM, LOAD, LOCALITY, RACK
  }
  private double[] weightsOfGenerators;
  private List<CostFunction> costFunctions; // FindBugs: Wants this protected; IS2_INCONSISTENT_SYNC
  private float sumMultiplier;
  // to save and report costs to JMX
  private double curOverallCost = 0d;
  private double[] tempFunctionCosts;
  private double[] curFunctionCosts;

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
      candidateGenerators.add(GeneratorType.RANDOM.ordinal(), new RandomCandidateGenerator());
      candidateGenerators.add(GeneratorType.LOAD.ordinal(), new LoadCandidateGenerator());
      candidateGenerators.add(GeneratorType.LOCALITY.ordinal(), localityCandidateGenerator);
      candidateGenerators.add(GeneratorType.RACK.ordinal(),
        new RegionReplicaRackCandidateGenerator());
    }
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
    addCostFunction(new ReadRequestCostFunction(conf));
    addCostFunction(new WriteRequestCostFunction(conf));
    addCostFunction(new MemStoreSizeCostFunction(conf));
    addCostFunction(new StoreFileCostFunction(conf));
    loadCustomCostFunctions(conf);

    curFunctionCosts = new double[costFunctions.size()];
    tempFunctionCosts = new double[costFunctions.size()];

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

    LOG.info(
      "Loaded config; maxSteps=" + maxSteps + ", runMaxSteps=" + runMaxSteps +
        ", stepsPerRegion=" + stepsPerRegion +
        ", maxRunningTime=" + maxRunningTime + ", isByTable=" + isByTable +
        ", CostFunctions=" + Arrays.toString(getCostFunctionNames()) +
        " , sum of multiplier of cost functions = " + sumMultiplier + " etc.");
  }

  private void loadCustomCostFunctions(Configuration conf) {
    String[] functionsNames = conf.getStrings(COST_FUNCTIONS_COST_FUNCTIONS_KEY);

    if (null == functionsNames) {
      return;
    }

    costFunctions.addAll(Arrays.stream(functionsNames).map(c -> {
      Class<? extends CostFunction> klass = null;
      try {
        klass = Class.forName(c).asSubclass(CostFunction.class);
      } catch (ClassNotFoundException e) {
        LOG.warn("Cannot load class " + c + "': " + e.getMessage());
      }
      if (null == klass) {
        return null;
      }
      CostFunction reflected = ReflectionUtils.newInstance(klass, conf);
      LOG.info(
        "Successfully loaded custom CostFunction '" + reflected.getClass().getSimpleName() + "'");
      return reflected;
    }).filter(Objects::nonNull).collect(Collectors.toList()));
  }

  protected void setCandidateGenerators(List<CandidateGenerator> customCandidateGenerators) {
    this.candidateGenerators = customCandidateGenerators;
  }

  /**
   * Exposed for Testing!
   */
  public List<CandidateGenerator> getCandidateGenerators() {
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

    // update metrics size
    try {
      // by-table or ensemble mode
      int tablesCount = isByTable ? services.getTableDescriptors().getAll().size() : 1;
      int functionsCount = getCostFunctionNames().length;

      updateMetricsSize(tablesCount * (functionsCount + 1)); // +1 for overall
    } catch (Exception e) {
      LOG.error("failed to get the size of all tables", e);
    }
  }

  /**
   * Update the number of metrics that are reported to JMX
   */
  public void updateMetricsSize(int size) {
    if (metricsBalancer instanceof MetricsStochasticBalancer) {
        ((MetricsStochasticBalancer) metricsBalancer).updateMetricsSize(size);
    }
  }

  @Override
  protected synchronized boolean areSomeRegionReplicasColocated(Cluster c) {
    regionReplicaHostCostFunction.init(c);
    if (Math.abs(regionReplicaHostCostFunction.cost()) > CostFunction.COST_EPSILON) {
      return true;
    }
    return (Math.abs(regionReplicaRackCostFunction.cost()) > CostFunction.COST_EPSILON);
  }

  @Override
  protected boolean needsBalance(TableName tableName, Cluster cluster) {
    ClusterLoadState cs = new ClusterLoadState(cluster.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      LOG.info("Not running balancer because only " + cs.getNumServers() +
        " active regionserver(s)");
      sendRejectionReasonToRingBuffer("The number of RegionServers " + cs.getNumServers() +
        " < MIN_SERVER_BALANCE(" + MIN_SERVER_BALANCE + ")", null);
      return false;
    }
    if (areSomeRegionReplicasColocated(cluster)) {
      LOG.info("Running balancer because at least one server hosts replicas of the same region." +
        " function cost={}", functionCost());
      return true;
    }

    if (idleRegionServerExist(cluster)){
      LOG.info("Running balancer because cluster has idle server(s)."+
        " function cost={}", functionCost());
      return true;
    }

    double total = 0.0;
    for (CostFunction c : costFunctions) {
      if (!c.isNeeded()) {
        LOG.trace("{} not needed", c.getClass().getSimpleName());
        continue;
      }
      total += c.cost() * c.getMultiplier();
    }

    boolean balanced = (total / sumMultiplier < minCostNeedBalance);
    if (balanced) {
      if (isBalancerRejectionRecording) {
        String reason = "";
        if (total <= 0) {
          reason =
            "(cost1*multiplier1)+(cost2*multiplier2)+...+(costn*multipliern) = " + total + " <= 0";
        } else if (sumMultiplier <= 0) {
          reason = "sumMultiplier = " + sumMultiplier + " <= 0";
        } else if ((total / sumMultiplier) < minCostNeedBalance) {
          reason =
            "[(cost1*multiplier1)+(cost2*multiplier2)+...+(costn*multipliern)]/sumMultiplier = " + (
              total / sumMultiplier) + " <= minCostNeedBalance(" + minCostNeedBalance + ")";
        }
        sendRejectionReasonToRingBuffer(reason, costFunctions);
      }
      LOG.info("{} - skipping load balancing because weighted average imbalance={} <= " +
          "threshold({}). If you want more aggressive balancing, either lower "
          + "hbase.master.balancer.stochastic.minCostNeedBalance from {} or increase the relative"
          + " multiplier(s) of the specific cost function(s). functionCost={}",
        isByTable ? "Table specific (" + tableName + ")" : "Cluster wide", total / sumMultiplier,
        minCostNeedBalance, minCostNeedBalance, functionCost());
    } else {
      LOG.info("{} - Calculating plan. may take up to {}ms to complete.",
        isByTable ? "Table specific (" + tableName + ")" : "Cluster wide", maxRunningTime);
    }
    return !balanced;
  }

  @InterfaceAudience.Private
  Cluster.Action nextAction(Cluster cluster) {
    return getRandomGenerator().generate(cluster);
  }

  /**
   * Select the candidate generator to use based on the cost of cost functions. The chance of
   * selecting a candidate generator is propotional to the share of cost of all cost functions among
   * all cost functions that benefit from it.
   */
  protected CandidateGenerator getRandomGenerator() {
    double sum = 0;
    for (int i = 0; i < weightsOfGenerators.length; i++) {
      sum += weightsOfGenerators[i];
      weightsOfGenerators[i] = sum;
    }
    if (sum == 0) {
      return candidateGenerators.get(0);
    }
    for (int i = 0; i < weightsOfGenerators.length; i++) {
      weightsOfGenerators[i] /= sum;
    }
    double rand = RANDOM.nextDouble();
    for (int i = 0; i < weightsOfGenerators.length; i++) {
      if (rand <= weightsOfGenerators[i]) {
        return candidateGenerators.get(i);
      }
    }
    return candidateGenerators.get(candidateGenerators.size() - 1);
  }

  /**
   * Given the cluster state this will try and approach an optimal balance. This
   * should always approach the optimal state given enough steps.
   */
  @Override
  public synchronized List<RegionPlan> balanceTable(TableName tableName, Map<ServerName,
    List<RegionInfo>> loadOfOneTable) {
    List<RegionPlan> plans = balanceMasterRegions(loadOfOneTable);
    if (plans != null || loadOfOneTable == null || loadOfOneTable.size() <= 1) {
      return plans;
    }

    if (masterServerName != null && loadOfOneTable.containsKey(masterServerName)) {
      if (loadOfOneTable.size() <= 2) {
        return null;
      }
      loadOfOneTable = new HashMap<>(loadOfOneTable);
      loadOfOneTable.remove(masterServerName);
    }

    // On clusters with lots of HFileLinks or lots of reference files,
    // instantiating the storefile infos can be quite expensive.
    // Allow turning this feature off if the locality cost is not going to
    // be used in any computations.
    RegionLocationFinder finder = null;
    if ((this.localityCost != null && this.localityCost.getMultiplier() > 0) || (
      this.rackLocalityCost != null && this.rackLocalityCost.getMultiplier() > 0)) {
      finder = this.regionFinder;
    }

    //The clusterState that is given to this method contains the state
    //of all the regions in the table(s) (that's true today)
    // Keep track of servers to iterate through them.
    Cluster cluster = new Cluster(loadOfOneTable, loads, finder, rackManager);

    long startTime = EnvironmentEdgeManager.currentTime();

    initCosts(cluster);
    sumMultiplier = 0;
    for (CostFunction c : costFunctions) {
      if(c.isNeeded()) {
        sumMultiplier += c.getMultiplier();
      }
    }
    if (sumMultiplier <= 0) {
      LOG.error("At least one cost function needs a multiplier > 0. For example, set "
        + "hbase.master.balancer.stochastic.regionCountCost to a positive value or default");
      return null;
    }

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
        ((long) cluster.numRegions * (long) this.stepsPerRegion * (long) cluster.numServers));
    } else {
      long calculatedMaxSteps =
        (long) cluster.numRegions * (long) this.stepsPerRegion * (long) cluster.numServers;
      computedMaxSteps = Math.min(this.maxSteps, calculatedMaxSteps);
      if (calculatedMaxSteps > maxSteps) {
        LOG.warn("calculatedMaxSteps:{} for loadbalancer's stochastic walk is larger than " +
            "maxSteps:{}. Hence load balancing may not work well. Setting parameter " +
            "\"hbase.master.balancer.stochastic.runMaxSteps\" to true can overcome this issue." +
            "(This config change does not require service restart)", calculatedMaxSteps,
          maxSteps);
      }
    }
    LOG.info("Start StochasticLoadBalancer.balancer, initial weighted average imbalance={}," +
        " functionCost={} computedMaxSteps={}",
      currentCost / sumMultiplier, functionCost(), computedMaxSteps);

    final String initFunctionTotalCosts = totalCostsPerFunc();
    // Perform a stochastic walk to see if we can get a good fit.
    long step;

    for (step = 0; step < computedMaxSteps; step++) {
      Cluster.Action action = nextAction(cluster);

      if (action.type == Type.NULL) {
        continue;
      }

      cluster.doAction(action);
      updateCostsAndWeightsWithAction(cluster, action);

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
        Action undoAction = action.undoAction();
        cluster.doAction(undoAction);
        updateCostsAndWeightsWithAction(cluster, undoAction);
      }

      if (EnvironmentEdgeManager.currentTime() - startTime > maxRunningTime) {
        break;
      }
    }
    long endTime = EnvironmentEdgeManager.currentTime();

    metricsBalancer.balanceCluster(endTime - startTime);

    // update costs metrics
    updateStochasticCosts(tableName, curOverallCost, curFunctionCosts);
    if (initCost > currentCost) {
      plans = createRegionPlans(cluster);
      LOG.info("Finished computing new moving plan. Computation took {} ms" +
          " to try {} different iterations.  Found a solution that moves " +
          "{} regions; Going from a computed imbalance of {}" +
        " to a new imbalance of {}. funtionCost={}",
        endTime - startTime, step, plans.size(),
        initCost / sumMultiplier, currentCost / sumMultiplier, functionCost());

      sendRegionPlansToRingBuffer(plans, currentCost, initCost, initFunctionTotalCosts, step);
      return plans;
    }
    LOG.info("Could not find a better moving plan.  Tried {} different configurations in "
        + "{} ms, and did not find anything with an imbalance score less than {}", step,
      endTime - startTime, initCost / sumMultiplier);
    return null;
  }

  private void sendRejectionReasonToRingBuffer(String reason, List<CostFunction> costFunctions){
    if (this.isBalancerRejectionRecording){
      BalancerRejection.Builder builder =
        new BalancerRejection.Builder()
        .setReason(reason);
      if (costFunctions != null) {
        for (CostFunction c : costFunctions) {
          if (!c.isNeeded()) {
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
  private void updateStochasticCosts(TableName tableName, double overall, double[] subCosts) {
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
        double costPercent = (overall == 0) ? 0 : (subCosts[i] / overall);
        // TODO: cost function may need a specific description
        balancer.updateStochasticCost(tableName.getNameAsString(), costFunctionName,
          "The percent of " + costFunctionName, costPercent);
      }
    }
  }

  private void addCostFunction(CostFunction costFunction) {
    float multiplier = costFunction.getMultiplier();
    if (multiplier > 0) {
      costFunctions.add(costFunction);
    }
  }

  private String functionCost() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c : costFunctions) {
      builder.append(c.getClass().getSimpleName());
      builder.append(" : (");
      if (c.isNeeded()) {
        builder.append("multiplier=" + c.getMultiplier());
        builder.append(", ");
        double cost = c.cost();
        builder.append("imbalance=" + cost);
        if (cost >= minCostNeedBalance) {
          builder.append(", need balance");
        }
      } else {
        builder.append("not needed");
      }
      builder.append("); ");
    }
    return builder.toString();
  }

  private String totalCostsPerFunc() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c : costFunctions) {
      if (!c.isNeeded()) {
        continue;
      }
      double cost = c.getMultiplier() * c.cost();
      if (cost > 0.0) {
        builder.append(" ");
        builder.append(c.getClass().getSimpleName());
        builder.append(" : ");
        builder.append(cost);
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
  private List<RegionPlan> createRegionPlans(Cluster cluster) {
    List<RegionPlan> plans = new LinkedList<>();
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
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void initCosts(Cluster cluster) {
    // Initialize the weights of generator every time
    weightsOfGenerators = new double[this.candidateGenerators.size()];
    for (CostFunction c : costFunctions) {
      c.init(cluster);
      c.updateWeight(weightsOfGenerators);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void updateCostsAndWeightsWithAction(Cluster cluster, Action action) {
    // Reset all the weights to 0
    for (int i = 0; i < weightsOfGenerators.length; i++) {
      weightsOfGenerators[i] = 0;
    }
    for (CostFunction c : costFunctions) {
      if (c.isNeeded()) {
        c.postAction(action);
        c.updateWeight(weightsOfGenerators);
      }
    }
  }

  /**
   * Get the names of the cost functions
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  String[] getCostFunctionNames() {
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
  double computeCost(Cluster cluster, double previousCost) {
    double total = 0;

    for (int i = 0; i < costFunctions.size(); i++) {
      CostFunction c = costFunctions.get(i);
      this.tempFunctionCosts[i] = 0.0;

      if (!c.isNeeded()) {
        continue;
      }

      Float multiplier = c.getMultiplier();
      double cost = c.cost();

      this.tempFunctionCosts[i] = multiplier*cost;
      total += this.tempFunctionCosts[i];

      if (total > previousCost) {
        break;
      }
    }

    return total;
  }

  static class RandomCandidateGenerator extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {

      int thisServer = pickRandomServer(cluster);

      // Pick the other server
      int otherServer = pickOtherRandomServer(cluster, thisServer);

      return pickRandomRegions(cluster, thisServer, otherServer);
    }
  }

  /**
   * Generates candidates which moves the replicas out of the rack for
   * co-hosted region replicas in the same rack
   */
  static class RegionReplicaRackCandidateGenerator extends RegionReplicaCandidateGenerator {
    @Override
    Cluster.Action generate(Cluster cluster) {
      int rackIndex = pickRandomRack(cluster);
      if (cluster.numRacks <= 1 || rackIndex == -1) {
        return super.generate(cluster);
      }

      int regionIndex = selectCoHostedRegionPerGroup(
        cluster.primariesOfRegionsPerRack[rackIndex],
        cluster.regionsPerRack[rackIndex],
        cluster.regionIndexToPrimaryIndex);

      // if there are no pairs of region replicas co-hosted, default to random generator
      if (regionIndex == -1) {
        // default to randompicker
        return randomGenerator.generate(cluster);
      }

      int serverIndex = cluster.regionIndexToServerIndex[regionIndex];
      int toRackIndex = pickOtherRandomRack(cluster, rackIndex);

      int rand = RANDOM.nextInt(cluster.serversPerRack[toRackIndex].length);
      int toServerIndex = cluster.serversPerRack[toRackIndex][rand];
      int toRegionIndex = pickRandomRegion(cluster, toServerIndex, 0.9f);
      return getAction(serverIndex, regionIndex, toServerIndex, toRegionIndex);
    }
  }

  /**
   * Base class of StochasticLoadBalancer's Cost Functions.
   */
  public abstract static class CostFunction {

    public static final double COST_EPSILON = 0.0001;

    private float multiplier = 0;

    protected Cluster cluster;

    public CostFunction(Configuration c) {
    }

    boolean isNeeded() {
      return true;
    }
    float getMultiplier() {
      return multiplier;
    }

    void setMultiplier(float m) {
      this.multiplier = m;
    }

    /** Called once per LB invocation to give the cost function
     * to initialize it's state, and perform any costly calculation.
     */
    void init(Cluster cluster) {
      this.cluster = cluster;
    }

    /** Called once per cluster Action to give the cost function
     * an opportunity to update it's state. postAction() is always
     * called at least once before cost() is called with the cluster
     * that this action is performed on. */
    void postAction(Action action) {
      switch (action.type) {
      case NULL: break;
      case ASSIGN_REGION:
        AssignRegionAction ar = (AssignRegionAction) action;
        regionMoved(ar.region, -1, ar.server);
        break;
      case MOVE_REGION:
        MoveRegionAction mra = (MoveRegionAction) action;
        regionMoved(mra.region, mra.fromServer, mra.toServer);
        break;
      case SWAP_REGIONS:
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionMoved(a.fromRegion, a.fromServer, a.toServer);
        regionMoved(a.toRegion, a.toServer, a.fromServer);
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.type);
      }
    }

    protected void regionMoved(int region, int oldServer, int newServer) {
    }

    protected abstract double cost();

    /**
     * Add the cost of this cost function to the weight of the candidate generator that is optimized
     * for this cost function. By default it is the RandomCandiateGenerator for a cost function.
     * Called once per init or after postAction.
     * @param weights the weights for every generator.
     */
    public void updateWeight(double[] weights) {
      weights[StochasticLoadBalancer.GeneratorType.RANDOM.ordinal()] += cost();
    }
  }

  /**
   * Given the starting state of the regions and a potential ending state
   * compute cost based upon the number of regions that have moved.
   */
  static class MoveCostFunction extends CostFunction {
    private static final String MOVE_COST_KEY = "hbase.master.balancer.stochastic.moveCost";
    private static final String MOVE_COST_OFFPEAK_KEY =
      "hbase.master.balancer.stochastic.moveCost.offpeak";
    private static final String MAX_MOVES_PERCENT_KEY =
        "hbase.master.balancer.stochastic.maxMovePercent";
    static final float DEFAULT_MOVE_COST = 7;
    static final float DEFAULT_MOVE_COST_OFFPEAK = 3;
    private static final int DEFAULT_MAX_MOVES = 600;
    private static final float DEFAULT_MAX_MOVE_PERCENT = 1.0f;

    private final float maxMovesPercent;
    private final OffPeakHours offPeakHours;
    private final float moveCost;
    private final float moveCostOffPeak;

    MoveCostFunction(Configuration conf) {
      super(conf);
      // What percent of the number of regions a single run of the balancer can move.
      maxMovesPercent = conf.getFloat(MAX_MOVES_PERCENT_KEY, DEFAULT_MAX_MOVE_PERCENT);
      offPeakHours = OffPeakHours.getInstance(conf);
      moveCost = conf.getFloat(MOVE_COST_KEY, DEFAULT_MOVE_COST);
      moveCostOffPeak = conf.getFloat(MOVE_COST_OFFPEAK_KEY, DEFAULT_MOVE_COST_OFFPEAK);
      // Initialize the multiplier so that addCostFunction will add this cost function.
      // It may change during later evaluations, due to OffPeakHours.
      this.setMultiplier(moveCost);
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      // Move cost multiplier should be the same cost or higher than the rest of the costs to ensure
      // that large benefits are need to overcome the cost of a move.
      if (offPeakHours.isOffPeakHour()) {
        this.setMultiplier(moveCostOffPeak);
      } else {
        this.setMultiplier(moveCost);
      }
    }

    @Override
    protected double cost() {
      // Try and size the max number of Moves, but always be prepared to move some.
      int maxMoves = Math.max((int) (cluster.numRegions * maxMovesPercent),
          DEFAULT_MAX_MOVES);

      double moveCost = cluster.numMovedRegions;

      // Don't let this single balance move more than the max moves.
      // This allows better scaling to accurately represent the actual cost of a move.
      if (moveCost > maxMoves) {
        return 1000000;   // return a number much greater than any of the other cost
      }

      return scale(0, Math.min(cluster.numRegions, maxMoves), moveCost);
    }
  }

  /**
   * Compute the cost of a potential cluster state from skew in number of
   * regions on a cluster.
   */
  static class RegionCountSkewCostFunction extends CostFunction {
    static final String REGION_COUNT_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.regionCountCost";
    static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;

    private final DoubleArrayCost cost = new DoubleArrayCost();

    RegionCountSkewCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as it is the most general way to balance data.
      this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      cost.prepare(cluster.numServers);
      cost.applyCostsChange(costs -> {
        for (int i = 0; i < cluster.numServers; i++) {
          costs[i] = cluster.regionsPerServer[i].length;
        }
      });
    }

    @Override
    protected double cost() {
      return cost.cost();
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      cost.applyCostsChange(costs -> {
        costs[oldServer] = cluster.regionsPerServer[oldServer].length;
        costs[newServer] = cluster.regionsPerServer[newServer].length;
      });
    }

    @Override
    public final void updateWeight(double[] weights) {
      weights[StochasticLoadBalancer.GeneratorType.LOAD.ordinal()] += cost();
    }
  }

  /**
   * Compute the cost of a potential cluster state from skew in number of
   * primary regions on a cluster.
   */
  static class PrimaryRegionCountSkewCostFunction extends CostFunction {
    private static final String PRIMARY_REGION_COUNT_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.primaryRegionCountCost";
    private static final float DEFAULT_PRIMARY_REGION_COUNT_SKEW_COST = 500;

    private final DoubleArrayCost cost = new DoubleArrayCost();

    PrimaryRegionCountSkewCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as primary regions serve majority of reads/writes.
      this.setMultiplier(conf.getFloat(PRIMARY_REGION_COUNT_SKEW_COST_KEY,
        DEFAULT_PRIMARY_REGION_COUNT_SKEW_COST));
    }

    private double computeCostForRegionServer(int regionServerIndex) {
      int cost = 0;
      for (int regionIdx : cluster.regionsPerServer[regionServerIndex]) {
        if (regionIdx == cluster.regionIndexToPrimaryIndex[regionIdx]) {
          cost++;
        }
      }
      return cost;
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      if (!isNeeded()) {
        return;
      }
      cost.prepare(cluster.numServers);
      cost.applyCostsChange(costs -> {
        for (int i = 0; i < costs.length; i++) {
          costs[i] = computeCostForRegionServer(i);
        }
      });
    }

    @Override
    boolean isNeeded() {
      return cluster.hasRegionReplicas;
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      cost.applyCostsChange(costs -> {
        costs[oldServer] = computeCostForRegionServer(oldServer);
        costs[newServer] = computeCostForRegionServer(newServer);
      });
    }
    @Override
    protected double cost() {
      return cost.cost();
    }
  }

  /**
   * Compute the cost of a potential cluster configuration based upon how evenly
   * distributed tables are.
   */
  static class TableSkewCostFunction extends CostFunction {

    private static final String TABLE_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.tableSkewCost";
    private static final float DEFAULT_TABLE_SKEW_COST = 35;
    DoubleArrayCost[] costsPerTable;

    TableSkewCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(TABLE_SKEW_COST_KEY, DEFAULT_TABLE_SKEW_COST));
    }
    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      costsPerTable = new DoubleArrayCost[cluster.numTables];
      for (int tableIdx = 0; tableIdx < cluster.numTables; tableIdx++) {
        costsPerTable[tableIdx] = new DoubleArrayCost();
        costsPerTable[tableIdx].prepare(cluster.numServers);
        final int tableIndex = tableIdx;
        costsPerTable[tableIdx].applyCostsChange(costs -> {
          // Keep a cached deep copy for change-only recomputation
          for (int i = 0; i < cluster.numServers; i++) {
            costs[i] = cluster.numRegionsPerServerPerTable[tableIndex][i];
          }
        });
      }
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      int tableIdx = cluster.regionIndexToTableIndex[region];
      costsPerTable[tableIdx].applyCostsChange(costs -> {
        costs[oldServer] = cluster.numRegionsPerServerPerTable[tableIdx][oldServer];
        costs[newServer] = cluster.numRegionsPerServerPerTable[tableIdx][newServer];
      });
    }

    @Override
    protected double cost() {
      double cost = 0;
      for (int tableIdx = 0; tableIdx < cluster.numTables; tableIdx++) {
        cost += costsPerTable[tableIdx].cost();
      }
      return cost;
    }
  }

  /**
   * Compute a cost of a potential cluster configuration based upon where
   * {@link org.apache.hadoop.hbase.regionserver.HStoreFile}s are located.
   */
  static abstract class LocalityBasedCostFunction extends CostFunction {

    private final LocalityType type;

    private double bestLocality; // best case locality across cluster weighted by local data size
    private double locality; // current locality across cluster weighted by local data size

    LocalityBasedCostFunction(Configuration conf, LocalityType type, String localityCostKey,
      float defaultLocalityCost) {
      super(conf);
      this.type = type;
      this.setMultiplier(conf.getFloat(localityCostKey, defaultLocalityCost));
      this.locality = 0.0;
      this.bestLocality = 0.0;
    }

    /**
     * Maps region to the current entity (server or rack) on which it is stored
     */
    abstract int regionIndexToEntityIndex(int region);

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      locality = 0.0;
      bestLocality = 0.0;

      for (int region = 0; region < cluster.numRegions; region++) {
        locality += getWeightedLocality(region, regionIndexToEntityIndex(region));
        bestLocality += getWeightedLocality(region, getMostLocalEntityForRegion(region));
      }

      // We normalize locality to be a score between 0 and 1.0 representing how good it
      // is compared to how good it could be. If bestLocality is 0, assume locality is 100
      // (and the cost is 0)
      locality = bestLocality == 0 ? 1.0 : locality / bestLocality;
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      int oldEntity = type == LocalityType.SERVER ? oldServer : cluster.serverIndexToRackIndex[oldServer];
      int newEntity = type == LocalityType.SERVER ? newServer : cluster.serverIndexToRackIndex[newServer];
      double localityDelta = getWeightedLocality(region, newEntity) - getWeightedLocality(region, oldEntity);
      double normalizedDelta = bestLocality == 0 ? 0.0 : localityDelta / bestLocality;
      locality += normalizedDelta;
    }

    @Override
    protected double cost() {
      return 1 - locality;
    }

    private int getMostLocalEntityForRegion(int region) {
      return cluster.getOrComputeRegionsToMostLocalEntities(type)[region];
    }

    private double getWeightedLocality(int region, int entity) {
      return cluster.getOrComputeWeightedLocality(region, entity, type);
    }

    @Override
    public final void updateWeight(double[] weights) {
      weights[StochasticLoadBalancer.GeneratorType.LOCALITY.ordinal()] += cost();
    }
  }

  static class ServerLocalityCostFunction extends LocalityBasedCostFunction {

    private static final String LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.localityCost";
    private static final float DEFAULT_LOCALITY_COST = 25;

    ServerLocalityCostFunction(Configuration conf) {
      super(conf, LocalityType.SERVER, LOCALITY_COST_KEY, DEFAULT_LOCALITY_COST);
    }

    @Override
    int regionIndexToEntityIndex(int region) {
      return cluster.regionIndexToServerIndex[region];
    }
  }

  static class RackLocalityCostFunction extends LocalityBasedCostFunction {

    private static final String RACK_LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.rackLocalityCost";
    private static final float DEFAULT_RACK_LOCALITY_COST = 15;

    public RackLocalityCostFunction(Configuration conf) {
      super(conf, LocalityType.RACK, RACK_LOCALITY_COST_KEY, DEFAULT_RACK_LOCALITY_COST);
    }

    @Override
    int regionIndexToEntityIndex(int region) {
      return cluster.getRackForRegion(region);
    }
  }

  /**
   * Base class the allows writing costs functions from rolling average of some
   * number from RegionLoad.
   */
  abstract static class CostFromRegionLoadFunction extends CostFunction {

    private final DoubleArrayCost cost = new DoubleArrayCost();

    CostFromRegionLoadFunction(Configuration conf) {
      super(conf);
    }

    private double computeCostForRegionServer(int regionServerIndex) {
      // Cost this server has from RegionLoad
      double cost = 0;

      // for every region on this server get the rl
      for (int regionIndex : cluster.regionsPerServer[regionServerIndex]) {
        Collection<BalancerRegionLoad> regionLoadList = cluster.regionLoads[regionIndex];

        // Now if we found a region load get the type of cost that was requested.
        if (regionLoadList != null) {
          cost += getRegionLoadCost(regionLoadList);
        }
      }
      return cost;
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      cost.prepare(cluster.numServers);
      cost.applyCostsChange(costs -> {
        for (int i = 0; i < costs.length; i++) {
          costs[i] = computeCostForRegionServer(i);
        }
      });
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      // recompute the stat for the given two region servers
      cost.applyCostsChange(costs -> {
        costs[oldServer] = computeCostForRegionServer(oldServer);
        costs[newServer] = computeCostForRegionServer(newServer);
      });
    }

    @Override
    protected final double cost() {
      return cost.cost();
    }

    protected double getRegionLoadCost(Collection<BalancerRegionLoad> regionLoadList) {
      double cost = 0;
      for (BalancerRegionLoad rl : regionLoadList) {
        cost += getCostFromRl(rl);
      }
      return cost / regionLoadList.size();
    }

    protected abstract double getCostFromRl(BalancerRegionLoad rl);
  }

  /**
   * Class to be used for the subset of RegionLoad csts that should be treated as rates.
   * We do not compare about the actual rate in requests per second but rather the rate relative
   * to the rest of the regions.
   */
  abstract static class CostFromRegionLoadAsRateFunction extends CostFromRegionLoadFunction {

    CostFromRegionLoadAsRateFunction(Configuration conf) {
      super(conf);
    }

    @Override
    protected double getRegionLoadCost(Collection<BalancerRegionLoad> regionLoadList) {
      Iterator<BalancerRegionLoad> iter = regionLoadList.iterator();
      if (!iter.hasNext()) {
        return 0;
      }
      double previous = getCostFromRl(iter.next());
      if (!iter.hasNext()) {
        return 0;
      }
      double cost = 0;
      do {
        double current = getCostFromRl(iter.next());
        cost += current >= previous ? current - previous : current;
        previous = current;
      } while (iter.hasNext());
      return Math.max(0, cost / (regionLoadList.size() - 1));
    }
  }

  /**
   * Compute the cost of total number of read requests  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */

  static class ReadRequestCostFunction extends CostFromRegionLoadAsRateFunction {

    private static final String READ_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.readRequestCost";
    private static final float DEFAULT_READ_REQUEST_COST = 5;

    ReadRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(READ_REQUEST_COST_KEY, DEFAULT_READ_REQUEST_COST));
    }

    @Override
    protected double getCostFromRl(BalancerRegionLoad rl) {
      return rl.getReadRequestsCount();
    }
  }

  /**
   * Compute the cost of total number of write requests.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  static class WriteRequestCostFunction extends CostFromRegionLoadAsRateFunction {

    private static final String WRITE_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.writeRequestCost";
    private static final float DEFAULT_WRITE_REQUEST_COST = 5;

    WriteRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(WRITE_REQUEST_COST_KEY, DEFAULT_WRITE_REQUEST_COST));
    }

    @Override
    protected double getCostFromRl(BalancerRegionLoad rl) {
      return rl.getWriteRequestsCount();
    }
  }

  /**
   * A cost function for region replicas. We give a very high cost to hosting
   * replicas of the same region in the same host. We do not prevent the case
   * though, since if numReplicas > numRegionServers, we still want to keep the
   * replica open.
   */
  static class RegionReplicaHostCostFunction extends CostFunction {
    private static final String REGION_REPLICA_HOST_COST_KEY =
        "hbase.master.balancer.stochastic.regionReplicaHostCostKey";
    private static final float DEFAULT_REGION_REPLICA_HOST_COST_KEY = 100000;

    long maxCost = 0;
    long[] costsPerGroup; // group is either server, host or rack
    int[][] primariesOfRegionsPerGroup;

    public RegionReplicaHostCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(REGION_REPLICA_HOST_COST_KEY,
        DEFAULT_REGION_REPLICA_HOST_COST_KEY));
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      // max cost is the case where every region replica is hosted together regardless of host
      maxCost = cluster.numHosts > 1 ? getMaxCost(cluster) : 0;
      costsPerGroup = new long[cluster.numHosts];
      primariesOfRegionsPerGroup = cluster.multiServersPerHost // either server based or host based
          ? cluster.primariesOfRegionsPerHost
          : cluster.primariesOfRegionsPerServer;
      for (int i = 0 ; i < primariesOfRegionsPerGroup.length; i++) {
        costsPerGroup[i] = costPerGroup(primariesOfRegionsPerGroup[i]);
      }
    }

    long getMaxCost(Cluster cluster) {
      if (!cluster.hasRegionReplicas) {
        return 0; // short circuit
      }
      // max cost is the case where every region replica is hosted together regardless of host
      int[] primariesOfRegions = new int[cluster.numRegions];
      System.arraycopy(cluster.regionIndexToPrimaryIndex, 0, primariesOfRegions, 0,
          cluster.regions.length);

      Arrays.sort(primariesOfRegions);

      // compute numReplicas from the sorted array
      return costPerGroup(primariesOfRegions);
    }

    @Override
    boolean isNeeded() {
      return cluster.hasRegionReplicas;
    }

    @Override
    protected double cost() {
      if (maxCost <= 0) {
        return 0;
      }

      long totalCost = 0;
      for (int i = 0 ; i < costsPerGroup.length; i++) {
        totalCost += costsPerGroup[i];
      }
      return scale(0, maxCost, totalCost);
    }

    /**
     * For each primary region, it computes the total number of replicas in the array (numReplicas)
     * and returns a sum of numReplicas-1 squared. For example, if the server hosts
     * regions a, b, c, d, e, f where a and b are same replicas, and c,d,e are same replicas, it
     * returns (2-1) * (2-1) + (3-1) * (3-1) + (1-1) * (1-1).
     * @param primariesOfRegions a sorted array of primary regions ids for the regions hosted
     * @return a sum of numReplicas-1 squared for each primary region in the group.
     */
    protected long costPerGroup(int[] primariesOfRegions) {
      long cost = 0;
      int currentPrimary = -1;
      int currentPrimaryIndex = -1;
      // primariesOfRegions is a sorted array of primary ids of regions. Replicas of regions
      // sharing the same primary will have consecutive numbers in the array.
      for (int j = 0 ; j <= primariesOfRegions.length; j++) {
        int primary = j < primariesOfRegions.length ? primariesOfRegions[j] : -1;
        if (primary != currentPrimary) { // we see a new primary
          int numReplicas = j - currentPrimaryIndex;
          // square the cost
          if (numReplicas > 1) { // means consecutive primaries, indicating co-location
            cost += (numReplicas - 1) * (numReplicas - 1);
          }
          currentPrimary = primary;
          currentPrimaryIndex = j;
        }
      }

      return cost;
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      if (maxCost <= 0) {
        return; // no need to compute
      }
      if (cluster.multiServersPerHost) {
        int oldHost = cluster.serverIndexToHostIndex[oldServer];
        int newHost = cluster.serverIndexToHostIndex[newServer];
        if (newHost != oldHost) {
          costsPerGroup[oldHost] = costPerGroup(cluster.primariesOfRegionsPerHost[oldHost]);
          costsPerGroup[newHost] = costPerGroup(cluster.primariesOfRegionsPerHost[newHost]);
        }
      } else {
        costsPerGroup[oldServer] = costPerGroup(cluster.primariesOfRegionsPerServer[oldServer]);
        costsPerGroup[newServer] = costPerGroup(cluster.primariesOfRegionsPerServer[newServer]);
      }
    }

    @Override
    public final void updateWeight(double[] weights) {
      weights[StochasticLoadBalancer.GeneratorType.RACK.ordinal()] += cost();
    }
  }

  /**
   * A cost function for region replicas for the rack distribution. We give a relatively high
   * cost to hosting replicas of the same region in the same rack. We do not prevent the case
   * though.
   */
  static class RegionReplicaRackCostFunction extends RegionReplicaHostCostFunction {
    private static final String REGION_REPLICA_RACK_COST_KEY =
        "hbase.master.balancer.stochastic.regionReplicaRackCostKey";
    private static final float DEFAULT_REGION_REPLICA_RACK_COST_KEY = 10000;

    public RegionReplicaRackCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(REGION_REPLICA_RACK_COST_KEY,
        DEFAULT_REGION_REPLICA_RACK_COST_KEY));
    }

    @Override
    void init(Cluster cluster) {
      this.cluster = cluster;
      if (cluster.numRacks <= 1) {
        maxCost = 0;
        return; // disabled for 1 rack
      }
      // max cost is the case where every region replica is hosted together regardless of rack
      maxCost = getMaxCost(cluster);
      costsPerGroup = new long[cluster.numRacks];
      for (int i = 0 ; i < cluster.primariesOfRegionsPerRack.length; i++) {
        costsPerGroup[i] = costPerGroup(cluster.primariesOfRegionsPerRack[i]);
      }
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      if (maxCost <= 0) {
        return; // no need to compute
      }
      int oldRack = cluster.serverIndexToRackIndex[oldServer];
      int newRack = cluster.serverIndexToRackIndex[newServer];
      if (newRack != oldRack) {
        costsPerGroup[oldRack] = costPerGroup(cluster.primariesOfRegionsPerRack[oldRack]);
        costsPerGroup[newRack] = costPerGroup(cluster.primariesOfRegionsPerRack[newRack]);
      }
    }
  }

  /**
   * Compute the cost of total memstore size.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  static class MemStoreSizeCostFunction extends CostFromRegionLoadAsRateFunction {

    private static final String MEMSTORE_SIZE_COST_KEY =
        "hbase.master.balancer.stochastic.memstoreSizeCost";
    private static final float DEFAULT_MEMSTORE_SIZE_COST = 5;

    MemStoreSizeCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(MEMSTORE_SIZE_COST_KEY, DEFAULT_MEMSTORE_SIZE_COST));
    }

    @Override
    protected double getCostFromRl(BalancerRegionLoad rl) {
      return rl.getMemStoreSizeMB();
    }
  }
  /**
   * Compute the cost of total open storefiles size.  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */
  static class StoreFileCostFunction extends CostFromRegionLoadFunction {

    private static final String STOREFILE_SIZE_COST_KEY =
        "hbase.master.balancer.stochastic.storefileSizeCost";
    private static final float DEFAULT_STOREFILE_SIZE_COST = 5;

    StoreFileCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(STOREFILE_SIZE_COST_KEY, DEFAULT_STOREFILE_SIZE_COST));
    }

    @Override
    protected double getCostFromRl(BalancerRegionLoad rl) {
      return rl.getStorefileSizeMB();
    }
  }

  /**
   * A helper function to compose the attribute name from tablename and costfunction name
   */
  public static String composeAttributeName(String tableName, String costFunctionName) {
    return tableName + TABLE_FUNCTION_SEP + costFunctionName;
  }

  /**
   * Scale the value between 0 and 1.
   * @param min Min value
   * @param max The Max value
   * @param value The value to be scaled.
   * @return The scaled value.
   */
  static double scale(double min, double max, double value) {
    if (max <= min || value <= min
      || Math.abs(max - min) <= COST_EPSILON || Math.abs(value - min) <= COST_EPSILON) {
      return 0;
    }
    if (max <= min || Math.abs(max - min) <= COST_EPSILON) {
      return 0;
    }

    return Math.max(0d, Math.min(1d, (value - min) / (max - min)));
  }
}
