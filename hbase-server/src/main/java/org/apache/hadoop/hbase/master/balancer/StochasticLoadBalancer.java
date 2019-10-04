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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action.Type;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.AssignRegionAction;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.LocalityType;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.MoveRegionAction;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.SwapRegionsAction;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Optional;
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

  Map<String, Deque<BalancerRegionLoad>> loads = new HashMap<>();

  // values are defaults
  private int maxSteps = 1000000;
  private boolean runMaxSteps = false;
  private int stepsPerRegion = 800;
  private long maxRunningTime = 30 * 1000 * 1; // 30 seconds.
  private int numRegionLoadsToRemember = 15;
  private float minCostNeedBalance = 0.05f;

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
  private boolean isByTable = false;
  private TableName tableName = null;

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
    isByTable = conf.getBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, isByTable);
    minCostNeedBalance = conf.getFloat(MIN_COST_NEED_BALANCE_KEY, minCostNeedBalance);
    if (localityCandidateGenerator == null) {
      localityCandidateGenerator = new LocalityBasedCandidateGenerator(services);
    }
    localityCost = new ServerLocalityCostFunction(conf, services);
    rackLocalityCost = new RackLocalityCostFunction(conf, services);

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
    costFunctions.add(new RegionCountSkewCostFunction(conf));
    costFunctions.add(new PrimaryRegionCountSkewCostFunction(conf));
    costFunctions.add(new MoveCostFunction(conf));
    costFunctions.add(localityCost);
    costFunctions.add(rackLocalityCost);
    costFunctions.add(new TableSkewCostFunction(conf));
    costFunctions.add(regionReplicaHostCostFunction);
    costFunctions.add(regionReplicaRackCostFunction);
    costFunctions.add(regionLoadFunctions[0]);
    costFunctions.add(regionLoadFunctions[1]);
    costFunctions.add(regionLoadFunctions[2]);
    costFunctions.add(regionLoadFunctions[3]);
    costFunctions.add(regionLoadFunctions[4]);
    loadCustomCostFunctions(conf);

    curFunctionCosts= new Double[costFunctions.size()];
    tempFunctionCosts= new Double[costFunctions.size()];
    LOG.info("Loaded config; maxSteps=" + maxSteps + ", stepsPerRegion=" + stepsPerRegion +
            ", maxRunningTime=" + maxRunningTime + ", isByTable=" + isByTable + ", CostFunctions=" +
            Arrays.toString(getCostFunctionNames()) + " etc.");
  }

  private void loadCustomCostFunctions(Configuration conf) {
    String[] functionsNames = conf.getStrings(COST_FUNCTIONS_COST_FUNCTIONS_KEY);

    if (null == functionsNames) {
      return;
    }

    costFunctions.addAll(Arrays.stream(functionsNames)
            .map(c -> {
              Class<? extends CostFunction> klass = null;
              try {
                klass = (Class<? extends CostFunction>) Class.forName(c);
              } catch (ClassNotFoundException e) {
                LOG.warn("Cannot load class " + c + "': " + e.getMessage());
              }
              if (null == klass) {
                return null;
              }

              CostFunction reflected = ReflectionUtils.newInstance(klass, conf);
              LOG.info("Successfully loaded custom CostFunction '" +
                      reflected.getClass().getSimpleName() + "'");

              return reflected;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
  }

  protected void setCandidateGenerators(List<CandidateGenerator> customCandidateGenerators) {
    this.candidateGenerators = customCandidateGenerators;
  }

  @Override
  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", 0.001F);
  }

  @Override
  public synchronized void setClusterMetrics(ClusterMetrics st) {
    super.setClusterMetrics(st);
    updateRegionLoad();
    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setClusterMetrics(st);
    }

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
  public synchronized void setMasterServices(MasterServices masterServices) {
    super.setMasterServices(masterServices);
    this.localityCost.setServices(masterServices);
    this.rackLocalityCost.setServices(masterServices);
    this.localityCandidateGenerator.setServices(masterServices);
  }

  @Override
  protected synchronized boolean areSomeRegionReplicasColocated(Cluster c) {
    regionReplicaHostCostFunction.init(c);
    if (regionReplicaHostCostFunction.cost() > 0) return true;
    regionReplicaRackCostFunction.init(c);
    if (regionReplicaRackCostFunction.cost() > 0) return true;
    return false;
  }

  @Override
  protected boolean needsBalance(Cluster cluster) {
    ClusterLoadState cs = new ClusterLoadState(cluster.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
    if (areSomeRegionReplicasColocated(cluster)) {
      return true;
    }

    double total = 0.0;
    float sumMultiplier = 0.0f;
    for (CostFunction c : costFunctions) {
      float multiplier = c.getMultiplier();
      if (multiplier <= 0) {
        continue;
      }
      if (!c.isNeeded()) {
        LOG.debug("{} not needed", c.getClass().getSimpleName());
        continue;
      }
      sumMultiplier += multiplier;
      total += c.cost() * multiplier;
    }

    if (total <= 0 || sumMultiplier <= 0
        || (sumMultiplier > 0 && (total / sumMultiplier) < minCostNeedBalance)) {
      if (LOG.isTraceEnabled()) {
        final String loadBalanceTarget =
            isByTable ? String.format("table (%s)", tableName) : "cluster";
        LOG.trace("Skipping load balancing because the {} is balanced. Total cost: {}, "
            + "Sum multiplier: {}, Minimum cost needed for balance: {}", loadBalanceTarget, total,
            sumMultiplier, minCostNeedBalance);
      }
      return false;
    }
    return true;
  }

  @Override
  public synchronized List<RegionPlan> balanceCluster(TableName tableName, Map<ServerName,
    List<RegionInfo>> clusterState) {
    this.tableName = tableName;
    return balanceCluster(clusterState);
  }

  @VisibleForTesting
  Cluster.Action nextAction(Cluster cluster) {
    return candidateGenerators.get(RANDOM.nextInt(candidateGenerators.size()))
            .generate(cluster);
  }

  /**
   * Given the cluster state this will try and approach an optimal balance. This
   * should always approach the optimal state given enough steps.
   */
  @Override
  public synchronized List<RegionPlan> balanceCluster(Map<ServerName,
    List<RegionInfo>> clusterState) {
    List<RegionPlan> plans = balanceMasterRegions(clusterState);
    if (plans != null || clusterState == null || clusterState.size() <= 1) {
      return plans;
    }

    if (masterServerName != null && clusterState.containsKey(masterServerName)) {
      if (clusterState.size() <= 2) {
        return null;
      }
      clusterState = new HashMap<>(clusterState);
      clusterState.remove(masterServerName);
    }

    // On clusters with lots of HFileLinks or lots of reference files,
    // instantiating the storefile infos can be quite expensive.
    // Allow turning this feature off if the locality cost is not going to
    // be used in any computations.
    RegionLocationFinder finder = null;
    if ((this.localityCost != null && this.localityCost.getMultiplier() > 0)
        || (this.rackLocalityCost != null && this.rackLocalityCost.getMultiplier() > 0)) {
      finder = this.regionFinder;
    }

    //The clusterState that is given to this method contains the state
    //of all the regions in the table(s) (that's true today)
    // Keep track of servers to iterate through them.
    Cluster cluster = new Cluster(clusterState, loads, finder, rackManager);

    long startTime = EnvironmentEdgeManager.currentTime();

    initCosts(cluster);

    if (!needsBalance(cluster)) {
      return null;
    }

    double currentCost = computeCost(cluster, Double.MAX_VALUE);
    curOverallCost = currentCost;
    for (int i = 0; i < this.curFunctionCosts.length; i++) {
      curFunctionCosts[i] = tempFunctionCosts[i];
    }
    double initCost = currentCost;
    double newCost = currentCost;

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

    // Perform a stochastic walk to see if we can get a good fit.
    long step;

    for (step = 0; step < computedMaxSteps; step++) {
      Cluster.Action action = nextAction(cluster);

      if (action.type == Type.NULL) {
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
        for (int i = 0; i < this.curFunctionCosts.length; i++) {
          curFunctionCosts[i] = tempFunctionCosts[i];
        }
      } else {
        // Put things back the way they were before.
        // TODO: undo by remembering old values
        Action undoAction = action.undoAction();
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
      plans = createRegionPlans(cluster);
      LOG.info("Finished computing new load balance plan. Computation took {}" +
        " to try {} different iterations.  Found a solution that moves " +
        "{} regions; Going from a computed cost of {}" +
        " to a new cost of {}", java.time.Duration.ofMillis(endTime - startTime),
        step, plans.size(), initCost, currentCost);
      return plans;
    }
    LOG.info("Could not find a better load balance plan.  Tried {} different configurations in " +
      "{}, and did not find anything with a computed cost less than {}", step,
      java.time.Duration.ofMillis(endTime - startTime), initCost);
    return null;
  }

  /**
   * update costs to JMX
   */
  private void updateStochasticCosts(TableName tableName, Double overall, Double[] subCosts) {
    if (tableName == null) return;

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

    for(CostFromRegionLoadFunction cost : regionLoadFunctions) {
      cost.setLoads(loads);
    }
  }

  protected void initCosts(Cluster cluster) {
    for (CostFunction c:costFunctions) {
      c.init(cluster);
    }
  }

  protected void updateCostsWithAction(Cluster cluster, Action action) {
    for (CostFunction c : costFunctions) {
      c.postAction(action);
    }
  }

  /**
   * Get the names of the cost functions
   */
  public String[] getCostFunctionNames() {
    if (costFunctions == null) return null;
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
  protected double computeCost(Cluster cluster, double previousCost) {
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

  /** Generates a candidate action to be applied to the cluster for cost function search */
  abstract static class CandidateGenerator {
    abstract Cluster.Action generate(Cluster cluster);

    /**
     * From a list of regions pick a random one. Null can be returned which
     * {@link StochasticLoadBalancer#balanceCluster(Map)} recognize as signal to try a region move
     * rather than swap.
     *
     * @param cluster        The state of the cluster
     * @param server         index of the server
     * @param chanceOfNoSwap Chance that this will decide to try a move rather
     *                       than a swap.
     * @return a random {@link RegionInfo} or null if an asymmetrical move is
     *         suggested.
     */
    protected int pickRandomRegion(Cluster cluster, int server, double chanceOfNoSwap) {
      // Check to see if this is just a move.
      if (cluster.regionsPerServer[server].length == 0 || RANDOM.nextFloat() < chanceOfNoSwap) {
        // signal a move only.
        return -1;
      }
      int rand = RANDOM.nextInt(cluster.regionsPerServer[server].length);
      return cluster.regionsPerServer[server][rand];

    }
    protected int pickRandomServer(Cluster cluster) {
      if (cluster.numServers < 1) {
        return -1;
      }

      return RANDOM.nextInt(cluster.numServers);
    }

    protected int pickRandomRack(Cluster cluster) {
      if (cluster.numRacks < 1) {
        return -1;
      }

      return RANDOM.nextInt(cluster.numRacks);
    }

    protected int pickOtherRandomServer(Cluster cluster, int serverIndex) {
      if (cluster.numServers < 2) {
        return -1;
      }
      while (true) {
        int otherServerIndex = pickRandomServer(cluster);
        if (otherServerIndex != serverIndex) {
          return otherServerIndex;
        }
      }
    }

    protected int pickOtherRandomRack(Cluster cluster, int rackIndex) {
      if (cluster.numRacks < 2) {
        return -1;
      }
      while (true) {
        int otherRackIndex = pickRandomRack(cluster);
        if (otherRackIndex != rackIndex) {
          return otherRackIndex;
        }
      }
    }

    protected Cluster.Action pickRandomRegions(Cluster cluster,
                                                       int thisServer,
                                                       int otherServer) {
      if (thisServer < 0 || otherServer < 0) {
        return Cluster.NullAction;
      }

      // Decide who is most likely to need another region
      int thisRegionCount = cluster.getNumRegions(thisServer);
      int otherRegionCount = cluster.getNumRegions(otherServer);

      // Assign the chance based upon the above
      double thisChance = (thisRegionCount > otherRegionCount) ? 0 : 0.5;
      double otherChance = (thisRegionCount <= otherRegionCount) ? 0 : 0.5;

      int thisRegion = pickRandomRegion(cluster, thisServer, thisChance);
      int otherRegion = pickRandomRegion(cluster, otherServer, otherChance);

      return getAction(thisServer, thisRegion, otherServer, otherRegion);
    }

    protected Cluster.Action getAction(int fromServer, int fromRegion,
        int toServer, int toRegion) {
      if (fromServer < 0 || toServer < 0) {
        return Cluster.NullAction;
      }
      if (fromRegion > 0 && toRegion > 0) {
        return new Cluster.SwapRegionsAction(fromServer, fromRegion,
          toServer, toRegion);
      } else if (fromRegion > 0) {
        return new Cluster.MoveRegionAction(fromRegion, fromServer, toServer);
      } else if (toRegion > 0) {
        return new Cluster.MoveRegionAction(toRegion, toServer, fromServer);
      } else {
        return Cluster.NullAction;
      }
    }

    /**
     * Returns a random iteration order of indexes of an array with size length
     */
    protected List<Integer> getRandomIterationOrder(int length) {
      ArrayList<Integer> order = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        order.add(i);
      }
      Collections.shuffle(order);
      return order;
    }
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

  static class LoadCandidateGenerator extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {
      cluster.sortServersByRegionCount();
      int thisServer = pickMostLoadedServer(cluster, -1);
      int otherServer = pickLeastLoadedServer(cluster, thisServer);

      return pickRandomRegions(cluster, thisServer, otherServer);
    }

    private int pickLeastLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;

      int index = 0;
      while (servers[index] == null || servers[index] == thisServer) {
        index++;
        if (index == servers.length) {
          return -1;
        }
      }
      return servers[index];
    }

    private int pickMostLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;

      int index = servers.length - 1;
      while (servers[index] == null || servers[index] == thisServer) {
        index--;
        if (index < 0) {
          return -1;
        }
      }
      return servers[index];
    }
  }

  static class LocalityBasedCandidateGenerator extends CandidateGenerator {

    private MasterServices masterServices;

    LocalityBasedCandidateGenerator(MasterServices masterServices) {
      this.masterServices = masterServices;
    }

    @Override
    Cluster.Action generate(Cluster cluster) {
      if (this.masterServices == null) {
        int thisServer = pickRandomServer(cluster);
        // Pick the other server
        int otherServer = pickOtherRandomServer(cluster, thisServer);
        return pickRandomRegions(cluster, thisServer, otherServer);
      }

      // Randomly iterate through regions until you find one that is not on ideal host
      for (int region : getRandomIterationOrder(cluster.numRegions)) {
        int currentServer = cluster.regionIndexToServerIndex[region];
        if (currentServer != cluster.getOrComputeRegionsToMostLocalEntities(LocalityType.SERVER)[region]) {
          Optional<Action> potential = tryMoveOrSwap(
              cluster,
              currentServer,
              region,
              cluster.getOrComputeRegionsToMostLocalEntities(LocalityType.SERVER)[region]
          );
          if (potential.isPresent()) {
            return potential.get();
          }
        }
      }
      return Cluster.NullAction;
    }

    /**
     * Try to generate a move/swap fromRegion between fromServer and toServer such that locality is improved.
     * Returns empty optional if no move can be found
     */
    private Optional<Action> tryMoveOrSwap(Cluster cluster,
                                           int fromServer,
                                           int fromRegion,
                                           int toServer) {
      // Try move first. We know apriori fromRegion has the highest locality on toServer
      if (cluster.serverHasTooFewRegions(toServer)) {
        return Optional.of(getAction(fromServer, fromRegion, toServer, -1));
      }

      // Compare locality gain/loss from swapping fromRegion with regions on toServer
      double fromRegionLocalityDelta =
          getWeightedLocality(cluster, fromRegion, toServer) - getWeightedLocality(cluster, fromRegion, fromServer);
      for (int toRegionIndex : getRandomIterationOrder(cluster.regionsPerServer[toServer].length)) {
        int toRegion = cluster.regionsPerServer[toServer][toRegionIndex];
        double toRegionLocalityDelta =
            getWeightedLocality(cluster, toRegion, fromServer) - getWeightedLocality(cluster, toRegion, toServer);
        // If locality would remain neutral or improve, attempt the swap
        if (fromRegionLocalityDelta + toRegionLocalityDelta >= 0) {
          return Optional.of(getAction(fromServer, fromRegion, toServer, toRegion));
        }
      }

      return Optional.absent();
    }

    private double getWeightedLocality(Cluster cluster, int region, int server) {
      return cluster.getOrComputeWeightedLocality(region, server, LocalityType.SERVER);
    }

    void setServices(MasterServices services) {
      this.masterServices = services;
    }
  }

  /**
   * Generates candidates which moves the replicas out of the region server for
   * co-hosted region replicas
   */
  static class RegionReplicaCandidateGenerator extends CandidateGenerator {

    RandomCandidateGenerator randomGenerator = new RandomCandidateGenerator();

    /**
     * Randomly select one regionIndex out of all region replicas co-hosted in the same group
     * (a group is a server, host or rack)
     * @param primariesOfRegionsPerGroup either Cluster.primariesOfRegionsPerServer,
     * primariesOfRegionsPerHost or primariesOfRegionsPerRack
     * @param regionsPerGroup either Cluster.regionsPerServer, regionsPerHost or regionsPerRack
     * @param regionIndexToPrimaryIndex Cluster.regionsIndexToPrimaryIndex
     * @return a regionIndex for the selected primary or -1 if there is no co-locating
     */
    int selectCoHostedRegionPerGroup(int[] primariesOfRegionsPerGroup, int[] regionsPerGroup
        , int[] regionIndexToPrimaryIndex) {
      int currentPrimary = -1;
      int currentPrimaryIndex = -1;
      int selectedPrimaryIndex = -1;
      double currentLargestRandom = -1;
      // primariesOfRegionsPerGroup is a sorted array. Since it contains the primary region
      // ids for the regions hosted in server, a consecutive repetition means that replicas
      // are co-hosted
      for (int j = 0; j <= primariesOfRegionsPerGroup.length; j++) {
        int primary = j < primariesOfRegionsPerGroup.length
            ? primariesOfRegionsPerGroup[j] : -1;
        if (primary != currentPrimary) { // check for whether we see a new primary
          int numReplicas = j - currentPrimaryIndex;
          if (numReplicas > 1) { // means consecutive primaries, indicating co-location
            // decide to select this primary region id or not
            double currentRandom = RANDOM.nextDouble();
            // we don't know how many region replicas are co-hosted, we will randomly select one
            // using reservoir sampling (http://gregable.com/2007/10/reservoir-sampling.html)
            if (currentRandom > currentLargestRandom) {
              selectedPrimaryIndex = currentPrimary;
              currentLargestRandom = currentRandom;
            }
          }
          currentPrimary = primary;
          currentPrimaryIndex = j;
        }
      }

      // we have found the primary id for the region to move. Now find the actual regionIndex
      // with the given primary, prefer to move the secondary region.
      for (int j = 0; j < regionsPerGroup.length; j++) {
        int regionIndex = regionsPerGroup[j];
        if (selectedPrimaryIndex == regionIndexToPrimaryIndex[regionIndex]) {
          // always move the secondary, not the primary
          if (selectedPrimaryIndex != regionIndex) {
            return regionIndex;
          }
        }
      }
      return -1;
    }

    @Override
    Cluster.Action generate(Cluster cluster) {
      int serverIndex = pickRandomServer(cluster);
      if (cluster.numServers <= 1 || serverIndex == -1) {
        return Cluster.NullAction;
      }

      int regionIndex = selectCoHostedRegionPerGroup(
        cluster.primariesOfRegionsPerServer[serverIndex],
        cluster.regionsPerServer[serverIndex],
        cluster.regionIndexToPrimaryIndex);

      // if there are no pairs of region replicas co-hosted, default to random generator
      if (regionIndex == -1) {
        // default to randompicker
        return randomGenerator.generate(cluster);
      }

      int toServerIndex = pickOtherRandomServer(cluster, serverIndex);
      int toRegionIndex = pickRandomRegion(cluster, toServerIndex, 0.9f);
      return getAction(serverIndex, regionIndex, toServerIndex, toRegionIndex);
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

    @SuppressWarnings("checkstyle:linelength")
    /**
     * Function to compute a scaled cost using
     * {@link org.apache.commons.math3.stat.descriptive.DescriptiveStatistics#DescriptiveStatistics()}.
     * It assumes that this is a zero sum set of costs.  It assumes that the worst case
     * possible is all of the elements in one region server and the rest having 0.
     *
     * @param stats the costs
     * @return a scaled set of costs.
     */
    protected double costFromArray(double[] stats) {
      double totalCost = 0;
      double total = getSum(stats);

      double count = stats.length;
      double mean = total/count;

      // Compute max as if all region servers had 0 and one had the sum of all costs.  This must be
      // a zero sum cost for this to make sense.
      double max = ((count - 1) * mean) + (total - mean);

      // It's possible that there aren't enough regions to go around
      double min;
      if (count > total) {
        min = ((count - total) * mean) + ((1 - mean) * total);
      } else {
        // Some will have 1 more than everything else.
        int numHigh = (int) (total - (Math.floor(mean) * count));
        int numLow = (int) (count - numHigh);

        min = (numHigh * (Math.ceil(mean) - mean)) + (numLow * (mean - Math.floor(mean)));

      }
      min = Math.max(0, min);
      for (int i=0; i<stats.length; i++) {
        double n = stats[i];
        double diff = Math.abs(mean - n);
        totalCost += diff;
      }

      double scaled =  scale(min, max, totalCost);
      return scaled;
    }

    private double getSum(double[] stats) {
      double total = 0;
      for(double s:stats) {
        total += s;
      }
      return total;
    }

    /**
     * Scale the value between 0 and 1.
     *
     * @param min   Min value
     * @param max   The Max value
     * @param value The value to be scaled.
     * @return The scaled value.
     */
    protected double scale(double min, double max, double value) {
      if (max <= min || value <= min) {
        return 0;
      }
      if ((max - min) == 0) return 0;

      return Math.max(0d, Math.min(1d, (value - min) / (max - min)));
    }
  }

  /**
   * Given the starting state of the regions and a potential ending state
   * compute cost based upon the number of regions that have moved.
   */
  static class MoveCostFunction extends CostFunction {
    private static final String MOVE_COST_KEY = "hbase.master.balancer.stochastic.moveCost";
    private static final String MAX_MOVES_PERCENT_KEY =
        "hbase.master.balancer.stochastic.maxMovePercent";
    private static final float DEFAULT_MOVE_COST = 7;
    private static final int DEFAULT_MAX_MOVES = 600;
    private static final float DEFAULT_MAX_MOVE_PERCENT = 0.25f;

    private final float maxMovesPercent;

    MoveCostFunction(Configuration conf) {
      super(conf);

      // Move cost multiplier should be the same cost or higher than the rest of the costs to ensure
      // that large benefits are need to overcome the cost of a move.
      this.setMultiplier(conf.getFloat(MOVE_COST_KEY, DEFAULT_MOVE_COST));
      // What percent of the number of regions a single run of the balancer can move.
      maxMovesPercent = conf.getFloat(MAX_MOVES_PERCENT_KEY, DEFAULT_MAX_MOVE_PERCENT);
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
    private static final String REGION_COUNT_SKEW_COST_KEY =
        "hbase.master.balancer.stochastic.regionCountCost";
    private static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;

    private double[] stats = null;

    RegionCountSkewCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as it is the most general way to balance data.
      this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    }

    @Override
    protected double cost() {
      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i =0; i < cluster.numServers; i++) {
        stats[i] = cluster.regionsPerServer[i].length;
      }

      return costFromArray(stats);
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

    private double[] stats = null;

    PrimaryRegionCountSkewCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as primary regions serve majority of reads/writes.
      this.setMultiplier(conf.getFloat(PRIMARY_REGION_COUNT_SKEW_COST_KEY,
        DEFAULT_PRIMARY_REGION_COUNT_SKEW_COST));
    }

    @Override
    protected double cost() {
      if (!cluster.hasRegionReplicas) {
        return 0;
      }
      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i = 0; i < cluster.numServers; i++) {
        stats[i] = 0;
        for (int regionIdx : cluster.regionsPerServer[i]) {
          if (regionIdx == cluster.regionIndexToPrimaryIndex[regionIdx]) {
            stats[i]++;
          }
        }
      }

      return costFromArray(stats);
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

    TableSkewCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(TABLE_SKEW_COST_KEY, DEFAULT_TABLE_SKEW_COST));
    }

    @Override
    protected double cost() {
      double max = cluster.numRegions;
      double min = ((double) cluster.numRegions) / cluster.numServers;
      double value = 0;

      for (int i = 0; i < cluster.numMaxRegionsPerTable.length; i++) {
        value += cluster.numMaxRegionsPerTable[i];
      }

      return scale(min, max, value);
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

    private MasterServices services;

    LocalityBasedCostFunction(Configuration conf,
                              MasterServices srv,
                              LocalityType type,
                              String localityCostKey,
                              float defaultLocalityCost) {
      super(conf);
      this.type = type;
      this.setMultiplier(conf.getFloat(localityCostKey, defaultLocalityCost));
      this.services = srv;
      this.locality = 0.0;
      this.bestLocality = 0.0;
    }

    /**
     * Maps region to the current entity (server or rack) on which it is stored
     */
    abstract int regionIndexToEntityIndex(int region);

    public void setServices(MasterServices srvc) {
      this.services = srvc;
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      locality = 0.0;
      bestLocality = 0.0;

      // If no master, no computation will work, so assume 0 cost
      if (this.services == null) {
        return;
      }

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
      if (this.services == null) {
        return;
      }
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

  }

  static class ServerLocalityCostFunction extends LocalityBasedCostFunction {

    private static final String LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.localityCost";
    private static final float DEFAULT_LOCALITY_COST = 25;

    ServerLocalityCostFunction(Configuration conf, MasterServices srv) {
      super(
          conf,
          srv,
          LocalityType.SERVER,
          LOCALITY_COST_KEY,
          DEFAULT_LOCALITY_COST
      );
    }

    @Override
    int regionIndexToEntityIndex(int region) {
      return cluster.regionIndexToServerIndex[region];
    }
  }

  static class RackLocalityCostFunction extends LocalityBasedCostFunction {

    private static final String RACK_LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.rackLocalityCost";
    private static final float DEFAULT_RACK_LOCALITY_COST = 15;

    public RackLocalityCostFunction(Configuration conf, MasterServices services) {
      super(
          conf,
          services,
          LocalityType.RACK,
          RACK_LOCALITY_COST_KEY,
          DEFAULT_RACK_LOCALITY_COST
      );
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

    private ClusterMetrics clusterStatus = null;
    private Map<String, Deque<BalancerRegionLoad>> loads = null;
    private double[] stats = null;
    CostFromRegionLoadFunction(Configuration conf) {
      super(conf);
    }

    void setClusterMetrics(ClusterMetrics status) {
      this.clusterStatus = status;
    }

    void setLoads(Map<String, Deque<BalancerRegionLoad>> l) {
      this.loads = l;
    }

    @Override
    protected double cost() {
      if (clusterStatus == null || loads == null) {
        return 0;
      }

      if (stats == null || stats.length != cluster.numServers) {
        stats = new double[cluster.numServers];
      }

      for (int i =0; i < stats.length; i++) {
        //Cost this server has from RegionLoad
        long cost = 0;

        // for every region on this server get the rl
        for(int regionIndex:cluster.regionsPerServer[i]) {
          Collection<BalancerRegionLoad> regionLoadList =  cluster.regionLoads[regionIndex];

          // Now if we found a region load get the type of cost that was requested.
          if (regionLoadList != null) {
            cost = (long) (cost + getRegionLoadCost(regionLoadList));
          }
        }

        // Add the total cost to the stats.
        stats[i] = cost;
      }

      // Now return the scaled cost from data held in the stats object.
      return costFromArray(stats);
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
   * Class to be used for the subset of RegionLoad costs that should be treated as rates.
   * We do not compare about the actual rate in requests per second but rather the rate relative
   * to the rest of the regions.
   */
  abstract static class CostFromRegionLoadAsRateFunction extends CostFromRegionLoadFunction {

    CostFromRegionLoadAsRateFunction(Configuration conf) {
      super(conf);
    }

    @Override
    protected double getRegionLoadCost(Collection<BalancerRegionLoad> regionLoadList) {
      double cost = 0;
      double previous = 0;
      boolean isFirst = true;
      for (BalancerRegionLoad rl : regionLoadList) {
        double current = getCostFromRl(rl);
        if (isFirst) {
          isFirst = false;
        } else {
          cost += current - previous;
        }
        previous = current;
      }
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
   * Compute the cost of total number of coprocessor requests  The more unbalanced the higher the
   * computed cost will be.  This uses a rolling average of regionload.
   */

  static class CPRequestCostFunction extends CostFromRegionLoadAsRateFunction {

    private static final String CP_REQUEST_COST_KEY =
        "hbase.master.balancer.stochastic.cpRequestCost";
    private static final float DEFAULT_CP_REQUEST_COST = 5;

    CPRequestCostFunction(Configuration conf) {
      super(conf);
      this.setMultiplier(conf.getFloat(CP_REQUEST_COST_KEY, DEFAULT_CP_REQUEST_COST));
    }

    @Override
    protected double getCostFromRl(BalancerRegionLoad rl) {
      return rl.getCpRequestsCount();
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
}
