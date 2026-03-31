/*
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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * The base class for load balancers. It provides the functions used to by {@code AssignmentManager}
 * to assign regions in the edge cases. It doesn't provide an implementation of the actual balancing
 * algorithm.
 * <p/>
 * Since 3.0.0, all the balancers will be wrapped inside a {@code RSGroupBasedLoadBalancer}, it will
 * be in charge of the synchronization of balancing and configuration changing, so we do not need to
 * synchronize by ourselves.
 */
@InterfaceAudience.Private
public abstract class BaseLoadBalancer implements LoadBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(BaseLoadBalancer.class);

  public static final String BALANCER_DECISION_BUFFER_ENABLED =
    "hbase.master.balancer.decision.buffer.enabled";
  public static final boolean DEFAULT_BALANCER_DECISION_BUFFER_ENABLED = false;

  public static final String BALANCER_REJECTION_BUFFER_ENABLED =
    "hbase.master.balancer.rejection.buffer.enabled";
  public static final boolean DEFAULT_BALANCER_REJECTION_BUFFER_ENABLED = false;

  public static final boolean DEFAULT_HBASE_MASTER_LOADBALANCE_BYTABLE = false;

  public static final String REGIONS_SLOP_KEY = "hbase.regions.slop";
  public static final float REGIONS_SLOP_DEFAULT = 0.2f;

  protected static final int MIN_SERVER_BALANCE = 2;
  private volatile boolean stopped = false;

  protected volatile RegionHDFSBlockLocationFinder regionFinder;
  protected boolean useRegionFinder;
  protected boolean isByTable = DEFAULT_HBASE_MASTER_LOADBALANCE_BYTABLE;

  // slop for regions
  protected float slop;
  protected volatile RackManager rackManager;
  protected MetricsBalancer metricsBalancer = null;
  protected ClusterMetrics clusterStatus = null;
  protected ServerName masterServerName;
  protected ClusterInfoProvider provider;

  /**
   * The constructor that uses the basic MetricsBalancer
   */
  protected BaseLoadBalancer() {
    this(null);
  }

  /**
   * This Constructor accepts an instance of MetricsBalancer, which will be used instead of creating
   * a new one
   */
  protected BaseLoadBalancer(MetricsBalancer metricsBalancer) {
    this.metricsBalancer = (metricsBalancer != null) ? metricsBalancer : new MetricsBalancer();
  }

  protected final Configuration getConf() {
    return provider.getConfiguration();
  }

  @Override
  public void updateClusterMetrics(ClusterMetrics st) {
    this.clusterStatus = st;
    if (useRegionFinder) {
      regionFinder.setClusterMetrics(st);
    }
  }

  @Override
  public void setClusterInfoProvider(ClusterInfoProvider provider) {
    this.provider = provider;
  }

  @Override
  public void postMasterStartupInitialize() {
    if (provider != null && regionFinder != null) {
      regionFinder.refreshAndWait(provider.getAssignedRegions());
    }
  }

  protected final boolean idleRegionServerExist(BalancerClusterState c) {
    boolean isServerExistsWithMoreRegions = false;
    boolean isServerExistsWithZeroRegions = false;
    for (int[] serverList : c.regionsPerServer) {
      if (serverList.length > 1) {
        isServerExistsWithMoreRegions = true;
      }
      if (serverList.length == 0) {
        isServerExistsWithZeroRegions = true;
      }
    }
    return isServerExistsWithMoreRegions && isServerExistsWithZeroRegions;
  }

  protected final boolean sloppyRegionServerExist(ClusterLoadState cs) {
    if (slop < 0) {
      LOG.debug("Slop is less than zero, not checking for sloppiness.");
      return false;
    }
    float average = cs.getLoadAverage(); // for logging
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    int maxLoad = cs.getMaxLoad();
    int minLoad = cs.getMinLoad();
    if (!(maxLoad > ceiling || minLoad < floor)) {
      NavigableMap<ServerAndLoad, List<RegionInfo>> serversByLoad = cs.getServersByLoad();
      if (LOG.isTraceEnabled()) {
        // If nothing to balance, then don't say anything unless trace-level logging.
        LOG.trace("Skipping load balancing because balanced cluster; " + "servers="
          + cs.getNumServers() + " regions=" + cs.getNumRegions() + " average=" + average
          + " mostloaded=" + serversByLoad.lastKey().getLoad() + " leastloaded="
          + serversByLoad.firstKey().getLoad());
      }
      return false;
    }
    return true;
  }

  /**
   * Generates a bulk assignment plan to be used on cluster startup using a simple round-robin
   * assignment.
   * <p/>
   * Takes a list of all the regions and all the servers in the cluster and returns a map of each
   * server to the regions that it should be assigned.
   * <p/>
   * Currently implemented as a round-robin assignment. Same invariant as load balancing, all
   * servers holding floor(avg) or ceiling(avg). TODO: Use block locations from HDFS to place
   * regions with their blocks
   * @param regions all regions
   * @param servers all servers
   * @return map of server to the regions it should take, or emptyMap if no assignment is possible
   *         (ie. no servers)
   */
  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
    List<ServerName> servers) throws HBaseIOException {
    metricsBalancer.incrMiscInvocations();
    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to do round robin assignment but no servers to assign to");
      return Collections.singletonMap(BOGUS_SERVER_NAME, new ArrayList<>(regions));
    }

    // TODO: instead of retainAssignment() and roundRobinAssignment(), we should just run the
    // normal LB.balancerCluster() with unassignedRegions. We only need to have a candidate
    // generator for AssignRegionAction. The LB will ensure the regions are mostly local
    // and balanced. This should also run fast with fewer number of iterations.

    if (numServers == 1) { // Only one server, nothing fancy we can do here
      return Collections.singletonMap(servers.get(0), new ArrayList<>(regions));
    }

    BalancerClusterState cluster = createCluster(servers, regions);
    Map<ServerName, List<RegionInfo>> assignments = new HashMap<>();
    roundRobinAssignment(cluster, regions, servers, assignments);
    return Collections.unmodifiableMap(assignments);
  }

  private BalancerClusterState createCluster(List<ServerName> servers,
    Collection<RegionInfo> regions) throws HBaseIOException {
    boolean hasRegionReplica = false;
    try {
      if (provider != null) {
        hasRegionReplica = provider.hasRegionReplica(regions);
      }
    } catch (IOException ioe) {
      throw new HBaseIOException(ioe);
    }

    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments, but this will only contain
    // replicas of the regions that are passed (for performance).
    Map<ServerName, List<RegionInfo>> clusterState = null;
    if (!hasRegionReplica) {
      clusterState = getRegionAssignmentsByServer(regions);
    } else {
      // for the case where we have region replica it is better we get the entire cluster's snapshot
      clusterState = getRegionAssignmentsByServer(null);
    }

    for (ServerName server : servers) {
      if (!clusterState.containsKey(server)) {
        clusterState.put(server, Collections.emptyList());
      }
    }
    return new BalancerClusterState(regions, clusterState, null, this.regionFinder, rackManager,
      null);
  }

  private List<ServerName> findIdleServers(List<ServerName> servers) {
    return provider.getOnlineServersListWithPredicator(servers,
      metrics -> metrics.getRegionMetrics().isEmpty());
  }

  /**
   * Used to assign a single region to a random server.
   */
  @Override
  public ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers)
    throws HBaseIOException {
    metricsBalancer.incrMiscInvocations();
    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to retain assignment but no servers to assign to");
      return null;
    }
    if (numServers == 1) { // Only one server, nothing fancy we can do here
      return servers.get(0);
    }
    List<ServerName> idleServers = findIdleServers(servers);
    if (idleServers.size() == 1) {
      return idleServers.get(0);
    }
    final List<ServerName> finalServers = idleServers.isEmpty() ? servers : idleServers;
    List<RegionInfo> regions = Lists.newArrayList(regionInfo);
    BalancerClusterState cluster = createCluster(finalServers, regions);
    return randomAssignment(cluster, regionInfo, finalServers);
  }

  /**
   * Generates a bulk assignment startup plan, attempting to reuse the existing assignment
   * information from META, but adjusting for the specified list of available/online servers
   * available for assignment.
   * <p>
   * Takes a map of all regions to their existing assignment from META. Also takes a list of online
   * servers for regions to be assigned to. Attempts to retain all assignment, so in some instances
   * initial assignment will not be completely balanced.
   * <p>
   * Any leftover regions without an existing server to be assigned to will be assigned randomly to
   * available servers.
   * @param regions regions and existing assignment from meta
   * @param servers available servers
   * @return map of servers and regions to be assigned to them, or emptyMap if no assignment is
   *         possible (ie. no servers)
   */
  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
    List<ServerName> servers) throws HBaseIOException {
    // Update metrics
    metricsBalancer.incrMiscInvocations();
    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to do retain assignment but no servers to assign to");
      return Collections.singletonMap(BOGUS_SERVER_NAME, new ArrayList<>(regions.keySet()));
    }

    if (numServers == 1) { // Only one server, nothing fancy we can do here
      return Collections.singletonMap(servers.get(0), new ArrayList<>(regions.keySet()));
    }

    // Group all the old assignments by their hostname.
    // We can't group directly by ServerName since the servers all have
    // new start-codes.

    // Group the servers by their hostname. It's possible we have multiple
    // servers on the same host on different ports.
    Map<ServerName, List<RegionInfo>> assignments = new HashMap<>();
    ArrayListMultimap<String, ServerName> serversByHostname = ArrayListMultimap.create();
    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<>());
      serversByHostname.put(server.getHostnameLowerCase(), server);
    }

    // Collection of the hostnames that used to have regions
    // assigned, but for which we no longer have any RS running
    // after the cluster restart.
    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();

    // If the old servers aren't present, lets assign those regions later.
    List<RegionInfo> randomAssignRegions = Lists.newArrayList();

    int numRandomAssignments = 0;
    int numRetainedAssigments = 0;
    for (Map.Entry<RegionInfo, ServerName> entry : regions.entrySet()) {
      RegionInfo region = entry.getKey();
      ServerName oldServerName = entry.getValue();
      List<ServerName> localServers = new ArrayList<>();
      if (oldServerName != null) {
        localServers = serversByHostname.get(oldServerName.getHostnameLowerCase());
      }
      if (localServers.isEmpty()) {
        // No servers on the new cluster match up with this hostname, assign randomly, later.
        randomAssignRegions.add(region);
        if (oldServerName != null) {
          oldHostsNoLongerPresent.add(oldServerName.getHostnameLowerCase());
        }
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        ServerName target = localServers.get(0);
        assignments.get(target).add(region);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        if (localServers.contains(oldServerName)) {
          assignments.get(oldServerName).add(region);
          numRetainedAssigments++;
        } else {
          ServerName target = null;
          for (ServerName tmp : localServers) {
            if (tmp.getPort() == oldServerName.getPort()) {
              target = tmp;
              assignments.get(tmp).add(region);
              numRetainedAssigments++;
              break;
            }
          }
          if (target == null) {
            randomAssignRegions.add(region);
          }
        }
      }
    }

    // If servers from prior assignment aren't present, then lets do randomAssignment on regions.
    if (randomAssignRegions.size() > 0) {
      BalancerClusterState cluster = createCluster(servers, regions.keySet());
      for (Map.Entry<ServerName, List<RegionInfo>> entry : assignments.entrySet()) {
        ServerName sn = entry.getKey();
        for (RegionInfo region : entry.getValue()) {
          cluster.doAssignRegion(region, sn);
        }
      }
      for (RegionInfo region : randomAssignRegions) {
        ServerName target = randomAssignment(cluster, region, servers);
        assignments.get(target).add(region);
        numRandomAssignments++;
      }
    }

    String randomAssignMsg = "";
    if (numRandomAssignments > 0) {
      randomAssignMsg = numRandomAssignments + " regions were assigned "
        + "to random hosts, since the old hosts for these regions are no "
        + "longer present in the cluster. These hosts were:\n  "
        + Joiner.on("\n  ").join(oldHostsNoLongerPresent);
    }

    LOG.info("Reassigned " + regions.size() + " regions. " + numRetainedAssigments
      + " retained the pre-restart assignment. " + randomAssignMsg);
    return Collections.unmodifiableMap(assignments);
  }

  protected float getDefaultSlop() {
    return REGIONS_SLOP_DEFAULT;
  }

  private RegionHDFSBlockLocationFinder createRegionLocationFinder(Configuration conf) {
    RegionHDFSBlockLocationFinder finder = new RegionHDFSBlockLocationFinder();
    finder.setConf(conf);
    finder.setClusterInfoProvider(provider);
    return finder;
  }

  protected void loadConf(Configuration conf) {
    this.slop = conf.getFloat(REGIONS_SLOP_KEY, getDefaultSlop());
    this.rackManager = new RackManager(conf);
    useRegionFinder = conf.getBoolean("hbase.master.balancer.uselocality", true);
    if (useRegionFinder) {
      regionFinder = createRegionLocationFinder(conf);
    } else {
      regionFinder = null;
    }
    this.isByTable = conf.getBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE,
      DEFAULT_HBASE_MASTER_LOADBALANCE_BYTABLE);
    // Print out base configs. Don't print overallSlop since it for simple balancer exclusively.
    LOG.info("slop={}", this.slop);
  }

  @Override
  public void initialize() {
    loadConf(getConf());
  }

  @Override
  public void regionOnline(RegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(RegionInfo regionInfo) {
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public void stop(String why) {
    LOG.info("Load Balancer stop requested: {}", why);
    stopped = true;
  }

  /**
   * Updates the balancer status tag reported to JMX
   */
  @Override
  public void updateBalancerStatus(boolean status) {
    metricsBalancer.balancerStatus(status);
  }

  /**
   * Used to assign a single region to a random server.
   */
  private ServerName randomAssignment(BalancerClusterState cluster, RegionInfo regionInfo,
    List<ServerName> servers) {
    int numServers = servers.size(); // servers is not null, numServers > 1
    ServerName sn = null;
    final int maxIterations = numServers * 4;
    int iterations = 0;
    List<ServerName> usedSNs = new ArrayList<>(servers.size());
    Random rand = ThreadLocalRandom.current();
    do {
      int i = rand.nextInt(numServers);
      sn = servers.get(i);
      if (!usedSNs.contains(sn)) {
        usedSNs.add(sn);
      }
    } while (cluster.wouldLowerAvailability(regionInfo, sn) && iterations++ < maxIterations);
    if (iterations >= maxIterations) {
      // We have reached the max. Means the servers that we collected is still lowering the
      // availability
      for (ServerName unusedServer : servers) {
        if (!usedSNs.contains(unusedServer)) {
          // check if any other unused server is there for us to use.
          // If so use it. Else we have not other go but to go with one of them
          if (!cluster.wouldLowerAvailability(regionInfo, unusedServer)) {
            sn = unusedServer;
            break;
          }
        }
      }
    }
    cluster.doAssignRegion(regionInfo, sn);
    return sn;
  }

  /**
   * Round-robin a list of regions to a list of servers
   */
  private void roundRobinAssignment(BalancerClusterState cluster, List<RegionInfo> regions,
    List<ServerName> servers, Map<ServerName, List<RegionInfo>> assignments) {
    Random rand = ThreadLocalRandom.current();
    List<RegionInfo> unassignedRegions = new ArrayList<>();
    int numServers = servers.size();
    int numRegions = regions.size();
    int max = (int) Math.ceil((float) numRegions / numServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = rand.nextInt(numServers);
    }
    int regionIdx = 0;
    for (int j = 0; j < numServers; j++) {
      ServerName server = servers.get((j + serverIdx) % numServers);
      List<RegionInfo> serverRegions = new ArrayList<>(max);
      for (int i = regionIdx; i < numRegions; i += numServers) {
        RegionInfo region = regions.get(i % numRegions);
        if (cluster.wouldLowerAvailability(region, server)) {
          unassignedRegions.add(region);
        } else {
          serverRegions.add(region);
          cluster.doAssignRegion(region, server);
        }
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }

    List<RegionInfo> lastFewRegions = new ArrayList<>();
    // assign the remaining by going through the list and try to assign to servers one-by-one
    serverIdx = rand.nextInt(numServers);
    for (RegionInfo region : unassignedRegions) {
      boolean assigned = false;
      for (int j = 0; j < numServers; j++) { // try all servers one by one
        ServerName server = servers.get((j + serverIdx) % numServers);
        if (cluster.wouldLowerAvailability(region, server)) {
          continue;
        } else {
          assignments.computeIfAbsent(server, k -> new ArrayList<>()).add(region);
          cluster.doAssignRegion(region, server);
          serverIdx = (j + serverIdx + 1) % numServers; // remain from next server
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        lastFewRegions.add(region);
      }
    }
    // just sprinkle the rest of the regions on random regionservers. The balanceCluster will
    // make it optimal later. we can end up with this if numReplicas > numServers.
    for (RegionInfo region : lastFewRegions) {
      int i = rand.nextInt(numServers);
      ServerName server = servers.get(i);
      assignments.computeIfAbsent(server, k -> new ArrayList<>()).add(region);
      cluster.doAssignRegion(region, server);
    }
  }

  // return a modifiable map, as we may add more entries into the returned map.
  private Map<ServerName, List<RegionInfo>>
    getRegionAssignmentsByServer(Collection<RegionInfo> regions) {
    return provider != null
      ? new HashMap<>(provider.getSnapShotOfAssignment(regions))
      : new HashMap<>();
  }

  protected final Map<ServerName, List<RegionInfo>>
    toEnsumbleTableLoad(Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable) {
    Map<ServerName, List<RegionInfo>> returnMap = new TreeMap<>();
    for (Map<ServerName, List<RegionInfo>> serverNameListMap : LoadOfAllTable.values()) {
      serverNameListMap.forEach((serverName, regionInfoList) -> {
        List<RegionInfo> regionInfos =
          returnMap.computeIfAbsent(serverName, k -> new ArrayList<>());
        regionInfos.addAll(regionInfoList);
      });
    }
    return returnMap;
  }

  /**
   * Perform the major balance operation for table, all sub classes should override this method.
   * <p/>
   * Will be invoked by {@link #balanceCluster(Map)}. If
   * {@link HConstants#HBASE_MASTER_LOADBALANCE_BYTABLE} is enabled, we will call this method
   * multiple times, one table a time, where we will only pass in the regions for a single table
   * each time. If not, we will pass in all the regions at once, and the {@code tableName} will be
   * {@link HConstants#ENSEMBLE_TABLE_NAME}.
   * @param tableName      the table to be balanced
   * @param loadOfOneTable region load of servers for the specific one table
   * @return List of plans
   */
  protected abstract List<RegionPlan> balanceTable(TableName tableName,
    Map<ServerName, List<RegionInfo>> loadOfOneTable);

  /**
   * Called before actually executing balanceCluster. The sub classes could override this method to
   * do some initialization work.
   */
  protected void
    preBalanceCluster(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
  }

  /**
   * Perform the major balance operation for cluster, will invoke
   * {@link #balanceTable(TableName, Map)} to do actual balance.
   * <p/>
   * THIs method is marked as final which means you should not override this method. See the javadoc
   * for {@link #balanceTable(TableName, Map)} for more details.
   * @param loadOfAllTable region load of servers for all table
   * @return a list of regions to be moved, including source and destination, or null if cluster is
   *         already balanced
   * @see #balanceTable(TableName, Map)
   */
  @Override
  public final List<RegionPlan>
    balanceCluster(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
    preBalanceCluster(loadOfAllTable);
    if (isByTable) {
      List<RegionPlan> result = new ArrayList<>();
      loadOfAllTable.forEach((tableName, loadOfOneTable) -> {
        LOG.info("Start Generate Balance plan for table: " + tableName);
        List<RegionPlan> partialPlans = balanceTable(tableName, loadOfOneTable);
        if (partialPlans != null) {
          result.addAll(partialPlans);
        }
      });
      return result;
    } else {
      LOG.debug("Start Generate Balance plan for cluster.");
      return balanceTable(HConstants.ENSEMBLE_TABLE_NAME, toEnsumbleTableLoad(loadOfAllTable));
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    loadConf(conf);
  }
}
