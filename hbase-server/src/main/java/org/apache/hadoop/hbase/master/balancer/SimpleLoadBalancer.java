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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionPlan;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(Map)}.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This classes produces plans for the
 * {@link org.apache.hadoop.hbase.master.AssignmentManager} to execute.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SimpleLoadBalancer extends BaseLoadBalancer {
  private static final Log LOG = LogFactory.getLog(SimpleLoadBalancer.class);
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private RegionInfoComparator riComparator = new RegionInfoComparator();
  private RegionPlan.RegionPlanComparator rpComparator = new RegionPlan.RegionPlanComparator();
  private float avgLoadOverall;
  private List<ServerAndLoad> serverLoadList;

  /**
   * Stores additional per-server information about the regions added/removed
   * during the run of the balancing algorithm.
   *
   * For servers that shed regions, we need to track which regions we have already
   * shed. <b>nextRegionForUnload</b> contains the index in the list of regions on
   * the server that is the next to be shed.
   */
  static class BalanceInfo {

    private int nextRegionForUnload;
    private int numRegionsAdded;
    private List<HRegionInfo> hriList;

    public BalanceInfo(int nextRegionForUnload, int numRegionsAdded, List<HRegionInfo> hriList) {
      this.nextRegionForUnload = nextRegionForUnload;
      this.numRegionsAdded = numRegionsAdded;
      this.hriList = hriList;
    }

    int getNextRegionForUnload() {
      return nextRegionForUnload;
    }

    int getNumRegionsAdded() {
      return numRegionsAdded;
    }

    void setNumRegionsAdded(int numAdded) {
      this.numRegionsAdded = numAdded;
    }

    List<HRegionInfo> getHriList() {
      return hriList;
    }

    void setNextRegionForUnload(int nextRegionForUnload) {
      this.nextRegionForUnload = nextRegionForUnload;
    }

  }

  public void setClusterLoad(Map<TableName, Map<ServerName, List<HRegionInfo>>> clusterLoad){
    serverLoadList = new ArrayList<>();
    float sum = 0;
    for(Map.Entry<TableName, Map<ServerName, List<HRegionInfo>>> clusterEntry : clusterLoad.entrySet()){
      for(Map.Entry<ServerName, List<HRegionInfo>> entry : clusterEntry.getValue().entrySet()){
        if(entry.getKey().equals(masterServerName)) continue; // we shouldn't include master as potential assignee
        serverLoadList.add(new ServerAndLoad(entry.getKey(), entry.getValue().size()));
        sum += entry.getValue().size();
      }
    }
    avgLoadOverall = sum / serverLoadList.size();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    float originSlop = slop;
    float originOverallSlop = overallSlop;
    super.setConf(conf);
    LOG.info("Update configuration of SimpleLoadBalancer, previous slop is "
            + originSlop + ", current slop is " + slop + "previous overallSlop is" +
            originOverallSlop + ", current overallSlop is " + originOverallSlop);
  }

  private void setLoad(List<ServerAndLoad> slList, int i, int loadChange){
    ServerAndLoad newsl = new ServerAndLoad(slList.get(i).getServerName(),slList.get(i).getLoad() + loadChange);
    slList.set(i, newsl);
  }

  /**
   * A checker function to decide when we want balance overall and certain table has been balanced,
   * do we still need to re-distribute regions of this table to achieve the state of overall-balance
   * @return true if this table should be balanced.
   */
  private boolean overallNeedsBalance() {
    int floor = (int) Math.floor(avgLoadOverall * (1 - overallSlop));
    int ceiling = (int) Math.ceil(avgLoadOverall * (1 + overallSlop));
    int max = 0, min = Integer.MAX_VALUE;
    for(ServerAndLoad server : serverLoadList){
      max = Math.max(server.getLoad(), max);
      min = Math.min(server.getLoad(), min);
    }
    if (max <= ceiling && min >= floor) {
      if (LOG.isTraceEnabled()) {
        // If nothing to balance, then don't say anything unless trace-level logging.
        LOG.trace("Skipping load balancing because cluster is balanced at overall level");
      }
      return false;
    }
    return true;
  }

  /**
   * Generate a global load balancing plan according to the specified map of
   * server information to the most loaded regions of each server.
   *
   * The load balancing invariant is that all servers are within 1 region of the
   * average number of regions per server.  If the average is an integer number,
   * all servers will be balanced to the average.  Otherwise, all servers will
   * have either floor(average) or ceiling(average) regions.
   *
   * HBASE-3609 Modeled regionsToMove using Guava's MinMaxPriorityQueue so that
   *   we can fetch from both ends of the queue.
   * At the beginning, we check whether there was empty region server
   *   just discovered by Master. If so, we alternately choose new / old
   *   regions from head / tail of regionsToMove, respectively. This alternation
   *   avoids clustering young regions on the newly discovered region server.
   *   Otherwise, we choose new regions from head of regionsToMove.
   *
   * Another improvement from HBASE-3609 is that we assign regions from
   *   regionsToMove to underloaded servers in round-robin fashion.
   *   Previously one underloaded server would be filled before we move onto
   *   the next underloaded server, leading to clustering of young regions.
   *
   * Finally, we randomly shuffle underloaded servers so that they receive
   *   offloaded regions relatively evenly across calls to balanceCluster().
   *
   * The algorithm is currently implemented as such:
   *
   * <ol>
   * <li>Determine the two valid numbers of regions each server should have,
   *     <b>MIN</b>=floor(average) and <b>MAX</b>=ceiling(average).
   *
   * <li>Iterate down the most loaded servers, shedding regions from each so
   *     each server hosts exactly <b>MAX</b> regions.  Stop once you reach a
   *     server that already has &lt;= <b>MAX</b> regions.
   *     <p>
   *     Order the regions to move from most recent to least.
   *
   * <li>Iterate down the least loaded servers, assigning regions so each server
   *     has exactly <b>MIN</b> regions.  Stop once you reach a server that
   *     already has &gt;= <b>MIN</b> regions.
   *
   *     Regions being assigned to underloaded servers are those that were shed
   *     in the previous step.  It is possible that there were not enough
   *     regions shed to fill each underloaded server to <b>MIN</b>.  If so we
   *     end up with a number of regions required to do so, <b>neededRegions</b>.
   *
   *     It is also possible that we were able to fill each underloaded but ended
   *     up with regions that were unassigned from overloaded servers but that
   *     still do not have assignment.
   *
   *     If neither of these conditions hold (no regions needed to fill the
   *     underloaded servers, no regions leftover from overloaded servers),
   *     we are done and return.  Otherwise we handle these cases below.
   *
   * <li>If <b>neededRegions</b> is non-zero (still have underloaded servers),
   *     we iterate the most loaded servers again, shedding a single server from
   *     each (this brings them from having <b>MAX</b> regions to having
   *     <b>MIN</b> regions).
   *
   * <li>We now definitely have more regions that need assignment, either from
   *     the previous step or from the original shedding from overloaded servers.
   *     Iterate the least loaded servers filling each to <b>MIN</b>.
   *
   * <li>If we still have more regions that need assignment, again iterate the
   *     least loaded servers, this time giving each one (filling them to
   *     <b>MAX</b>) until we run out.
   *
   * <li>All servers will now either host <b>MIN</b> or <b>MAX</b> regions.
   *
   *     In addition, any server hosting &gt;= <b>MAX</b> regions is guaranteed
   *     to end up with <b>MAX</b> regions at the end of the balancing.  This
   *     ensures the minimal number of regions possible are moved.
   * </ol>
   *
   * TODO: We can at-most reassign the number of regions away from a particular
   *       server to be how many they report as most loaded.
   *       Should we just keep all assignment in memory?  Any objections?
   *       Does this mean we need HeapSize on HMaster?  Or just careful monitor?
   *       (current thinking is we will hold all assignments in memory)
   *
   * @param clusterMap Map of regionservers and their load/region information to
   *                   a list of their most loaded regions
   * @return a list of regions to be moved, including source and destination,
   *         or null if cluster is already balanced
   */
  @Override
  public List<RegionPlan> balanceCluster(
      Map<ServerName, List<HRegionInfo>> clusterMap) {
    List<RegionPlan> regionsToReturn = balanceMasterRegions(clusterMap);
    if (regionsToReturn != null || clusterMap == null || clusterMap.size() <= 1) {
      return regionsToReturn;
    }
    if (masterServerName != null && clusterMap.containsKey(masterServerName)) {
      if (clusterMap.size() <= 2) {
        return null;
      }
      clusterMap = new HashMap<ServerName, List<HRegionInfo>>(clusterMap);
      clusterMap.remove(masterServerName);
    }

    long startTime = System.currentTimeMillis();

    // construct a Cluster object with clusterMap and rest of the
    // argument as defaults
    Cluster c = new Cluster(clusterMap, null, this.regionFinder, this.rackManager);
    if (!this.needsBalance(c) && !this.overallNeedsBalance()) return null;

    ClusterLoadState cs = new ClusterLoadState(clusterMap);
    int numServers = cs.getNumServers();
    NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
    int numRegions = cs.getNumRegions();
    float average = cs.getLoadAverage();
    int max = (int)Math.ceil(average);
    int min = (int)average;

    // Using to check balance result.
    StringBuilder strBalanceParam = new StringBuilder();
    strBalanceParam.append("Balance parameter: numRegions=").append(numRegions)
        .append(", numServers=").append(numServers).append(", max=").append(max)
        .append(", min=").append(min);
    LOG.debug(strBalanceParam.toString());

    // Balance the cluster
    // TODO: Look at data block locality or a more complex load to do this
    MinMaxPriorityQueue<RegionPlan> regionsToMove =
      MinMaxPriorityQueue.orderedBy(rpComparator).create();
    regionsToReturn = new ArrayList<RegionPlan>();

    // Walk down most loaded, pruning each to the max
    int serversOverloaded = 0;
    // flag used to fetch regions from head and tail of list, alternately
    boolean fetchFromTail = false;
    Map<ServerName, BalanceInfo> serverBalanceInfo =
      new TreeMap<ServerName, BalanceInfo>();
    for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server:
        serversByLoad.descendingMap().entrySet()) {
      ServerAndLoad sal = server.getKey();
      int load = sal.getLoad();
      if (load <= max) {
        serverBalanceInfo.put(sal.getServerName(), new BalanceInfo(0, 0, server.getValue()));
        continue;
      }
      serversOverloaded++;
      List<HRegionInfo> regions = server.getValue();
      int numToOffload = Math.min(load - max, regions.size());
      // account for the out-of-band regions which were assigned to this server
      // after some other region server crashed
      Collections.sort(regions, riComparator);
      int numTaken = 0;
      for (int i = 0; i <= numToOffload; ) {
        HRegionInfo hri = regions.get(i); // fetch from head
        if (fetchFromTail) {
          hri = regions.get(regions.size() - 1 - i);
        }
        i++;
        // Don't rebalance special regions.
        if (shouldBeOnMaster(hri)
            && masterServerName.equals(sal.getServerName())) continue;
        regionsToMove.add(new RegionPlan(hri, sal.getServerName(), null));
        numTaken++;
        if (numTaken >= numToOffload) break;
      }
      serverBalanceInfo.put(sal.getServerName(),
              new BalanceInfo(numToOffload, (-1)*numTaken, server.getValue()));
    }
    int totalNumMoved = regionsToMove.size();

    // Walk down least loaded, filling each to the min
    int neededRegions = 0; // number of regions needed to bring all up to min
    fetchFromTail = false;

    Map<ServerName, Integer> underloadedServers = new HashMap<ServerName, Integer>();
    int maxToTake = numRegions - min;
    for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server:
        serversByLoad.entrySet()) {
      if (maxToTake == 0) break; // no more to take
      int load = server.getKey().getLoad();
      if (load >= min) {
        continue; // look for other servers which haven't reached min
      }
      int regionsToPut = min - load;
      maxToTake -= regionsToPut;
      underloadedServers.put(server.getKey().getServerName(), regionsToPut);
    }
    // number of servers that get new regions
    int serversUnderloaded = underloadedServers.size();
    int incr = 1;
    List<ServerName> sns =
      Arrays.asList(underloadedServers.keySet().toArray(new ServerName[serversUnderloaded]));
    Collections.shuffle(sns, RANDOM);
    while (regionsToMove.size() > 0) {
      int cnt = 0;
      int i = incr > 0 ? 0 : underloadedServers.size()-1;
      for (; i >= 0 && i < underloadedServers.size(); i += incr) {
        if (regionsToMove.isEmpty()) break;
        ServerName si = sns.get(i);
        int numToTake = underloadedServers.get(si);
        if (numToTake == 0) continue;

        addRegionPlan(regionsToMove, fetchFromTail, si, regionsToReturn);

        underloadedServers.put(si, numToTake-1);
        cnt++;
        BalanceInfo bi = serverBalanceInfo.get(si);
        bi.setNumRegionsAdded(bi.getNumRegionsAdded()+1);
      }
      if (cnt == 0) break;
      // iterates underloadedServers in the other direction
      incr = -incr;
    }
    for (Integer i : underloadedServers.values()) {
      // If we still want to take some, increment needed
      neededRegions += i;
    }

    // Need to do a second pass.
    // Either more regions to assign out or servers that are still underloaded

    // If we need more to fill min, grab one from each most loaded until enough
    if (neededRegions != 0) {
      // Walk down most loaded, grabbing one from each until we get enough
      for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
        serversByLoad.descendingMap().entrySet()) {
        BalanceInfo balanceInfo =
          serverBalanceInfo.get(server.getKey().getServerName());
        int idx =
          balanceInfo == null ? 0 : balanceInfo.getNextRegionForUnload();
        if (idx >= server.getValue().size()) break;
        HRegionInfo region = server.getValue().get(idx);
        if (region.isMetaRegion()) continue; // Don't move meta regions.
        regionsToMove.add(new RegionPlan(region, server.getKey().getServerName(), null));
        balanceInfo.setNumRegionsAdded(balanceInfo.getNumRegionsAdded() - 1);
        balanceInfo.setNextRegionForUnload(balanceInfo.getNextRegionForUnload() + 1);
        totalNumMoved++;
        if (--neededRegions == 0) {
          // No more regions needed, done shedding
          break;
        }
      }
    }

    // Now we have a set of regions that must be all assigned out
    // Assign each underloaded up to the min, then if leftovers, assign to max

    // Walk down least loaded, assigning to each to fill up to min
    for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
        serversByLoad.entrySet()) {
      int regionCount = server.getKey().getLoad();
      if (regionCount >= min) break;
      BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey().getServerName());
      if(balanceInfo != null) {
        regionCount += balanceInfo.getNumRegionsAdded();
      }
      if(regionCount >= min) {
        continue;
      }
      int numToTake = min - regionCount;
      int numTaken = 0;
      while(numTaken < numToTake && 0 < regionsToMove.size()) {
        addRegionPlan(regionsToMove, fetchFromTail,
          server.getKey().getServerName(), regionsToReturn);
        numTaken++;
      }
    }

    if (min != max) {
      balanceOverall(regionsToReturn, serverBalanceInfo, fetchFromTail, regionsToMove, max, min);
    }

    long endTime = System.currentTimeMillis();

    if (!regionsToMove.isEmpty() || neededRegions != 0) {
      // Emit data so can diagnose how balancer went astray.
      LOG.warn("regionsToMove=" + totalNumMoved +
        ", numServers=" + numServers + ", serversOverloaded=" + serversOverloaded +
        ", serversUnderloaded=" + serversUnderloaded);
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<ServerName, List<HRegionInfo>> e: clusterMap.entrySet()) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(e.getKey().toString());
        sb.append(" ");
        sb.append(e.getValue().size());
      }
      LOG.warn("Input " + sb.toString());
    }

    // All done!
    LOG.info("Done. Calculated a load balance in " + (endTime-startTime) + "ms. " +
        "Moving " + totalNumMoved + " regions off of " +
        serversOverloaded + " overloaded servers onto " +
        serversUnderloaded + " less loaded servers");

    return regionsToReturn;
  }

  /**
   * If we need to balanceoverall, we need to add one more round to peel off one region from each max.
   * Together with other regions left to be assigned, we distribute all regionToMove, to the RS
   * that have less regions in whole cluster scope.
   */
  public void balanceOverall(List<RegionPlan> regionsToReturn,
                                       Map<ServerName, BalanceInfo> serverBalanceInfo, boolean fetchFromTail,
                                         MinMaxPriorityQueue<RegionPlan> regionsToMove, int max, int min ){
    // Step 1.
    // A map to record the plan we have already got as status quo, in order to resolve a cyclic assignment pair,
    // e.g. plan 1: A -> B, plan 2: B ->C => resolve plan1 to A -> C, remove plan2
    Map<ServerName, List<Integer>> returnMap = new HashMap<>();
    for (int i = 0; i < regionsToReturn.size(); i++) {
      List<Integer> pos = returnMap.get(regionsToReturn.get(i).getDestination());
      if (pos == null) {
        pos = new ArrayList<>();
        returnMap.put(regionsToReturn.get(i).getDestination(), pos);
      }
      pos.add(i);
    }

    // Step 2.
    // Peel off one region from each RS which has max number of regions now.
    // Each RS should have either max or min numbers of regions for this table.
    for (int i = 0; i < serverLoadList.size(); i++) {
      ServerAndLoad serverload = serverLoadList.get(i);
      BalanceInfo balanceInfo = serverBalanceInfo.get(serverload.getServerName());
      setLoad(serverLoadList, i, balanceInfo.getNumRegionsAdded());
      if (balanceInfo.getHriList().size() + balanceInfo.getNumRegionsAdded() == max) {
        HRegionInfo hriToPlan;
        if (balanceInfo.getHriList().size() == 0) {
          LOG.debug("During balanceOverall, we found " + serverload.getServerName()
                  + " has no HRegionInfo, no operation needed");
          continue;
        } else if (balanceInfo.getNextRegionForUnload() >= balanceInfo.getHriList().size()) {
          continue;
        } else {
          hriToPlan = balanceInfo.getHriList().get(balanceInfo.getNextRegionForUnload());
        }
        RegionPlan maxPlan = new RegionPlan(hriToPlan, serverload.getServerName(), null);
        regionsToMove.add(maxPlan);
        setLoad(serverLoadList, i, -1);
      }else if(balanceInfo.getHriList().size() + balanceInfo.getNumRegionsAdded() > max
              || balanceInfo.getHriList().size() + balanceInfo.getNumRegionsAdded() < min){
        LOG.warn("Encounter incorrect region numbers after calculating move plan during balanceOverall, " +
                "for this table, " + serverload.getServerName() + " originally has " + balanceInfo.getHriList().size() +
                " regions and " + balanceInfo.getNumRegionsAdded() + " regions have been added. Yet, max =" +
                max + ", min =" + min + ". Thus stop balance for this table"); // should not happen
        return;
      }
    }

    // Step 3. sort the ServerLoadList, the ArrayList hold overall load for each server.
    // We only need to assign the regionsToMove to
    // the first n = regionsToMove.size() RS that has least load.
    Collections.sort(serverLoadList,new Comparator<ServerAndLoad>(){
      @Override
      public int compare(ServerAndLoad s1, ServerAndLoad s2) {
        if(s1.getLoad() == s2.getLoad()) return 0;
        else return (s1.getLoad() > s2.getLoad())? 1 : -1;
      }});

    // Step 4.
    // Preparation before assign out all regionsToMove.
    // We need to remove the plan that has the source RS equals to destination RS,
    // since the source RS belongs to the least n loaded RS.
    int assignLength = regionsToMove.size();
    // A structure help to map ServerName to  it's load and index in ServerLoadList
    Map<ServerName, Pair<ServerAndLoad,Integer>> SnLoadMap = new HashMap<>();
    for (int i = 0; i < serverLoadList.size(); i++) {
      SnLoadMap.put(serverLoadList.get(i).getServerName(),
              new Pair<ServerAndLoad, Integer>(serverLoadList.get(i), i));
    }
    Pair<ServerAndLoad,Integer> shredLoad;
    // A List to help mark the plan in regionsToMove that should be removed
    List<RegionPlan> planToRemoveList = new ArrayList<>();
    // A structure to record how many times a server becomes the source of a plan, from regionsToMove.
    Map<ServerName, Integer> sourceMap = new HashMap<>();
    // We remove one of the plan which would cause source RS equals destination RS.
    // But we should keep in mind that the second plan from such RS should be kept.
    for(RegionPlan plan: regionsToMove){
      // the source RS's load and index in ServerLoadList
      shredLoad = SnLoadMap.get(plan.getSource());
      if(!sourceMap.containsKey(plan.getSource())) sourceMap.put(plan.getSource(), 0);
      sourceMap.put(plan.getSource(), sourceMap.get(plan.getSource()) + 1);
      if(shredLoad.getSecond() < assignLength && sourceMap.get(plan.getSource()) == 1) {
        planToRemoveList.add(plan);
        // While marked as to be removed, the count should be add back to the source RS
        setLoad(serverLoadList, shredLoad.getSecond(), 1);
      }
    }
    // Remove those marked plans from regionsToMove,
    // we cannot direct remove them during iterating through
    // regionsToMove, due to the fact that regionsToMove is a MinMaxPriorityQueue.
    for(RegionPlan planToRemove : planToRemoveList){
      regionsToMove.remove(planToRemove);
    }

    // Step 5.
    // We only need to assign the regionsToMove to
    // the first n = regionsToMove.size() of them, with least load.
    // With this strategy adopted, we can gradually achieve the overall balance,
    // while keeping table level balanced.
    for(int i = 0; i < assignLength; i++){
      // skip the RS that is also the source, we have removed them from regionsToMove in previous step
      if(sourceMap.containsKey(serverLoadList.get(i).getServerName())) continue;
      addRegionPlan(regionsToMove, fetchFromTail,
              serverLoadList.get(i).getServerName(), regionsToReturn);
      setLoad(serverLoadList, i, 1);
      // resolve a possible cyclic assignment pair if we just produced one:
      // e.g. plan1: A -> B, plan2: B -> C => resolve plan1 to A -> C and remove plan2
      List<Integer> pos = returnMap.get(regionsToReturn.get(regionsToReturn.size() - 1).getSource());
      if (pos != null && pos.size() != 0) {
        regionsToReturn.get(pos.get(pos.size() - 1)).setDestination(
                regionsToReturn.get(regionsToReturn.size() - 1).getDestination());
        pos.remove(pos.size() - 1);
        regionsToReturn.remove(regionsToReturn.size() - 1);
      }
    }
    // Done balance overall
  }

  /**
   * Add a region from the head or tail to the List of regions to return.
   */
  private void addRegionPlan(final MinMaxPriorityQueue<RegionPlan> regionsToMove,
      final boolean fetchFromTail, final ServerName sn, List<RegionPlan> regionsToReturn) {
    RegionPlan rp = null;
    if (!fetchFromTail) rp = regionsToMove.remove();
    else rp = regionsToMove.removeLast();
    rp.setDestination(sn);
    regionsToReturn.add(rp);
  }

  @Override
  public List<RegionPlan> balanceCluster(TableName tableName,
      Map<ServerName, List<HRegionInfo>> clusterState) throws HBaseIOException {
    LOG.debug("Start Generate Balance plan for table: " + tableName);
    return balanceCluster(clusterState);
  }
}
