/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(Map)}.
 *
 * <p>Inline region placement with {@link #immediateAssignment} can be used when
 * the Master needs to handle closed regions that it currently does not have
 * a destination set for.  This can happen during master failover.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This classes produces plans for the 
 * {@link org.apache.hadoop.hbase.master.AssignmentManager} to execute.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG) public class GroupLoadBalancer
    extends BaseLoadBalancer {
  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private RegionInfoComparator riComparator = new RegionInfoComparator();
  private RegionPlan.RegionPlanComparator rpComparator = new RegionPlan.RegionPlanComparator();

  /**
   * Stores additional per-server information about the regions added/removed
   * during the run of the balancing algorithm.
   *
   * For servers that shed regions, we need to track which regions we have already
   * shed. <b>nextRegionForUnload</b> contains the index in the list of regions on
   * the server that is the next to be shed.
   */
  static class BalanceInfo {

    private final int nextRegionForUnload;
    private int numRegionsAdded;

    public BalanceInfo(int nextRegionForUnload, int numRegionsAdded) {
      this.nextRegionForUnload = nextRegionForUnload;
      this.numRegionsAdded = numRegionsAdded;
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
   *     has exactly </b>MIN</b> regions.  Stop once you reach a server that
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
   *     </b>MAX</b>) until we run out.
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
  @Override public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterMap) {

    LOG.info("**************** USING GROUP LOAD BALANCER *******************");

    LOG.debug("**************** Debug enabled");

    LOG.info("**************** masterServerName " + masterServerName);

    // don't balance master
    if (masterServerName != null && clusterMap.containsKey(masterServerName)) {
      clusterMap = new HashMap<ServerName, List<HRegionInfo>>(clusterMap);
      clusterMap.remove(masterServerName);
    }

    // see if master regions need to be balanced
    List<RegionPlan> regionsToReturn = balanceMasterRegions(clusterMap);
    if (regionsToReturn != null) {
      LOG.info("**************** Master regions need to be balanced " + regionsToReturn);
      return regionsToReturn;
    }

    LOG.info("**************** clusterMap without masterServerName  *******************");

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterMap.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> hriList = entry.getValue();
      LOG.info("**************** serverName " + serverName);
      for (HRegionInfo hri : hriList) {
        LOG.info("**************** HRegionInfo shortname " + hri.getRegionNameAsString());
      }
    }

    // Move all tables to either first or last server
    ServerName destinationServer1 = masterServerName;
    ServerName destinationServer2 = masterServerName;

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterMap.entrySet()) {
      ServerName serverName = entry.getKey();
      if (serverName.toString().contains("10.255.196.145,60020")) {
        destinationServer1 = serverName;
      } else {
        destinationServer2 = serverName;
      }
    }

    if (destinationServer1 == masterServerName) {
      LOG.info("**************** destinationServer was not changed");
    } else {
      LOG.info("**************** destinationServer was changed to " + destinationServer1.toString());
    }

    regionsToReturn = new ArrayList<RegionPlan>();

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterMap.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> hriList = entry.getValue();
      for (HRegionInfo  hri : hriList) {
        ServerName destinationServer = hri.toString().contains("test")?destinationServer1:destinationServer2;
        LOG.info("\"**************** Region " + hri + " is going to " + destinationServer);
        RegionPlan rp = new RegionPlan(hri, serverName, destinationServer);
        LOG.info("**************** rp " + rp);
        regionsToReturn.add(rp);
        LOG.info("**************** here");
      }
    }

    //        RegionPlan rp = null;
    //        if (!fetchFromTail) rp = regionsToMove.remove();
    //        else rp = regionsToMove.removeLast();
    //        rp.setDestination(sn);
    //        regionsToReturn.add(rp);

    LOG.info("**************** regionsToReturn " + regionsToReturn);

    return regionsToReturn;

    //        LOG.info("**************** clusterMap " + clusterMap);
    //
    //        List<RegionPlan> regionsToReturn = balanceMasterRegions(clusterMap);
    //        if (regionsToReturn != null || clusterMap == null || clusterMap.size() <= 1) {
    //            return regionsToReturn;
    //        }
    //        if (masterServerName != null && clusterMap.containsKey(masterServerName)) {
    //            if (clusterMap.size() <= 2) {
    //                return null;
    //            }
    //            clusterMap = new HashMap<ServerName, List<HRegionInfo>>(clusterMap);
    //            clusterMap.remove(masterServerName);
    //        }
    //
    //        long startTime = System.currentTimeMillis();
    //
    //        // construct a Cluster object with clusterMap and rest of the
    //        // argument as defaults
    //        Cluster c = new Cluster(clusterMap, null, this.regionFinder, this.rackManager);
    ////        LOG.info("****************  this.needsBalance(c) " + this.needsBalance(c));
    ////        if (!this.needsBalance(c)) return null;
    //
    //        ClusterLoadState cs = new ClusterLoadState(clusterMap);
    //        int numServers = cs.getNumServers();
    //        NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
    //        int numRegions = cs.getNumRegions();
    //        float average = cs.getLoadAverage();
    //        int max = (int)Math.ceil(average);
    //        int min = (int)average;
    //
    //        // Using to check balance result.
    //        StringBuilder strBalanceParam = new StringBuilder();
    //        strBalanceParam.append("Balance parameter: numRegions=").append(numRegions)
    //                .append(", numServers=").append(numServers).append(", max=").append(max)
    //                .append(", min=").append(min);
    //        LOG.debug(strBalanceParam.toString());
    //
    //        // Balance the cluster
    //        // TODO: Look at data block locality or a more complex load to do this
    //        MinMaxPriorityQueue<RegionPlan> regionsToMove =
    //                MinMaxPriorityQueue.orderedBy(rpComparator).create();
    //        LOG.info("**************** regionsToMove " + regionsToMove);
    //        regionsToReturn = new ArrayList<RegionPlan>();
    //
    //        // Walk down most loaded, pruning each to the max
    //        int serversOverloaded = 0;
    //        // flag used to fetch regions from head and tail of list, alternately
    //        boolean fetchFromTail = false;
    //        Map<ServerName, BalanceInfo> serverBalanceInfo =
    //                new TreeMap<ServerName, BalanceInfo>();
    //        for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server:
    //                serversByLoad.descendingMap().entrySet()) {
    //            LOG.info("**************** server " + server);
    //            ServerAndLoad sal = server.getKey();
    //            int load = sal.getLoad();
    //            if (load <= max) {
    //                serverBalanceInfo.put(sal.getServerName(), new BalanceInfo(0, 0));
    //                break;
    //            }
    //            serversOverloaded++;
    //            List<HRegionInfo> regions = server.getValue();
    //            int numToOffload = Math.min(load - max, regions.size());
    //            // account for the out-of-band regions which were assigned to this server
    //            // after some other region server crashed
    //            Collections.sort(regions, riComparator);
    //            int numTaken = 0;
    //            for (int i = 0; i <= numToOffload; ) {
    //                HRegionInfo hri = regions.get(i); // fetch from head
    //                if (fetchFromTail) {
    //                    hri = regions.get(regions.size() - 1 - i);
    //                }
    //                i++;
    //                // Don't rebalance special regions.
    //                if (shouldBeOnMaster(hri)
    //                        && masterServerName.equals(sal.getServerName())) continue;
    //                regionsToMove.add(new RegionPlan(hri, sal.getServerName(), null));
    //                numTaken++;
    //                if (numTaken >= numToOffload) break;
    //            }
    //            serverBalanceInfo.put(sal.getServerName(),
    //                    new BalanceInfo(numToOffload, (-1)*numTaken));
    //        }
    //        int totalNumMoved = regionsToMove.size();
    //
    //        // Walk down least loaded, filling each to the min
    //        int neededRegions = 0; // number of regions needed to bring all up to min
    //        fetchFromTail = false;
    //
    //        Map<ServerName, Integer> underloadedServers = new HashMap<ServerName, Integer>();
    //        int maxToTake = numRegions - min;
    //        for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server:
    //                serversByLoad.entrySet()) {
    //            if (maxToTake == 0) break; // no more to take
    //            int load = server.getKey().getLoad();
    //            if (load >= min && load > 0) {
    //                continue; // look for other servers which haven't reached min
    //            }
    //            int regionsToPut = min - load;
    //            if (regionsToPut == 0)
    //            {
    //                regionsToPut = 1;
    //            }
    //            maxToTake -= regionsToPut;
    //            underloadedServers.put(server.getKey().getServerName(), regionsToPut);
    //            LOG.info("**************** regionsToMove " + regionsToMove);
    //        }
    //        // number of servers that get new regions
    //        int serversUnderloaded = underloadedServers.size();
    //        int incr = 1;
    //        List<ServerName> sns =
    //                Arrays.asList(underloadedServers.keySet().toArray(new ServerName[serversUnderloaded]));
    //        Collections.shuffle(sns, RANDOM);
    //        while (regionsToMove.size() > 0) {
    //            int cnt = 0;
    //            int i = incr > 0 ? 0 : underloadedServers.size()-1;
    //            for (; i >= 0 && i < underloadedServers.size(); i += incr) {
    //                if (regionsToMove.isEmpty()) break;
    //                ServerName si = sns.get(i);
    //                int numToTake = underloadedServers.get(si);
    //                if (numToTake == 0) continue;
    //
    //                addRegionPlan(regionsToMove, fetchFromTail, si, regionsToReturn);
    //
    //                underloadedServers.put(si, numToTake-1);
    //                cnt++;
    //                BalanceInfo bi = serverBalanceInfo.get(si);
    //                if (bi == null) {
    //                    bi = new BalanceInfo(0, 0);
    //                    serverBalanceInfo.put(si, bi);
    //                }
    //                bi.setNumRegionsAdded(bi.getNumRegionsAdded()+1);
    //            }
    //            if (cnt == 0) break;
    //            // iterates underloadedServers in the other direction
    //            incr = -incr;
    //        }
    //        for (Integer i : underloadedServers.values()) {
    //            // If we still want to take some, increment needed
    //            neededRegions += i;
    //        }
    //
    //        // If none needed to fill all to min and none left to drain all to max,
    //        // we are done
    //        if (neededRegions == 0 && regionsToMove.isEmpty()) {
    //            long endTime = System.currentTimeMillis();
    //            LOG.info("Calculated a load balance in " + (endTime-startTime) + "ms. " +
    //                    "Moving " + totalNumMoved + " regions off of " +
    //                    serversOverloaded + " overloaded servers onto " +
    //                    serversUnderloaded + " less loaded servers");
    //            LOG.info("**************** regionsToReturn " + regionsToReturn);
    //            return regionsToReturn;
    //        }
    //
    //        // Need to do a second pass.
    //        // Either more regions to assign out or servers that are still underloaded
    //
    //        // If we need more to fill min, grab one from each most loaded until enough
    //        if (neededRegions != 0) {
    //            // Walk down most loaded, grabbing one from each until we get enough
    //            for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
    //                    serversByLoad.descendingMap().entrySet()) {
    //                BalanceInfo balanceInfo =
    //                        serverBalanceInfo.get(server.getKey().getServerName());
    //                int idx =
    //                        balanceInfo == null ? 0 : balanceInfo.getNextRegionForUnload();
    //                if (idx >= server.getValue().size()) break;
    //                HRegionInfo region = server.getValue().get(idx);
    //                if (region.isMetaRegion()) continue; // Don't move meta regions.
    //                regionsToMove.add(new RegionPlan(region, server.getKey().getServerName(), null));
    //                totalNumMoved++;
    //                if (--neededRegions == 0) {
    //                    // No more regions needed, done shedding
    //                    break;
    //                }
    //            }
    //        }
    //
    //        // Now we have a set of regions that must be all assigned out
    //        // Assign each underloaded up to the min, then if leftovers, assign to max
    //
    //        // Walk down least loaded, assigning to each to fill up to min
    //        for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
    //                serversByLoad.entrySet()) {
    //            int regionCount = server.getKey().getLoad();
    //            if (regionCount >= min) break;
    //            BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey().getServerName());
    //            if(balanceInfo != null) {
    //                regionCount += balanceInfo.getNumRegionsAdded();
    //            }
    //            if(regionCount >= min) {
    //                continue;
    //            }
    //            int numToTake = min - regionCount;
    //            int numTaken = 0;
    //            while(numTaken < numToTake && 0 < regionsToMove.size()) {
    //                addRegionPlan(regionsToMove, fetchFromTail,
    //                        server.getKey().getServerName(), regionsToReturn);
    //                numTaken++;
    //            }
    //        }
    //
    //        // If we still have regions to dish out, assign underloaded to max
    //        if (0 < regionsToMove.size()) {
    //            for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
    //                    serversByLoad.entrySet()) {
    //                int regionCount = server.getKey().getLoad();
    //                BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey().getServerName());
    //                if(balanceInfo != null) {
    //                    regionCount += balanceInfo.getNumRegionsAdded();
    //                }
    //                if(regionCount >= max) {
    //                    break;
    //                }
    //                addRegionPlan(regionsToMove, fetchFromTail,
    //                        server.getKey().getServerName(), regionsToReturn);
    //                if (regionsToMove.isEmpty()) {
    //                    break;
    //                }
    //            }
    //        }
    //
    //        long endTime = System.currentTimeMillis();
    //
    //        if (!regionsToMove.isEmpty() || neededRegions != 0) {
    //            // Emit data so can diagnose how balancer went astray.
    //            LOG.warn("regionsToMove=" + totalNumMoved +
    //                    ", numServers=" + numServers + ", serversOverloaded=" + serversOverloaded +
    //                    ", serversUnderloaded=" + serversUnderloaded);
    //            StringBuilder sb = new StringBuilder();
    //            for (Map.Entry<ServerName, List<HRegionInfo>> e: clusterMap.entrySet()) {
    //                if (sb.length() > 0) sb.append(", ");
    //                sb.append(e.getKey().toString());
    //                sb.append(" ");
    //                sb.append(e.getValue().size());
    //            }
    //            LOG.warn("Input " + sb.toString());
    //        }
    //
    //        // All done!
    //        LOG.info("Done. Calculated a load balance in " + (endTime-startTime) + "ms. " +
    //                "Moving " + totalNumMoved + " regions off of " +
    //                serversOverloaded + " overloaded servers onto " +
    //                serversUnderloaded + " less loaded servers");
    //
    //        LOG.info("**************** regionsToReturn " + regionsToReturn);
    //        return regionsToReturn;
    //    }
    //
    //    /**
    //     * Add a region from the head or tail to the List of regions to return.
    //     */
    //    private void addRegionPlan(final MinMaxPriorityQueue<RegionPlan> regionsToMove,
    //                               final boolean fetchFromTail, final ServerName sn, List<RegionPlan> regionsToReturn) {
    //        RegionPlan rp = null;
    //        if (!fetchFromTail) rp = regionsToMove.remove();
    //        else rp = regionsToMove.removeLast();
    //        rp.setDestination(sn);
    //        regionsToReturn.add(rp);
    //    }
  }
}
