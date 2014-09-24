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
package org.apache.hadoop.hbase.master;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionState.State;

/**
 * Run bulk assign.  Does one RCP per regionserver passing a
 * batch of regions using {@link GeneralBulkAssigner.SingleServerBulkAssigner}.
 */
@InterfaceAudience.Private
public class GeneralBulkAssigner extends BulkAssigner {
  private static final Log LOG = LogFactory.getLog(GeneralBulkAssigner.class);

  private Map<ServerName, List<HRegionInfo>> failedPlans
    = new ConcurrentHashMap<ServerName, List<HRegionInfo>>();
  private ExecutorService pool;

  final Map<ServerName, List<HRegionInfo>> bulkPlan;
  final AssignmentManager assignmentManager;
  final boolean waitTillAllAssigned;

  public GeneralBulkAssigner(final Server server,
      final Map<ServerName, List<HRegionInfo>> bulkPlan,
      final AssignmentManager am, final boolean waitTillAllAssigned) {
    super(server);
    this.bulkPlan = bulkPlan;
    this.assignmentManager = am;
    this.waitTillAllAssigned = waitTillAllAssigned;
  }

  @Override
  protected String getThreadNamePrefix() {
    return this.server.getServerName() + "-GeneralBulkAssigner";
  }

  @Override
  protected void populatePool(ExecutorService pool) {
    this.pool = pool; // shut it down later in case some assigner hangs
    for (Map.Entry<ServerName, List<HRegionInfo>> e: this.bulkPlan.entrySet()) {
      pool.execute(new SingleServerBulkAssigner(e.getKey(), e.getValue(),
        this.assignmentManager, this.failedPlans));
    }
  }

  /**
   *
   * @param timeout How long to wait.
   * @return true if done.
   */
  @Override
  protected boolean waitUntilDone(final long timeout)
  throws InterruptedException {
    Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
    for (List<HRegionInfo> regionList : bulkPlan.values()) {
      regionSet.addAll(regionList);
    }

    pool.shutdown(); // no more task allowed
    int serverCount = bulkPlan.size();
    int regionCount = regionSet.size();
    long startTime = System.currentTimeMillis();
    long rpcWaitTime = startTime + timeout;
    while (!server.isStopped() && !pool.isTerminated()
        && rpcWaitTime > System.currentTimeMillis()) {
      if (failedPlans.isEmpty()) {
        pool.awaitTermination(100, TimeUnit.MILLISECONDS);
      } else {
        reassignFailedPlans();
      }
    }
    if (!pool.isTerminated()) {
      LOG.warn("bulk assigner is still running after "
        + (System.currentTimeMillis() - startTime) + "ms, shut it down now");
      // some assigner hangs, can't wait any more, shutdown the pool now
      List<Runnable> notStarted = pool.shutdownNow();
      if (notStarted != null && !notStarted.isEmpty()) {
        server.abort("some single server assigner hasn't started yet"
          + " when the bulk assigner timed out", null);
        return false;
      }
    }

    int reassigningRegions = 0;
    if (!failedPlans.isEmpty() && !server.isStopped()) {
      reassigningRegions = reassignFailedPlans();
    }

    Configuration conf = server.getConfiguration();
    long perRegionOpenTimeGuesstimate =
      conf.getLong("hbase.bulk.assignment.perregion.open.time", 1000);
    long endTime = Math.max(System.currentTimeMillis(), rpcWaitTime)
      + perRegionOpenTimeGuesstimate * (reassigningRegions + 1);
    RegionStates regionStates = assignmentManager.getRegionStates();
    // We're not synchronizing on regionsInTransition now because we don't use any iterator.
    while (!regionSet.isEmpty() && !server.isStopped() && endTime > System.currentTimeMillis()) {
      Iterator<HRegionInfo> regionInfoIterator = regionSet.iterator();
      while (regionInfoIterator.hasNext()) {
        HRegionInfo hri = regionInfoIterator.next();
        if (regionStates.isRegionOnline(hri) || regionStates.isRegionInState(hri,
            State.SPLITTING, State.SPLIT, State.MERGING, State.MERGED)) {
          regionInfoIterator.remove();
        }
      }
      if (!waitTillAllAssigned) {
        // No need to wait, let assignment going on asynchronously
        break;
      }
      if (!regionSet.isEmpty()) {
        regionStates.waitForUpdate(100);
      }
    }

    if (LOG.isDebugEnabled()) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      String status = "successfully";
      if (!regionSet.isEmpty()) {
        status = "with " + regionSet.size() + " regions still in transition";
      }
      LOG.debug("bulk assigning total " + regionCount + " regions to "
        + serverCount + " servers, took " + elapsedTime + "ms, " + status);
    }
    return regionSet.isEmpty();
  }

  @Override
  protected long getTimeoutOnRIT() {
    // Guess timeout.  Multiply the max number of regions on a server
    // by how long we think one region takes opening.
    Configuration conf = server.getConfiguration();
    long perRegionOpenTimeGuesstimate =
      conf.getLong("hbase.bulk.assignment.perregion.open.time", 1000);
    int maxRegionsPerServer = 1;
    for (List<HRegionInfo> regionList : bulkPlan.values()) {
      int size = regionList.size();
      if (size > maxRegionsPerServer) {
        maxRegionsPerServer = size;
      }
    }
    long timeout = perRegionOpenTimeGuesstimate * maxRegionsPerServer
      + conf.getLong("hbase.regionserver.rpc.startup.waittime", 60000)
      + conf.getLong("hbase.bulk.assignment.perregionserver.rpc.waittime",
        30000) * bulkPlan.size();
    LOG.debug("Timeout-on-RIT=" + timeout);
    return timeout;
  }

  @Override
  protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
    return new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.warn("Assigning regions in " + t.getName(), e);
      }
    };
  }

  private int reassignFailedPlans() {
    List<HRegionInfo> reassigningRegions = new ArrayList<HRegionInfo>();
    for (Map.Entry<ServerName, List<HRegionInfo>> e : failedPlans.entrySet()) {
      LOG.info("Failed assigning " + e.getValue().size()
          + " regions to server " + e.getKey() + ", reassigning them");
      reassigningRegions.addAll(failedPlans.remove(e.getKey()));
    }
    RegionStates regionStates = assignmentManager.getRegionStates();
    for (HRegionInfo region : reassigningRegions) {
      if (!regionStates.isRegionOnline(region)) {
        assignmentManager.invokeAssign(region);
      }
    }
    return reassigningRegions.size();
  }

  /**
   * Manage bulk assigning to a server.
   */
  static class SingleServerBulkAssigner implements Runnable {
    private final ServerName regionserver;
    private final List<HRegionInfo> regions;
    private final AssignmentManager assignmentManager;
    private final Map<ServerName, List<HRegionInfo>> failedPlans;

    SingleServerBulkAssigner(final ServerName regionserver,
        final List<HRegionInfo> regions, final AssignmentManager am,
        final Map<ServerName, List<HRegionInfo>> failedPlans) {
      this.regionserver = regionserver;
      this.regions = regions;
      this.assignmentManager = am;
      this.failedPlans = failedPlans;
    }

    @Override
    public void run() {
      try {
       if (!assignmentManager.assign(regionserver, regions)) {
         failedPlans.put(regionserver, regions);
       }
      } catch (Throwable t) {
        LOG.warn("Failed bulking assigning " + regions.size()
            + " region(s) to " + regionserver.getServerName()
            + ", and continue to bulk assign others", t);
        failedPlans.put(regionserver, regions);
      }
    }
  }
}
