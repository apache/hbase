/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.master.AssignmentPlan.POSITION;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Manages the preferences for assigning regions to specific servers.
 * It get the assignment plan from scanning the META region and keep this
 * assignment plan updated.
 *
 * The assignment manager executes the assignment plan by adding the regions
 * with its most favored live region server into the transient assignment.
 * Each transient assignment will be only valid for a configurable time
 * before expire. During these valid time, the region will only be assigned
 * based on the transient assignment.
 *
 * All access to this class is thread-safe.
 */
public class AssignmentManager {
  protected static final Log LOG = LogFactory.getLog(
      AssignmentManager.class);

  /**
   * The transient assignment map between each region server and
   * its attempted assigning regions
   */
  private Map<HServerAddress, Set<HRegionInfo>> transientAssignments;

  /**
   * Set of all regions that have a transient preferred assignment, used for
   * quick lookup.
   */
  private Set<HRegionInfo> regionsWithTransientAssignment;

  /**
   * Queue of transient assignments and their upcoming timeouts. When a
   * transient assignment expires from this queue, it will be removed from
   * the transient assignment map.
   */
  private DelayQueue<TransisentAssignment> transientAssignmentTimeout;

  /**
   * This thread polls the timeout queue and removes any assignments which
   * have timed out.
   */
  private TransientAssignmentHandler transientAssignmentHandler;

  /**
   * The assignment plan which contains the mapping between each region and its
   * favored region server list.
   */
  private AssignmentPlan assignmentPlan;

  private final HMaster master;
  private final Configuration conf;
  private long millisecondDelay;
  private POSITION[] positions;

  public AssignmentManager(HMaster master) {
    this.master = master;
    this.conf = master.getConfiguration();

    this.transientAssignmentTimeout = new DelayQueue<TransisentAssignment>();
    this.transientAssignmentHandler = new TransientAssignmentHandler();
    this.transientAssignments = new HashMap<HServerAddress, Set<HRegionInfo>>();
    this.regionsWithTransientAssignment = new HashSet<HRegionInfo>();
    this.millisecondDelay = conf.getLong(
        "hbase.regionserver.transientAssignment.regionHoldPeriod", 30000);

    this.assignmentPlan = new AssignmentPlan();
    positions = AssignmentPlan.POSITION.values();
  }

  public void start() {
    Threads.setDaemonThreadRunning(transientAssignmentHandler,
        "RegionManager.transientAssignmentHandler");
  }

  /**
   * Add a transient assignment for a region to a server. If the region already
   * has a transient assignment, then this method will do nothing.
   * @param server
   * @param region
   */
  public void addTransientAssignment(HServerAddress server,
      HRegionInfo region) {
    synchronized (transientAssignments) {
      if (regionsWithTransientAssignment.contains(region)) {
        LOG.info("Attempted to add transient assignment for " +
            region.getRegionNameAsString() + " to " +
            server.getHostNameWithPort() +
            " but already had assignment, new assignment ignored");
        return;
      }
      Set<HRegionInfo> regions = transientAssignments.get(server);
      if (regions == null) {
        regions = new ConcurrentSkipListSet<HRegionInfo>();
        transientAssignments.put(server, regions);
      }

      regions.add(region);
      regionsWithTransientAssignment.add(region);
      LOG.info("Add transient assignment for " +
          region.getRegionNameAsString() + " to " + server.getHostAddressWithPort());
    }
    // Add to delay queue
    transientAssignmentTimeout.add(new TransisentAssignment(region, server,
        EnvironmentEdgeManager.currentTimeMillis(), millisecondDelay));
  }

  /**
   * The assignment manager executes the assignment plan by adding the regions
   * with its most favored live region server into the transient assignment.
   * @param region get a transient assignment for this region
   */
  public void executeAssignmentPlan(HRegionInfo region) {
    List<HServerAddress> servers = this.getAssignmentFromPlan(region);
    if (servers != null) {
      for (int i = 0; i < servers.size(); i++) {
        HServerAddress server = servers.get(i);
        HServerInfo info = master.getServerManager().getHServerInfo(server);
        // A preferred server is only eligible for assignment if the master
        // knows about the server's info, the server is not in the collection of
        // dead servers, and the server has load information. Absence of load
        // information may indicate that the server is in the process of
        // shutting down.
        if (info != null &&
            !master.getServerManager().isDeadProcessingPending(info.getServerName()) &&
            master.getServerManager().getServersToServerInfo().get(info.getServerName()) != null &&
            !master.isServerBlackListed(info.getHostnamePort())) {
          LOG.info("Add a transient assignment from the assignment plan: "
              + " region " + region.getRegionNameAsString() + " to the "
              + positions[i] + " region server" + info.getHostnamePort());
          addTransientAssignment(info.getServerAddress(), region);
          return;
        }
      }
      LOG.warn("There is NO live favored region servers for the region " +
          region.getRegionNameAsString());
      return;
    }
    LOG.warn("There is no assignment plan for the region " +
        region.getRegionNameAsString());
  }

  public boolean removeTransientAssignment(HServerAddress server,
      HRegionInfo region) {
    synchronized (transientAssignments) {
      regionsWithTransientAssignment.remove(region);
      Set<HRegionInfo> regions = transientAssignments.get(server);
      if (regions != null) {
        regions.remove(region);
        if (regions.isEmpty()) {
          transientAssignments.remove(server);
        }
        LOG.debug("Remove the transisent assignment: region " +
            region.getRegionNameAsString() + " to " +
            server.getHostNameWithPort());
        return true;
      }
      return false;
    }
  }

  public Set<HRegionInfo> getTransientAssignments(HServerAddress server) {
    synchronized (transientAssignments) {
      return transientAssignments.get(server);
    }
  }

  public boolean hasTransientAssignment(HRegionInfo region) {
    synchronized (transientAssignments) {
      return regionsWithTransientAssignment.contains(region);
    }
  }

  /**
   * Add the assignment to the plan
   * @param region
   * @param servers
   * @param updateTimeStamp
   */
  public void updateAssignmentPlan(HRegionInfo region,
      List<HServerAddress> servers, long updateTimeStamp) {
   this.assignmentPlan.updateAssignmentPlan(region, servers,
       updateTimeStamp);
  }

  /**
   * Remove the assignment from the plan
   * @param region
   */
  public void removeAssignmentFromPlan(HRegionInfo region) {
    this.assignmentPlan.removeAssignment(region);
  }

  /**
   * @param region
   * @return true if there is a assignment for the region
   */
  public boolean hasAssignmentFromPlan(HRegionInfo region) {
    return this.assignmentPlan.hasAssignment(region);
  }

  /**
   * @param region
   * @return the server list which the region are supposed to be assigned to
   * based on the plan.
   */
  public List<HServerAddress> getAssignmentFromPlan(HRegionInfo region) {
    return this.assignmentPlan.getAssignment(region);
  }

  /**
   * @param region
   * @return the latest update time stamp for this region's favored assignment.
   */
  public long getAssignmentPlanUpdateTimeStamp(HRegionInfo region){
    return this.assignmentPlan.getAssignmentUpdateTS(region);
  }

  private class TransientAssignmentHandler extends HasThread {
    @Override
    public void run() {
      LOG.debug("Started TransientAssignmentHandler");
      TransisentAssignment plan = null;
      Configuration conf = master.getConfiguration();
      int resetFrequency = Math.min(
          conf.getInt("hbase.master.meta.thread.rescanfrequency",
              60 * 1000), // metaScanner runs at this rate
          10 * conf.getInt("hbase.regionserver.msginterval",
              HConstants.REGION_SERVER_MSG_INTERVAL)); // 10 regionServerReports
      while (!master.getClosed().get()) {
        try {
          // check if any regions waiting time expired
          plan = transientAssignmentTimeout.poll(resetFrequency,
              TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // no problem, just continue
          continue;
        }
        if (null == plan) {
          continue;
        }
        if (removeTransientAssignment(plan.getServer(), plan.getRegionInfo())) {
          LOG.info("Removed region from transient assignment: " +
              plan.getRegionInfo().getRegionNameAsString());
        }
      }
    }
  }

  private class TransisentAssignment implements Delayed {
    private long creationTime;
    private HRegionInfo region;
    private HServerAddress server;
    private long millisecondDelay;

    TransisentAssignment(HRegionInfo region, HServerAddress addr,
        long creationTime, long millisecondDelay) {
      this.region = region;
      this.server = addr;
      this.creationTime = creationTime;
      this.millisecondDelay = millisecondDelay;
    }

    public HServerAddress getServer() {
      return this.server;
    }

    public HRegionInfo getRegionInfo() {
      return this.region;
    }

    @Override
    public int compareTo(Delayed arg0) {
      long delta = this.getDelay(TimeUnit.MILLISECONDS)
          - arg0.getDelay(TimeUnit.MILLISECONDS);
      return (this.equals(arg0) ? 0 : (delta > 0 ? 1 : (delta < 0 ? -1 : 0)));
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(
          (this.creationTime + millisecondDelay) - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TransisentAssignment) {
        TransisentAssignment assignment = (TransisentAssignment)o;
        if (assignment.getServer().equals(this.getServer()) &&
            assignment.getRegionInfo().equals(this.getRegionInfo())) {
          return true;
        }
      }
      return false;
    }
  }
}
