package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Manages the preferences for assigning regions to specific servers. Regions
 * may have persistent assignments to a list of servers ordered by preference,
 * as well as a transient assignment to a specific server. Transient
 * assignments may be a choice of one of the persistent assignments for the
 * present time, or may be for any arbitrary server. Transient assignments
 * will expire after a configurable delay, whereas persistent assignments will
 * remain until removed.
 *
 * Transient assignments should be respected, whereas persistent assignments
 * are suggestions which can be used to inform the creation of transient
 * assignments and do not need to be respected directly.
 *
 * All access to this class is thread-safe.
 */
public class PreferredAssignmentManager {
  protected static final Log LOG = LogFactory.getLog(
      PreferredAssignmentManager.class);

  /**
   * Preferred assignment map for temporary preferences. These preferences
   * are for the regions which should be assigned to a server as soon as
   * possible, and will expire if such assignments are not made within a
   * certain timeout period.
   */
  private Map<HServerAddress, Set<HRegionInfo>> transientAssignments;

  /**
   * Preferred assignment map for persistent preferences. These preferences
   * are multiple favored servers for each region, with the first server in
   * each list being preferred over all others (the primary).
   */
  private Map<HRegionInfo, List<HServerAddress>> persistentAssignments;

  /**
   * Queue of transient assignments and their upcoming timeouts. When a
   * transient assignment expires from this queue, it will be removed from
   * the transient assignment map.
   */
  private DelayQueue<PreferredAssignment> transientAssignmentTimeout;

  /**
   * This thread polls the timeout queue and removes any assignments which
   * have timed out.
   */
  private PreferredAssignmentHandler preferredAssignmentHandlerThread;

  /**
   * Set of all regions that have a transient preferred assignment, used for
   * quick lookup.
   */
  private Set<HRegionInfo> regionsWithTransientAssignment;

  private final HMaster master;

  public PreferredAssignmentManager(HMaster master) {
    this.master = master;
    this.transientAssignmentTimeout = new DelayQueue<PreferredAssignment>();
    this.preferredAssignmentHandlerThread = new PreferredAssignmentHandler();
    this.transientAssignments =
        new ConcurrentHashMap<HServerAddress, Set<HRegionInfo>>();
    this.persistentAssignments =
        new ConcurrentHashMap<HRegionInfo, List<HServerAddress>>();
    this.regionsWithTransientAssignment =
        new ConcurrentSkipListSet<HRegionInfo>();
  }

  public void start() {
    Threads.setDaemonThreadRunning(preferredAssignmentHandlerThread,
        "RegionManager.preferredAssignmentHandler");
  }

  public void addPersistentAssignment(HRegionInfo region,
      List<HServerAddress> servers) {
    List<HServerAddress> oldServers = persistentAssignments.get(region);
    if (servers != null && !servers.equals(oldServers)) {
      persistentAssignments.put(region, servers);
      if (LOG.isDebugEnabled()) {
        StringBuffer sb = new StringBuffer();
        for (HServerAddress server : servers) {
          sb.append(server.getHostname() + ":" + server.getPort() + ",");
          if (master.getServerManager().getHServerInfo(server) == null) {
            LOG.info("Found persistent assignment for region " +
                region.getRegionNameAsString() + " to unknown server " +
                server);
          }
        }
        LOG.debug("Added persistent assignment for region " +
            region.getRegionNameAsString() + " to " + sb.toString());

        // Reopen the region so that the server hosting it can pick up the
        // new list of favored nodes.
        ThrottledRegionReopener reopener = master.getRegionManager()
            .createThrottledReopener(region.getTableDesc().getNameAsString());
        reopener.addRegionToReopen(region);
        try {
          reopener.reOpenRegionsThrottle();
        } catch (IOException e) {
          LOG.info("Exception reopening region " +
              region.getRegionNameAsString() + " to pick up favored nodes");
        }
      }
    }
  }

  public List<HServerAddress> removePersistentAssignment(HRegionInfo region) {
    List<HServerAddress> servers = persistentAssignments.remove(region);
    if (LOG.isDebugEnabled() && servers != null) {
      StringBuffer sb = new StringBuffer();
      for (HServerAddress server : servers) {
        sb.append(server.getHostname() + ":" + server.getPort() + ",");
      }
      LOG.debug("Removed persistent assignment for region " +
          region.getRegionNameAsString() + " to " + sb.toString());
    }
    return servers;
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
            region.getRegionNameAsString() + " to " + server +
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
    }
    // Add to delay queue
    long millisecondDelay = master.getConfiguration().getLong(
        "hbase.regionserver.preferredAssignment.regionHoldPeriod", 60000);
    transientAssignmentTimeout.add(new PreferredAssignment(region, server,
        System.currentTimeMillis(), millisecondDelay));
  }

  /**
   * If the specified region has persistent preferred assignments and one of
   * those preferred servers is not dead, then choose the first of those to be
   * the transient preferred assignment for this region. Otherwise, no change
   * will be made to the region's transient assignment.
   * @param region get a transient assignment for this region
   */
  public void putTransientFromPersistent(HRegionInfo region) {
    List<HServerAddress> servers = persistentAssignments.get(region);
    if (servers != null) {
      for (HServerAddress server : servers) {
        HServerInfo info = master.getServerManager().getHServerInfo(server);
        // A preferred server is only eligible for assignment if the master
        // knows about the server's info, the server is not in the collection of
        // dead servers, and the server has load information. Absence of load
        // information may indicate that the server is in the process of
        // shutting down.
        if (info != null &&
            !master.getServerManager().isDead(info.getServerName()) &&
            master.getServerManager().getServersToLoad()
            .get(info.getServerName()) != null) {
          addTransientAssignment(server, region);
          return;
        }
      }
    }
  }

  public boolean removeTransientAssignment(HServerAddress server,
      HRegionInfo region) {
    synchronized (transientAssignments) {
      regionsWithTransientAssignment.remove(region);
      Set<HRegionInfo> regions = transientAssignments.get(server);
      if (regions != null) {
        regions.remove(region);
        if (regions.size() == 0) {
          transientAssignments.remove(server);
        }
        return true;
      }
      return false;
    }
  }

  public Set<HRegionInfo> getPreferredAssignments(HServerAddress server) {
    return transientAssignments.get(server);
  }

  public List<HServerAddress> getPreferredAssignments(HRegionInfo info) {
    return persistentAssignments.get(info);
  }

  public boolean hasPreferredAssignment(HServerAddress server) {
    return transientAssignments.containsKey(server);
  }

  public boolean hasTransientAssignment(HRegionInfo region) {
    return regionsWithTransientAssignment.contains(region);
  }

  public boolean hasPersistentAssignment(HRegionInfo region) {
    return persistentAssignments.containsKey(region);
  }

  private class PreferredAssignmentHandler extends Thread {
    public PreferredAssignmentHandler() {
    }

    @Override
    public void run() {
      LOG.debug("Started PreferredAssignmentHandler");
      PreferredAssignment plan = null;
      while (!master.getClosed().get()) {
        try {
          // check if any regions waiting time expired
          plan = transientAssignmentTimeout.poll(master.getConfiguration()
              .getInt(HConstants.THREAD_WAKE_FREQUENCY, 30 * 1000),
              TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // no problem, just continue
          continue;
        }
        if (null == plan) {
          continue;
        }
        if (removeTransientAssignment(plan.getServer(), plan.getRegionInfo())) {
          LOG.info("Removed region from preferred assignment: " +
              plan.getRegionInfo().getRegionNameAsString());
        }
      }
    }
  }

  private class PreferredAssignment implements Delayed {
    private long creationTime;
    private HRegionInfo region;
    private HServerAddress server;
    private long millisecondDelay;

    PreferredAssignment(HRegionInfo region, HServerAddress addr,
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
      if (o instanceof PreferredAssignment) {
        PreferredAssignment assignment = (PreferredAssignment)o;
        if (assignment.getServer().equals(this.getServer()) &&
            assignment.getRegionInfo().equals(this.getRegionInfo())) {
          return true;
        }
      }
      return false;
    }
  }
}
