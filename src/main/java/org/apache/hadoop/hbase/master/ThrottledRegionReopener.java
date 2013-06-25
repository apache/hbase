package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;

public class ThrottledRegionReopener {
  protected static final Log LOG = LogFactory
      .getLog(ThrottledRegionReopener.class);
  private HMaster master;
  private RegionManager regionManager;
  private String tableName;
  private int totalNoOfRegionsToReopen = 0;
  private int regionCloseWaitInterval = 1000;
  private int numOfConcurrentClose = 5;

  private Closer closer;

  ThrottledRegionReopener(String tn, HMaster m, RegionManager regMgr,
                          int waitInterval, int maxConcurrentRegionsClosed) {
    this.tableName = tn;
    this.master = m;
    this.regionManager = regMgr;
    this.regionCloseWaitInterval = waitInterval;
    this.numOfConcurrentClose = maxConcurrentRegionsClosed;
    //Create a Daemon thread to close regions at the specified rate.
    this.closer = new Closer();
    this.closer.setDaemon(true);
  }

  /**
   * Set of regions that need to be processed for schema change. This is
   * required for throttling. Regions are added to this set when the .META. is
   * updated. Regions are held in this set until they can be closed. When a
   * region is closed it is removed from this set and added to the
   * alterTableReopeningRegions set.
   */
  private final Set<HRegionInfo> regionsToBeReopened = new HashSet<HRegionInfo>();

  /**
   * Set of regions that are currently being reopened by the master. Regions are
   * added to this set when their status is changed to "CLOSING" A region is
   * removed from this set when the master detects that the region has opened.
   */
  private final Set<HRegionInfo> regionsBeingReopened = new HashSet<HRegionInfo>();

  public synchronized void addRegionsToReopen(Set<HRegionInfo> regions) {
    regionsToBeReopened.addAll(regions);
    if (regionsToBeReopened.size() + regionsBeingReopened.size() == 0) {
      totalNoOfRegionsToReopen = regions.size();
    } else {
      totalNoOfRegionsToReopen += regions.size();
    }
  }

  public synchronized void addRegionToReopen(HRegionInfo region) {
    regionsToBeReopened.add(region);
    if (regionsToBeReopened.size() + regionsBeingReopened.size() == 0) {
      totalNoOfRegionsToReopen = 1;
    } else {
      totalNoOfRegionsToReopen += 1;
    }
  }

  /**
   * @return a pair of Integers (regions pending for reopen, total number of
   *         regions in the table).
   * @throws IOException
   */
  public synchronized Pair<Integer, Integer> getReopenStatus()
      throws IOException {
    int pending = regionsToBeReopened.size() + regionsBeingReopened.size();
    return new Pair<Integer, Integer>(pending, totalNoOfRegionsToReopen);
  }

  /**
   * When a reopening region changes state to OPEN, remove it from reopening
   * regions.
   *
   * @param region
   *
   */
  public synchronized void notifyRegionOpened(HRegionInfo region) {
    if (regionsBeingReopened.contains(region)) {
      LOG.info("Region reopened: " + region.getRegionNameAsString());
      regionsBeingReopened.remove(region);
    }
  }

  /**
   * Close a region to be reopened.
   *
   * @return True if a region was closed false otherwise.
   */
  private synchronized boolean doRegionClose() {
    for (Iterator<HRegionInfo> iter = regionsToBeReopened.iterator(); iter
        .hasNext();) {
      HRegionInfo region = iter.next();
      // Get the server name on which this is currently deployed
      String serverName = getRegionServerName(region);
      // Skip this region, process it when it has a non-null entry in META
      if (regionManager.regionIsInTransition(region.getRegionNameAsString())
          || serverName == null) {
        LOG.info("Skipping region in transition: "
            + region.getRegionNameAsString());
        continue;
      }

      LOG.debug("Closing region " + region.getRegionNameAsString());
      regionManager.setClosing(serverName, region, false);
      iter.remove(); // Remove from regionsToBeReopened
      regionsBeingReopened.add(region);

      return true;
    }
    return false;
  }

  /**
   * Get the name of the server serving this region from .META.
   *
   * @param region
   * @return serverName (host, port and startcode)
   */
  private String getRegionServerName(HRegionInfo region) {
    try {
      //TODO: is there a better way to do this?
      HTable metaTable = new HTable(this.master.getConfiguration(),
          HConstants.META_TABLE_NAME);

      Result result = metaTable.getRowOrBefore(region.getRegionName(),
          HConstants.CATALOG_FAMILY);
      HRegionInfo metaRegionInfo = this.master.getHRegionInfo(
          region.getRegionName(), result);
      if (metaRegionInfo.equals(region)) {
        String serverAddr = BaseScanner.getServerAddress(result);
        if (serverAddr != null && serverAddr.length() > 0) {
          long startCode = BaseScanner.getStartCode(result);
          String serverName = HServerInfo.getServerName(serverAddr, startCode);
          return serverName;
        }
      }
    } catch (IOException e) {
      LOG.warn("Could not get the server name for : " + region.getRegionNameAsString());
    }
    return null;
  }

  /**
   * Called when a region closes, if it is one of the reopening regions, add it
   * to preferred assignment
   *
   * @param region
   * @param serverInfo
   */
  public synchronized void addPreferredAssignmentForReopen(HRegionInfo region,
                                                           HServerInfo serverInfo) {
    if (regionsBeingReopened.contains(region) &&
        !master.isServerBlackListed(serverInfo.getHostnamePort())) {
      regionManager.getAssignmentManager().addTransientAssignment(
          serverInfo.getServerAddress(), region);
    }
  }

  /**
   * Called to start reopening regions of tableName
   *
   * @throws IOException
   */
  public void startRegionsReopening() throws IOException {
    if (HTable.isTableEnabled(master.getConfiguration(), tableName)) {
      LOG.info("Initiating reopen for all regions of " + tableName);
      closer.start();
    }
  }

  public int getRegionCloseWaitInterval() {
    return regionCloseWaitInterval;
  }

  public int getMaxConcurrentRegionsClosed() {
    return numOfConcurrentClose;
  }

  public synchronized int getNumRegionsCurrentlyClosed() {
    return regionsBeingReopened.size();
  }

  public synchronized int getNumRegionsLeftToReOpen() {
    return regionsToBeReopened.size();
  }

  private void failedToProgress() {
    LOG.error("Could not reopen regions of the table, "
        + tableName + ", retry the alter operation");
    regionManager.deleteThrottledReopener(tableName);
    this.closer.interrupt();
  }

  private void finished() {
    LOG.info("All regions of " + tableName + " reopened successfully.");
    regionManager.deleteThrottledReopener(tableName);
    this.closer.running.set(false);
  }

  private class Closer extends HasThread {

    private long timeAllowedToClose = EnvironmentEdgeManager.currentTimeMillis();
    private long closeInterval = 1000;
    private long timeOfLastRegionClose = 0;

    //Get progress timeout. Default is 5 min.
    private long progressTimeout = master.getConfiguration()
        .getLong("hbase.regionserver.alterTable.progressTimeout", 5 * 60 * 1000);

    private AtomicBoolean running = new AtomicBoolean(true);

    private void checkFinished() {
      if (getNumRegionsCurrentlyClosed() == 0
          && getNumRegionsLeftToReOpen() == 0) {
        running.set(false);
        finished();
      }
    }

    private void checkTimeout() {
      if (EnvironmentEdgeManager.currentTimeMillis() >=
          timeOfLastRegionClose + progressTimeout) {
        running.set(false);
        failedToProgress();
      }
    }

    @Override
    public void run() {

      closeInterval = Math.max(getRegionCloseWaitInterval() /
          getMaxConcurrentRegionsClosed(), 1);

      LOG.debug("Closer started for ThrottledRegionReopener of table " + tableName);

      while (running.get()) {
        try {
          long sleepTime = EnvironmentEdgeManager.currentTimeMillis() - timeAllowedToClose;
          if (sleepTime > 0) {
            LOG.debug("Closer running and about to wait. Currently Closed: " +
                getNumRegionsCurrentlyClosed() + "  Left to Close: " +
                getNumRegionsLeftToReOpen());
            sleep(sleepTime);
          }

          //Not allowed to try yet
          if (EnvironmentEdgeManager.currentTimeMillis() < timeAllowedToClose) {
            continue;
          }

          //Max concurrent regions already closed
          if (getNumRegionsCurrentlyClosed() >= getMaxConcurrentRegionsClosed()) {
            checkTimeout();
            continue;
          }

          //Looks like we can close a region now
          if (doRegionClose()) {
            //We closed a region so update the time we can try again.
            timeAllowedToClose = EnvironmentEdgeManager.currentTimeMillis() + closeInterval;
            timeOfLastRegionClose = EnvironmentEdgeManager.currentTimeMillis();
          }

          //Are we done?
          checkFinished();

          //Did we timeout before making any progress?
          checkTimeout();

        } catch (InterruptedException e) {
          LOG.warn("Closer of the ThrottledRegionReopener for table: " + tableName +
              " was interrupted. There are " + getNumRegionsLeftToReOpen() + " regions left to reopen.");
          running.set(false);
        }
      }

      LOG.debug("Closer for ThrottledRegionReopener of table " + tableName + " is stopping.");
    }
  }
}
