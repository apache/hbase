package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;

public class ThrottledRegionReopener {
  protected static final Log LOG = LogFactory
      .getLog(ThrottledRegionReopener.class);
  private HMaster master;
  private RegionManager regionManager;
  private String tableName;
  private int totalNoOfRegionsToReopen = 0;
  private int regionCloseWaitInterval = 1000;
  private long timeAllowedToProceed = 0;
  private int numOfConcurrentClose = 0;

  ThrottledRegionReopener(String tn, HMaster m, RegionManager regMgr) {
    this.tableName = tn;
    this.master = m;
    this.regionManager = regMgr;
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

      // Check if all the regions have reopened and log.
      if (closeSomeRegions() == 0) {
        if (regionsToBeReopened.size() == 0 && regionsBeingReopened.size() == 0) {
          this.numOfConcurrentClose = 0;
          LOG.info("All regions of " + tableName + " reopened successfully.");
        } else {
          LOG.error("All regions of " + tableName
              + " could not be reopened. Retry the operation.");
        }
        regionManager.deleteThrottledReopener(tableName);
      }
    }
  }

  /**
   * Close some of the reopening regions. Used for throttling the percentage of
   * regions of a table that may be reopened concurrently. This is configurable
   * by a hbase config parameter hbase.regionserver.alterTable.concurrentReopen
   * which defines the percentage of regions of a table that the master may
   * reopen concurrently (defaults to 1).
   *
   */
  public synchronized int closeSomeRegions() {

    float localNumOfConcurrentClose = this.numOfConcurrentClose;

    //Try to get the number from the config if class value is set to 0
    if (this.numOfConcurrentClose == 0) {
      float percentConcurrentClose = this.master.getConfiguration().getFloat(
          "hbase.regionserver.alterTable.concurrentReopen", 5);
      // Find the number of regions you are allowed to close concurrently
      float configNumOfConcurrentClose = (percentConcurrentClose / 100)
          * totalNoOfRegionsToReopen;
      // Close at least one region at a time
      if (configNumOfConcurrentClose < 1 && configNumOfConcurrentClose > 0) {
        configNumOfConcurrentClose = 1;
      }

      localNumOfConcurrentClose = configNumOfConcurrentClose;
    }

    localNumOfConcurrentClose -= regionsBeingReopened.size();
    if (localNumOfConcurrentClose <= 0) {
      return 0;
    }

    //If true we are not yet at the time when we can proceed
    if ( this.timeAllowedToProceed > System.currentTimeMillis()) {
      //Wait until we are allowed to try and close more regions
      try {
        long sleepLength = this.timeAllowedToProceed - System.currentTimeMillis();
        Thread.sleep(sleepLength);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    int cnt = 0;
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
      cnt++;

      // Close allowed number of regions, exit
      if (cnt == localNumOfConcurrentClose) {
        break;
      }
    }

    //If we closed some regions set the time we are allowed to close more regions
    if (cnt > 0) {
      this.timeAllowedToProceed = System.currentTimeMillis() + regionCloseWaitInterval;
    }

    return cnt;
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
    if (regionsBeingReopened.contains(region)) {
      regionManager.getAssignmentManager().addTransientAssignment(
          serverInfo.getServerAddress(), region);
    }
  }

  /**
   * Called to start reopening regions of tableName
   *
   * @throws IOException
   */
  public synchronized void reOpenRegionsThrottle() throws IOException {
    if (HTable.isTableEnabled(master.getConfiguration(), tableName)) {
      LOG.info("Initiating reopen for all regions of " + tableName);
      if (closeSomeRegions() == 0) {
        regionManager.deleteThrottledReopener(tableName);
        throw new IOException("Could not reopen regions of the table, "
            + "retry the alter operation");
      }
    }
  }

  public int getRegionCloseWaitInterval() {
    return regionCloseWaitInterval;
  }

  public int getNumConcurrentCloseRegions() {
    return numOfConcurrentClose;
  }

  public synchronized void setRegionCloseWaitInterval(int regionCloseWaitInterval) {
    LOG.info("ThrottledRegionReopener: Setting wait interval " + regionCloseWaitInterval + " !!!");
    //Wait interval is in milliseconds
    this.regionCloseWaitInterval = regionCloseWaitInterval;
  }

  public synchronized void setNumConcurrentCloseRegions(int numConcurrentClose) {
    this.numOfConcurrentClose = numConcurrentClose;
  }
}
