package org.apache.hadoop.hbase.master.normalizer;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;



/**
 * Implementation of MergeNormalizer
 *
 * Logic in use:
 *
 *  <ol>
 *  <li> get all regions of a given table
 *  <li> get avg size S of each region (by total size of store files reported in RegionLoad)
 *  <li> Otherwise, two region R1 and its smallest neighbor R2 are merged,
 *    if R1 + R1 &lt;  S, and all such regions are returned to be merged
 *  <li> Otherwise, no action is performed
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Also,
 * empty regions (less than 1MB) are also merged if the age of region is &gt  MIN_DURATION_FOR_MERGE (default 2)
 */

@InterfaceAudience.Private
public class MergeNormalizer implements RegionNormalizer {
  private static final Log LOG = LogFactory.getLog(MergeNormalizer.class);
  private static final int MIN_REGION_COUNT = 3;
  private static final int MIN_DURATION_FOR_MERGE=2;
  private MasterServices masterServices;
  private MasterRpcServices masterRpcServices;

  @Override public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override public void setMasterRpcServices(MasterRpcServices masterRpcServices) {
    this.masterRpcServices = masterRpcServices;
  }

  @Override public List<NormalizationPlan> computePlanForTable(TableName table)
    throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table " + table + " isn't allowed");
      return null;
    }
    boolean mergeEnabled = true;
    try {
      mergeEnabled = masterRpcServices.isSplitOrMergeEnabled(null,
        RequestConverter.buildIsSplitOrMergeEnabledRequest(Admin.MasterSwitchType.MERGE)).getEnabled();
    } catch (ServiceException se) {
      LOG.debug("Unable to determine whether merge is enabled", se);
    }
    if (!mergeEnabled) {
      LOG.debug("Merge disabled for table: " + table);
      return null;
    }
    List<NormalizationPlan> plans = new ArrayList<NormalizationPlan>();
    List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(table);
    if (tableRegions == null || tableRegions.size() < MIN_REGION_COUNT) {
      int nrRegions = tableRegions == null ? 0 : tableRegions.size();
      LOG.debug("Table " + table + " has " + nrRegions + " regions, required min number"
        + " of regions for normalizer to run is " + MIN_REGION_COUNT + ", not running normalizer");
      return null;
    }

    LOG.debug("Computing normalization plan for table: " + table +
      ", number of regions: " + tableRegions.size());

    long totalSizeMb = 0;
    int acutalRegionCnt = 0;

    for (int i = 0; i < tableRegions.size(); i++) {
      HRegionInfo hri = tableRegions.get(i);
      long regionSize = getRegionSize(hri);
      //don't consider regions that are in bytes for averaging the size.
      if (regionSize > 0) {
        acutalRegionCnt++;
        totalSizeMb += regionSize;
      }
    }

    double avgRegionSize = acutalRegionCnt == 0 ? 0 : totalSizeMb / (double) acutalRegionCnt;

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
    LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

    int candidateIdx = 0;
    while (candidateIdx < tableRegions.size()) {
      HRegionInfo hri = tableRegions.get(candidateIdx);
      long regionSize = getRegionSize(hri);
      if (candidateIdx == tableRegions.size() - 1) {
        break;
      }
      HRegionInfo hri2 = tableRegions.get(candidateIdx + 1);
      long regionSize2 = getRegionSize(hri2);
      if (regionSize >= 0 && regionSize2 >= 0 && regionSize + regionSize2 < avgRegionSize) {
        Timestamp hriTime = new Timestamp(hri.getRegionId());
        Timestamp hri2Time = new Timestamp(hri2.getRegionId());
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        try {
          // atleast one of the two regions should be older than MIN_REGION_DURATION days
          if (!(new Timestamp(hriTime.getTime() + TimeUnit.DAYS.toMillis(MIN_DURATION_FOR_MERGE)))
              .after(currentTime)
              || !(new Timestamp(
                  hri2Time.getTime() + TimeUnit.DAYS.toMillis(MIN_DURATION_FOR_MERGE)))
                      .after(currentTime)) {
            LOG.info(
              "Table " + table + ", small region size: " + regionSize + " plus its neighbor size: "
                  + regionSize2 + ", less than the avg size " + avgRegionSize + ", merging them");
            plans.add(new MergeNormalizationPlan(hri, hri2));
            candidateIdx++;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      candidateIdx++;
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: " + table);
      return null;
    }
    return plans;
  }

  private long getRegionSize(HRegionInfo hri) {
    ServerName sn = masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(hri);
    RegionLoad regionLoad = masterServices.getServerManager().getLoad(sn).
      getRegionsLoad().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
  }
}
