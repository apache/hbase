package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotInfo.SnapshotStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to calculate the size of each region in a snapshot.
 */
public class SnapshotRegionSizeCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegionSizeCalculator.class);
  private final SnapshotManifest manifest;
  private final Configuration conf;
  private final Map<String, Long> regionSizes;


  public SnapshotRegionSizeCalculator(Configuration conf, SnapshotManifest manifest)
    throws IOException {
    this.conf = conf;
    this.manifest = manifest;
    this.regionSizes = calculateRegionSizes();
  }

  /**
   * Calculate the size of each region in the snapshot.
   * @return A map of region encoded names to their total size in bytes.
   */
  public Map<String, Long> calculateRegionSizes() throws IOException {
    SnapshotStats stats =  SnapshotInfo.getSnapshotStats(conf, manifest, null);
    return stats.getRegionSizeMap();
  }



  public long getRegionSize(String encodedRegionName)  {
    Long size = regionSizes.get(encodedRegionName);
    if (size == null) {
      LOG.debug("Unknown region:" + encodedRegionName);
      return 0;
    } else {
      return size;
    }
  }

}
