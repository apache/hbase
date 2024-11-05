package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Custom implementation of DateTieredCompactionPolicy that calculates compaction boundaries based
 * on the <b>hbase.hstore.compaction.date.tiered.custom.age.limit.millis</b> configuration property
 * and the TIERING_CELL_MIN/TIERING_CELL_MAX stored on metadata of each store file.
 *
 * This policy would produce either one or two tiers:
 *  - One tier if either all files data age are older than the configured age limit or all files
 *  data age are younger than the configured age limit.
 *  - Two tiers if files have both younger and older data than the configured age limit.
 *
 */
@InterfaceAudience.Private
public class CustomCellDateTieredCompactionPolicy extends DateTieredCompactionPolicy {

  public static final String AGE_LIMIT_MILLIS =
    "hbase.hstore.compaction.date.tiered.custom.age.limit.millis";

  public static final String TIERING_CELL_MIN = "TIERING_CELL_MIN";

  public static final String TIERING_CELL_MAX = "TIERING_CELL_MAX";

  private long cutOffTimestamp;

  public CustomCellDateTieredCompactionPolicy(Configuration conf,
    StoreConfigInformation storeConfigInfo) throws IOException {
    super(conf, storeConfigInfo);
    cutOffTimestamp = EnvironmentEdgeManager.currentTime() -
      conf.getLong(AGE_LIMIT_MILLIS, (long) (10*365.25*24*60*60*1000));

  }

  @Override
  protected List<Long> getCompactBoundariesForMajor(Collection<HStoreFile> filesToCompact, long now) {
    MutableLong min = new MutableLong(Long.MAX_VALUE);
    MutableLong max = new MutableLong(0);
    filesToCompact.forEach(f -> {
        byte[] fileMin = f.getMetadataValue(Bytes.toBytes(TIERING_CELL_MIN));
        byte[] fileMax = f.getMetadataValue(Bytes.toBytes(TIERING_CELL_MAX));
        if (fileMin != null) {
          long minCurrent = Bytes.toLong(fileMin);
          if(min.getValue() < minCurrent) {
            min.setValue(minCurrent);
          }
        } else {
          min.setValue(0);
        }
        if (fileMax != null) {
          long maxCurrent = Bytes.toLong(fileMax);
          if(max.getValue() > maxCurrent) {
            max.setValue(maxCurrent);
          }
        } else {
          max.setValue(Long.MAX_VALUE);
        }
      });

    List<Long> boundaries = new ArrayList<>();
    if (min.getValue() < cutOffTimestamp) {
      boundaries.add(min.getValue());
      if (max.getValue() > cutOffTimestamp) {
        boundaries.add(cutOffTimestamp);
      }
    }
    boundaries.add(Long.MIN_VALUE);
    Collections.reverse(boundaries);
    return boundaries;
  }

}
