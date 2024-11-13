package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.OptionalLong;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.TIMERANGE_KEY;

@InterfaceAudience.Private
public class CellTSTiering implements DataTiering {
  private static final Logger LOG = LoggerFactory.getLogger(CellTSTiering.class);
  public long getTimestamp(HStoreFile hStoreFile) {
    OptionalLong maxTimestamp = hStoreFile.getMaximumTimestamp();
    if (!maxTimestamp.isPresent()) {
      LOG.info("Maximum timestamp not present for {}", hStoreFile.getPath());
      return Long.MAX_VALUE;
    }
    return maxTimestamp.getAsLong();
  }
  public long getTimestamp(HFileInfo hFileInfo) {
    try {
      byte[] hFileTimeRange = hFileInfo.get(TIMERANGE_KEY);
      if (hFileTimeRange == null) {
        LOG.info("Timestamp information not found for file: {}",
          hFileInfo.getHFileContext().getHFileName());
        return Long.MAX_VALUE;
      }
      return TimeRangeTracker.parseFrom(hFileTimeRange).getMax();
    } catch (IOException e) {
      LOG.error("Error occurred while reading the timestamp metadata of file: {}",
        hFileInfo.getHFileContext().getHFileName(), e);
      return Long.MAX_VALUE;
    }
  }
}
