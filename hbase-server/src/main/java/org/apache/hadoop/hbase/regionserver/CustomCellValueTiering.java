package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.TIERING_CELL_TIME_RANGE;

@InterfaceAudience.Private
public class CustomCellValueTiering implements DataTiering {
  private static final Logger LOG = LoggerFactory.getLogger(CustomCellValueTiering.class);
  private long getMaxTSFromTimeRange(byte[] hFileTimeRange, String hFileName) {
    try {
      if (hFileTimeRange == null) {
        LOG.debug("Custom cell-based timestamp information not found for file: {}", hFileName);
        return Long.MAX_VALUE;
      }
      return TimeRangeTracker.parseFrom(hFileTimeRange).getMax();
    } catch (IOException e) {
      LOG.error("Error occurred while reading the Custom cell-based timestamp metadata of file: {}",
        hFileName, e);
      return Long.MAX_VALUE;
    }
  }
  public long getTimestamp(HStoreFile hStoreFile) {
    return getMaxTSFromTimeRange(hStoreFile.getMetadataValue(TIERING_CELL_TIME_RANGE),
      hStoreFile.getPath().getName());
  }
  public long getTimestamp(HFileInfo hFileInfo) {
      return getMaxTSFromTimeRange(hFileInfo.get(TIERING_CELL_TIME_RANGE),
        hFileInfo.getHFileContext().getHFileName());
  }
}
