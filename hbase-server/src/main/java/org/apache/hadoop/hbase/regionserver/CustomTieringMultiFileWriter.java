package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

@InterfaceAudience.Private
public class CustomTieringMultiFileWriter extends DateTieredMultiFileWriter {

  public static final byte[] TIERING_CELL_TIME_RANGE =
    Bytes.toBytes("TIERING_CELL_TIME_RANGE");

  private NavigableMap<Long, TimeRangeTracker> lowerBoundary2TimeRanger = new TreeMap<>();

  public CustomTieringMultiFileWriter(List<Long> lowerBoundaries,
    Map<Long, String> lowerBoundariesPolicies, boolean needEmptyFile,
    Function<ExtendedCell, Long> tieringFunction) {
    super(lowerBoundaries, lowerBoundariesPolicies, needEmptyFile, tieringFunction);
    for (Long lowerBoundary : lowerBoundaries) {
      lowerBoundary2TimeRanger.put(lowerBoundary, null);
    }
  }

  @Override
  public void append(ExtendedCell cell) throws IOException {
    super.append(cell);
    long tieringValue = tieringFunction.apply(cell);
    Map.Entry<Long, TimeRangeTracker> entry =
      lowerBoundary2TimeRanger.floorEntry(tieringValue);
    if(entry.getValue()==null) {
      TimeRangeTracker timeRangeTracker = TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC);
      timeRangeTracker.setMin(tieringValue);
      timeRangeTracker.setMax(tieringValue);
      lowerBoundary2TimeRanger.put(entry.getKey(), timeRangeTracker);
      ((HFileWriterImpl)lowerBoundary2Writer.get(entry.getKey()).getLiveFileWriter())
        .setTimeRangeToTrack(()->timeRangeTracker);
    } else {
      TimeRangeTracker timeRangeTracker = entry.getValue();
      if(timeRangeTracker.getMin() > tieringValue) {
        timeRangeTracker.setMin(tieringValue);
      }
      if(timeRangeTracker.getMax() < tieringValue) {
        timeRangeTracker.setMax(tieringValue);
      }
    }
  }

  @Override
  public List<Path> commitWriters(long maxSeqId, boolean majorCompaction,
    Collection<HStoreFile> storeFiles) throws IOException {
    for(Map.Entry<Long, StoreFileWriter> entry : this.lowerBoundary2Writer.entrySet()){
      StoreFileWriter writer = entry.getValue();
      if(writer!=null) {
        writer.appendFileInfo(TIERING_CELL_TIME_RANGE,
          TimeRangeTracker.toByteArray(lowerBoundary2TimeRanger.get(entry.getKey())));
      }
    }
    return super.commitWriters(maxSeqId, majorCompaction, storeFiles);
  }

}
