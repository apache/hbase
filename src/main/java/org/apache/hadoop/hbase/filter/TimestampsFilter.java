package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

import com.google.common.base.Preconditions;

/**
 * Filter that returns only cells whose timestamp (version) is
 * in the specified list of timestamps (versions).
 * <p>
 * Note: Use of this filter overrides any time range/time stamp
 * options specified using {@link Get#setTimeRange(long, long)},
 * {@link Scan#setTimeRange(long, long)}, {@link Get#setTimeStamp(long)},
 * or {@link Scan#setTimeStamp(long)}.
 */
public class TimestampsFilter extends FilterBase {

  TreeSet<Long> timestamps;
  private static final int MAX_LOG_TIMESTAMPS = 5;

  // Used during scans to hint the scan to stop early
  // once the timestamps fall below the minTimeStamp.
  long minTimeStamp = Long.MAX_VALUE;

  /**
   * Used during deserialization. Do not use otherwise.
   */
  public TimestampsFilter() {
    super();
  }

  /**
   * Constructor for filter that retains only those
   * cells whose timestamp (version) is in the specified
   * list of timestamps.
   *
   * @param timestamps
   */
  public TimestampsFilter(List<Long> timestamps) {
    for (Long timestamp : timestamps) {
      Preconditions.checkArgument(timestamp >= 0, "must be positive %s", timestamp);
    }
    this.timestamps = new TreeSet<Long>(timestamps);
    init();
  }

  private void init() {
    if (this.timestamps.size() > 0) {
      minTimeStamp = this.timestamps.first();
    }
  }

  /**
   * Gets the minimum timestamp requested by filter.
   * @return  minimum timestamp requested by filter.
   */
  public long getMin() {
    return minTimeStamp;
  }

  public ReturnCode filterKeyValue(KeyValue v,
      List<KeyValueScanner> scanners) {
    if (this.timestamps.contains(v.getTimestamp())) {
      return ReturnCode.INCLUDE;
    } else if (v.getTimestamp() < minTimeStamp) {
      // The remaining versions of this column are guaranteed
      // to be lesser than all of the other values.
      return ReturnCode.NEXT_COL;
    }
    // Skip current KeyValue.
    // It may be incorrect to return ReturnCode.SEEK_NEXT_USING_HINT, as it could
    // skip past some DeleteColumn KV's to cause incorrect behavior. However, not
    // using ReturnCode.SEEK_NEXT_USING_HINT has a perf penalty. So, we let the
    // configuration specify decide what the application cares about.
    return (HRegionServer.useSeekNextUsingHint? ReturnCode.SEEK_NEXT_USING_HINT: ReturnCode.SKIP);
  }

  public TreeSet<Long> getTimestamps() {
    return this.timestamps;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    ArrayList<Long> timestamps = new ArrayList<Long>();
    for (int i = 0; i<filterArguments.size(); i++) {
      long timestamp = ParseFilter.convertByteArrayToLong(filterArguments.get(i));
      timestamps.add(timestamp);
    }
    return new TimestampsFilter(timestamps);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numTimestamps = in.readInt();
    this.timestamps = new TreeSet<Long>();
    for (int idx = 0; idx < numTimestamps; idx++) {
      this.timestamps.add(in.readLong());
    }
    init();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int numTimestamps = this.timestamps.size();
    out.writeInt(numTimestamps);
    for (Long timestamp : this.timestamps) {
      out.writeLong(timestamp);
    }
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    Long nextTimestampObject = timestamps.lower(kv.getTimestamp());
    long nextTimestamp = nextTimestampObject != null ? nextTimestampObject : 0;
    return KeyValue.createFirstOnRow(kv.getBuffer(), kv.getRowOffset(), kv
        .getRowLength(), kv.getBuffer(), kv.getFamilyOffset(), kv
        .getFamilyLength(), kv.getBuffer(), kv.getQualifierOffset(), kv
        .getQualifierLength(), nextTimestamp);
  }

  @Override
  public String toString() {
    return toString(MAX_LOG_TIMESTAMPS);
  }

  protected String toString(int maxTimestamps) {
    StringBuilder tsList = new StringBuilder();

    int count = 0;
    for (Long ts : this.timestamps) {
      if (count >= maxTimestamps) {
        break;
      }
      ++count;
      tsList.append(ts.toString());
      if (count < this.timestamps.size() && count < maxTimestamps) {
        tsList.append(", ");
      }
    }

    return String.format("%s (%d/%d): [%s]", this.getClass().getSimpleName(),
        count, this.timestamps.size(), tsList.toString());
  }
}
