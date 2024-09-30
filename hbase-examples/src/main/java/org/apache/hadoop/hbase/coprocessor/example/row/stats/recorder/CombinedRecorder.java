package org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder;

import java.util.Optional;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatisticsImpl;

@InterfaceAudience.Private
public class CombinedRecorder implements RowStatisticsRecorder {

  private final RowStatisticsRecorder one;
  private final RowStatisticsRecorder two;

  public CombinedRecorder(RowStatisticsRecorder one, RowStatisticsRecorder two) {
    this.one = one;
    this.two = two;
  }

  @Override
  public void record(
    RowStatisticsImpl stats,
    boolean isMajor,
    Optional<byte[]> fullRegionName
  ) {
    one.record(stats, isMajor, fullRegionName);
    two.record(stats, isMajor, fullRegionName);
  }

  public RowStatisticsRecorder getOne() {
    return one;
  }

  public RowStatisticsRecorder getTwo() {
    return two;
  }
}
