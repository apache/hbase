package org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder;

import java.util.Optional;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatisticsImpl;

@InterfaceAudience.Private
public interface RowStatisticsRecorder {
  void record(RowStatisticsImpl stats, boolean isMajor, Optional<byte[]> fullRegionName);
}
