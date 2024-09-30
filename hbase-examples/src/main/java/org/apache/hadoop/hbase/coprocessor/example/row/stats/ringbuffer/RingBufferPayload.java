package org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer;

import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatistics;

@InterfaceAudience.Private
public class RingBufferPayload {

  private final RowStatistics rowStatistics;
  private final boolean isMajor;
  private final byte[] fullRegionName;

  public RingBufferPayload(
    RowStatistics rowStatistics,
    boolean isMajor,
    byte[] fullRegionName
  ) {
    this.rowStatistics = rowStatistics;
    this.isMajor = isMajor;
    this.fullRegionName = fullRegionName;
  }

  public RowStatistics getRowStatistics() {
    return rowStatistics;
  }

  public boolean getIsMajor() {
    return isMajor;
  }

  public byte[] getFullRegionName() {
    return fullRegionName;
  }
}
