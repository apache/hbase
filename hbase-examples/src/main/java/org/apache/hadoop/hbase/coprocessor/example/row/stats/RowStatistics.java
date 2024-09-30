package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import java.util.Map;

@InterfaceAudience.Private
public interface RowStatistics {
  String getTable();

  String getRegion();

  String getColumnFamily();

  long getLargestRowBytes();

  int getLargestRowCells();

  long getLargestCellBytes();

  int getCellsLargerThanOneBlock();

  int getRowsLargerThanOneBlock();

  int getCellsLargerThanMaxCacheSize();

  int getTotalDeletes();

  int getTotalCells();

  int getTotalRows();

  long getTotalBytes();

  String getLargestRowAsString();

  String getLargestCellAsString();

  Map<String, Long> getRowSizeBuckets();

  Map<String, Long> getValueSizeBuckets();

  String getJsonString();
}
