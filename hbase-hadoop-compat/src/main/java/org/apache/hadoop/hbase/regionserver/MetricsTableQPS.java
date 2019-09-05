package org.apache.hadoop.hbase.regionserver;

public interface MetricsTableQPS {

  String TABLE_READ_QPS = "tableReadQPS";
  String TABLE_WRITE_QPS = "tableWriteQPS";

  /**
   * Update table read QPS
   * @param tableName The table the metric is for
   * @param count Number of occurrences to record
   */
  void updateTableReadQPS(String tableName, long count);

  /**
   * Update table read QPS
   * @param tableName The table the metric is for
   */
  void updateTableReadQPS(String tableName);

  /**
   * Update table write QPS
   * @param tableName The table the metric is for
   * @param count Number of occurrences to record
   */
  void updateTableWriteQPS(String tableName, long count);

  /**
   * Update table write QPS
   * @param tableName The table the metric is for
   */
  void updateTableWriteQPS(String tableName);
}
