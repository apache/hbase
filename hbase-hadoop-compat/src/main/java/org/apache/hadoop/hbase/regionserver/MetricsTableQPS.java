package org.apache.hadoop.hbase.regionserver;

public interface MetricsTableQPS {

  String TABLE_READ_QPS = "tableReadQPS";
  String TABLE_WRITE_QPS = "tableWriteQPS";

  /**
   *
   * @param tableName
   * @param count
   */
  void updateTableReadQPS(String tableName, long count);

  /**
   *
   * @param tableName
   */
  void updateTableReadQPS(String tableName);

  /**
   *
   * @param tableName
   * @param count
   */
  void updateTableWriteQPS(String tableName, long count);

  /**
   *
   * @param tableName
   */
  void updateTableWriteQPS(String tableName);
}
