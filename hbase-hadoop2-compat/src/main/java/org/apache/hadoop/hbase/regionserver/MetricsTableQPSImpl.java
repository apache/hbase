package org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistry;

@InterfaceAudience.Private
public class MetricsTableQPSImpl implements MetricsTableQPS {

  private final HashMap<TableName,TableMeters> metersByTable = new HashMap<>();
  private final MetricRegistry metricRegistry;

  public MetricsTableQPSImpl(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public static class TableMeters {
    final Meter tableReadQPSMeter;
    final Meter tableWriteQPSMeter;

    TableMeters(MetricRegistry metricRegistry, TableName tableName) {
      this.tableReadQPSMeter = metricRegistry.meter(qualifyMetricsName(tableName, TABLE_READ_QPS));
      this.tableWriteQPSMeter =
        metricRegistry.meter(qualifyMetricsName(tableName, TABLE_WRITE_QPS));
    }

    public void updateTableReadQPS(long count) {
      tableReadQPSMeter.mark(count);
    }
    public void updateTableReadQPS() {
      tableReadQPSMeter.mark();
    }
    public void updateTableWriteQPS(long count) {
      tableWriteQPSMeter.mark(count);
    }
    public void updateTableWriteQPS() {
      tableWriteQPSMeter.mark();
    }
  }

  private static String qualifyMetricsName(TableName tableName, String metric) {
    StringBuilder sb = new StringBuilder();
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    sb.append("_metric_").append(metric);
    return sb.toString();
  }

  private TableMeters getOrCreateTableMeter(String tableName) {
    final TableName tn = TableName.valueOf(tableName);
    TableMeters meter = metersByTable.get(tn);
    if (meter == null) {
      meter = new TableMeters(metricRegistry, tn);
      metersByTable.put(tn, meter);
    }
    return meter;
  }

  @Override
  public void updateTableReadQPS(String tableName, long count) {
    getOrCreateTableMeter(tableName).updateTableReadQPS(count);
  }

  @Override
  public void updateTableReadQPS(String tableName) {
    getOrCreateTableMeter(tableName).updateTableReadQPS();
  }

  @Override
  public void updateTableWriteQPS(String tableName, long count) {
    getOrCreateTableMeter(tableName).updateTableWriteQPS(count);
  }

  @Override
  public void updateTableWriteQPS(String tableName) {
    getOrCreateTableMeter(tableName).updateTableWriteQPS();
  }
}
