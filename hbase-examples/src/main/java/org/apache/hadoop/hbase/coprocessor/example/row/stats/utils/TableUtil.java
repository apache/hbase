package org.apache.hadoop.hbase.coprocessor.example.row.stats.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatistics;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class TableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TableUtil.class);
  public static final String NAMESPACE = "infra";
  public static final String TABLE_STRING = "row-statistics";
  public static final String VERSIONED_TABLE_STRING = TABLE_STRING + "-1";
  public static final TableName NAMESPACED_TABLE_NAME = TableName.valueOf(
    NAMESPACE,
    VERSIONED_TABLE_STRING
  );
  public static final byte[] CF = Bytes.toBytes("0");
  public static final String TABLE_RECORDER_KEY = "tableRecorder";

  public static Put buildPutForRegion(
    byte[] regionRowKey,
    RowStatistics rowStatistics,
    boolean isMajor
  ) {
    Put put = new Put(regionRowKey);
    String cq = rowStatistics.getColumnFamily() + (isMajor ? "1" : "0");
    String jsonString = rowStatistics.getJsonString();
    put.addColumn(CF, Bytes.toBytes(cq), Bytes.toBytes(jsonString));
    LOG.debug(jsonString);
    return put;
  }
}
