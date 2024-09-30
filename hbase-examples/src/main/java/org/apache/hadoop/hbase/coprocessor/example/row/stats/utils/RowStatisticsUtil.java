package org.apache.hadoop.hbase.coprocessor.example.row.stats.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

@InterfaceAudience.Private
public class RowStatisticsUtil {

  public static Cell cloneWithoutValue(RawCellBuilder cellBuilder, Cell cell) {
    return cellBuilder
      .clear()
      .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
      .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
      .setQualifier(
        cell.getQualifierArray(),
        cell.getQualifierOffset(),
        cell.getQualifierLength()
      )
      .setTimestamp(cell.getTimestamp())
      .setType(cell.getType())
      .build();
  }

  public static boolean isInternalTable(RegionCoprocessorEnvironment environment) {
    return isInternalTable(environment.getRegionInfo().getTable());
  }

  public static boolean isInternalTable(TableName tableName) {
    return (
      !isDefaultNamespace(tableName.getNamespaceAsString()) ||
      isTestTable(tableName.getNameAsString())
    );
  }

  private static boolean isDefaultNamespace(String namespace) {
    return namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
  }

  private static boolean isTestTable(String table) {
    return (
      table.startsWith("hbase-test-table") || table.startsWith("sharded-hbase-test-table")
    );
  }
}
