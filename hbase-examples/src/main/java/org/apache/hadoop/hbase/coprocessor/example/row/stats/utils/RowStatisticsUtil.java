/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example.row.stats.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RowStatisticsUtil {

  public static Cell cloneWithoutValue(RawCellBuilder cellBuilder, Cell cell) {
    return cellBuilder.clear().setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
      .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
      .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
      .setTimestamp(cell.getTimestamp()).setType(cell.getType()).build();
  }

  public static boolean isInternalTable(RegionCoprocessorEnvironment environment) {
    return isInternalTable(environment.getRegionInfo().getTable());
  }

  public static boolean isInternalTable(TableName tableName) {
    return (!isDefaultNamespace(tableName.getNamespaceAsString())
      || isTestTable(tableName.getNameAsString()));
  }

  private static boolean isDefaultNamespace(String namespace) {
    return namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
  }

  private static boolean isTestTable(String table) {
    return (table.startsWith("hbase-test-table") || table.startsWith("sharded-hbase-test-table"));
  }
}
