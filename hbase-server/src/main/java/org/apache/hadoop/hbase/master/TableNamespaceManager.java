/**
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

package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * This is a helper class used internally to manage the namespace metadata that is stored in the ns
 * family in meta table.
 */
@InterfaceAudience.Private
public class TableNamespaceManager {

  public static final String KEY_MAX_REGIONS = "hbase.namespace.quota.maxregions";
  public static final String KEY_MAX_TABLES = "hbase.namespace.quota.maxtables";
  static final String NS_INIT_TIMEOUT = "hbase.master.namespace.init.timeout";
  static final int DEFAULT_NS_INIT_TIMEOUT = 300000;

  private final ConcurrentMap<String, NamespaceDescriptor> cache = new ConcurrentHashMap<>();

  private final MasterServices masterServices;

  TableNamespaceManager(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  private void migrateNamespaceTable() throws IOException {
    try (Table nsTable = masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
      ResultScanner scanner = nsTable.getScanner(
        new Scan().addFamily(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES).readAllVersions());
      BufferedMutator mutator =
        masterServices.getConnection().getBufferedMutator(TableName.META_TABLE_NAME)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        Put put = new Put(result.getRow());
        result
          .getColumnCells(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES,
            TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES)
          .forEach(c -> put.addColumn(HConstants.NAMESPACE_FAMILY,
            HConstants.NAMESPACE_COL_DESC_QUALIFIER, c.getTimestamp(), CellUtil.cloneValue(c)));
        mutator.mutate(put);
      }
    }
    // schedule a disable procedure instead of block waiting here, as when disabling a table we will
    // wait until master is initialized, but we are part of the initialization...
    masterServices.getMasterProcedureExecutor().submitProcedure(
      new DisableTableProcedure(masterServices.getMasterProcedureExecutor().getEnvironment(),
        TableName.NAMESPACE_TABLE_NAME, false));
  }

  private void loadNamespaceIntoCache() throws IOException {
    try (Table table = masterServices.getConnection().getTable(TableName.META_TABLE_NAME);
      ResultScanner scanner = table.getScanner(HConstants.NAMESPACE_FAMILY)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        Cell cell = result.getColumnLatestCell(HConstants.NAMESPACE_FAMILY,
          HConstants.NAMESPACE_COL_DESC_QUALIFIER);
        NamespaceDescriptor ns = ProtobufUtil
          .toNamespaceDescriptor(HBaseProtos.NamespaceDescriptor.parseFrom(CodedInputStream
            .newInstance(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
        cache.put(ns.getName(), ns);
      }
    }
  }

  public void start() throws IOException {
    TableState nsTableState = MetaTableAccessor.getTableState(masterServices.getConnection(),
      TableName.NAMESPACE_TABLE_NAME);
    if (nsTableState != null && nsTableState.isEnabled()) {
      migrateNamespaceTable();
    }
    loadNamespaceIntoCache();
  }

  /**
   * check whether a namespace has already existed.
   */
  public boolean doesNamespaceExist(String namespaceName) throws IOException {
    return cache.containsKey(namespaceName);
  }

  public NamespaceDescriptor get(String name) throws IOException {
    return cache.get(name);
  }

  public void addOrUpdateNamespace(NamespaceDescriptor ns) throws IOException {
    insertNamespaceToMeta(masterServices.getConnection(), ns);
    cache.put(ns.getName(), ns);
  }

  public static void insertNamespaceToMeta(Connection conn, NamespaceDescriptor ns)
      throws IOException {
    byte[] row = Bytes.toBytes(ns.getName());
    Put put = new Put(row, true).addColumn(HConstants.NAMESPACE_FAMILY,
      HConstants.NAMESPACE_COL_DESC_QUALIFIER,
      ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray());
    try (Table table = conn.getTable(TableName.META_TABLE_NAME)) {
      table.put(put);
    }
  }

  public void deleteNamespace(String namespaceName) throws IOException {
    Delete d = new Delete(Bytes.toBytes(namespaceName));
    try (Table table = masterServices.getConnection().getTable(TableName.META_TABLE_NAME)) {
      table.delete(d);
    }
    cache.remove(namespaceName);
  }

  public List<NamespaceDescriptor> list() throws IOException {
    return cache.values().stream().collect(Collectors.toList());
  }

  public void validateTableAndRegionCount(NamespaceDescriptor desc) throws IOException {
    if (getMaxRegions(desc) <= 0) {
      throw new ConstraintException(
        "The max region quota for " + desc.getName() + " is less than or equal to zero.");
    }
    if (getMaxTables(desc) <= 0) {
      throw new ConstraintException(
        "The max tables quota for " + desc.getName() + " is less than or equal to zero.");
    }
  }

  public static long getMaxTables(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_TABLES);
    long maxTables = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxTables = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max tables.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxTables = Long.MAX_VALUE;
    }
    return maxTables;
  }

  public static long getMaxRegions(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_REGIONS);
    long maxRegions = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxRegions = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max regions.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxRegions = Long.MAX_VALUE;
    }
    return maxRegions;
  }
}
