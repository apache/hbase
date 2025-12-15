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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MigrateNamespaceTableProcedure;
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

  private volatile boolean migrationDone;

  TableNamespaceManager(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  private void tryMigrateNamespaceTable() throws IOException, InterruptedException {
    Optional<MigrateNamespaceTableProcedure> opt = masterServices.getProcedures().stream()
      .filter(p -> p instanceof MigrateNamespaceTableProcedure)
      .map(p -> (MigrateNamespaceTableProcedure) p).findAny();
    if (!opt.isPresent()) {
      // the procedure is not present, check whether have the ns family in meta table
      TableDescriptor metaTableDesc =
        masterServices.getTableDescriptors().get(MetaTableName.getInstance());
      if (metaTableDesc.hasColumnFamily(HConstants.NAMESPACE_FAMILY)) {
        // normal case, upgrading is done or the cluster is created with 3.x code
        migrationDone = true;
      } else {
        // submit the migration procedure
        MigrateNamespaceTableProcedure proc = new MigrateNamespaceTableProcedure();
        masterServices.getMasterProcedureExecutor().submitProcedure(proc);
      }
    } else {
      if (opt.get().isFinished()) {
        // the procedure is already done
        migrationDone = true;
      }
      // we have already submitted the procedure, continue
    }
  }

  private void addToCache(Result result, byte[] family, byte[] qualifier) throws IOException {
    Cell cell = result.getColumnLatestCell(family, qualifier);
    NamespaceDescriptor ns =
      ProtobufUtil.toNamespaceDescriptor(HBaseProtos.NamespaceDescriptor.parseFrom(CodedInputStream
        .newInstance(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
    cache.put(ns.getName(), ns);
  }

  private void loadFromMeta() throws IOException {
    try (Table table = masterServices.getConnection().getTable(MetaTableName.getInstance());
      ResultScanner scanner = table.getScanner(HConstants.NAMESPACE_FAMILY)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        addToCache(result, HConstants.NAMESPACE_FAMILY, HConstants.NAMESPACE_COL_DESC_QUALIFIER);
      }
    }
  }

  private void loadFromNamespace() throws IOException {
    try (Table table = masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
      ResultScanner scanner =
        table.getScanner(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES)) {
      for (Result result;;) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        addToCache(result, TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES,
          TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES);
      }
    }
  }

  private boolean shouldLoadFromMeta() throws IOException {
    if (migrationDone) {
      return true;
    }
    // the implementation is bit tricky
    // if there is already a disable namespace table procedure or the namespace table is already
    // disabled, we are safe to read from meta table as the migration is already done. If not, since
    // we are part of the master initialization work, so we can make sure that when reaching here,
    // the master has not been marked as initialize yet. And DisableTableProcedure can only be
    // executed after master is initialized, so here we are safe to read from namespace table,
    // without worrying about that the namespace table is disabled while we are reading and crash
    // the master startup.
    if (
      masterServices.getTableStateManager().isTableState(TableName.NAMESPACE_TABLE_NAME,
        TableState.State.DISABLED)
    ) {
      return true;
    }
    if (
      masterServices.getProcedures().stream().filter(p -> p instanceof DisableTableProcedure)
        .anyMatch(
          p -> ((DisableTableProcedure) p).getTableName().equals(TableName.NAMESPACE_TABLE_NAME))
    ) {
      return true;
    }
    return false;
  }

  private void loadNamespaceIntoCache() throws IOException {
    if (shouldLoadFromMeta()) {
      loadFromMeta();
    } else {
      loadFromNamespace();
    }

  }

  public void start() throws IOException, InterruptedException {
    tryMigrateNamespaceTable();
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

  private void checkMigrationDone() throws IOException {
    if (!migrationDone) {
      throw new HBaseIOException("namespace migration is ongoing, modification is disallowed");
    }
  }

  public void addOrUpdateNamespace(NamespaceDescriptor ns) throws IOException {
    checkMigrationDone();
    insertNamespaceToMeta(masterServices.getConnection(), ns);
    cache.put(ns.getName(), ns);
  }

  public static void insertNamespaceToMeta(Connection conn, NamespaceDescriptor ns)
    throws IOException {
    byte[] row = Bytes.toBytes(ns.getName());
    Put put = new Put(row, true).addColumn(HConstants.NAMESPACE_FAMILY,
      HConstants.NAMESPACE_COL_DESC_QUALIFIER,
      ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray());
    try (Table table = conn.getTable(MetaTableName.getInstance())) {
      table.put(put);
    }
  }

  public void deleteNamespace(String namespaceName) throws IOException {
    checkMigrationDone();
    Delete d = new Delete(Bytes.toBytes(namespaceName));
    try (Table table = masterServices.getConnection().getTable(MetaTableName.getInstance())) {
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

  public void setMigrationDone() {
    migrationDone = true;
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
