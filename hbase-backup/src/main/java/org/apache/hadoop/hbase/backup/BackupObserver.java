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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.BulkLoad;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * An Observer to facilitate backup operations
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupObserver
  implements RegionCoprocessor, RegionObserver, MasterCoprocessor, MasterObserver {
  private static final Logger LOG = LoggerFactory.getLogger(BackupObserver.class);

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void postBulkLoadHFile(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    List<Pair<byte[], String>> stagingFamilyPaths, Map<byte[], List<Path>> finalPaths)
    throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    if (finalPaths == null) {
      // there is no need to record state
      return;
    }
    if (!BackupManager.isBackupEnabled(cfg)) {
      LOG.debug("skipping recording bulk load in postBulkLoadHFile since backup is disabled");
      return;
    }

    registerBulkLoad(ctx, finalPaths);
  }

  @Override
  public void preCommitStoreFile(final ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    final byte[] family, final List<Pair<Path, Path>> pairs) throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    if (pairs == null || pairs.isEmpty() || !BackupManager.isBackupEnabled(cfg)) {
      LOG.debug("skipping recording bulk load in preCommitStoreFile since backup is disabled");
      return;
    }

    List<Path> hfiles = new ArrayList<>(pairs.size());
    for (Pair<Path, Path> pair : pairs) {
      hfiles.add(pair.getSecond());
    }
    registerBulkLoad(ctx, Collections.singletonMap(family, hfiles));
  }

  private void registerBulkLoad(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Map<byte[], List<Path>> cfToHFilePaths) throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    RegionInfo region = ctx.getEnvironment().getRegionInfo();
    TableName tableName = region.getTable();

    try (Connection connection = ConnectionFactory.createConnection(cfg);
      BackupSystemTable tbl = new BackupSystemTable(connection)) {
      Set<TableName> fullyBackedUpTables = tbl.getTablesIncludedInBackups();

      if (fullyBackedUpTables.contains(tableName)) {
        tbl.registerBulkLoad(tableName, region.getEncodedNameAsBytes(), cfToHFilePaths);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Table {} has not gone through full backup - skipping.", tableName);
        }
      }
    }
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    if (!BackupManager.isBackupEnabled(cfg)) {
      LOG.debug("Skipping postDeleteTable hook since backup is disabled");
      return;
    }
    deleteBulkLoads(cfg, tableName, (ignored) -> true);
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    if (!BackupManager.isBackupEnabled(cfg)) {
      LOG.debug("Skipping postTruncateTable hook since backup is disabled");
      return;
    }
    deleteBulkLoads(cfg, tableName, (ignored) -> true);
  }

  @Override
  public void postModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final TableName tableName, TableDescriptor oldDescriptor, TableDescriptor currentDescriptor)
    throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    if (!BackupManager.isBackupEnabled(cfg)) {
      LOG.debug("Skipping postModifyTable hook since backup is disabled");
      return;
    }

    Set<String> oldFamilies = Arrays.stream(oldDescriptor.getColumnFamilies())
      .map(ColumnFamilyDescriptor::getNameAsString).collect(Collectors.toSet());
    Set<String> newFamilies = Arrays.stream(currentDescriptor.getColumnFamilies())
      .map(ColumnFamilyDescriptor::getNameAsString).collect(Collectors.toSet());

    Set<String> removedFamilies = Sets.difference(oldFamilies, newFamilies);
    if (!removedFamilies.isEmpty()) {
      Predicate<BulkLoad> filter = bulkload -> removedFamilies.contains(bulkload.getColumnFamily());
      deleteBulkLoads(cfg, tableName, filter);
    }
  }

  /**
   * Deletes all bulk load entries for the given table, matching the provided predicate.
   */
  private void deleteBulkLoads(Configuration config, TableName tableName,
    Predicate<BulkLoad> filter) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(config);
      BackupSystemTable tbl = new BackupSystemTable(connection)) {
      List<BulkLoad> bulkLoads = tbl.readBulkloadRows(List.of(tableName));
      List<byte[]> rowsToDelete =
        bulkLoads.stream().filter(filter).map(BulkLoad::getRowKey).toList();
      tbl.deleteBulkLoadedRows(rowsToDelete);
    }
  }
}
