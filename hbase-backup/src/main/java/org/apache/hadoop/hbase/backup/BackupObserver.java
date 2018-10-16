/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Observer to facilitate backup operations
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupObserver implements RegionCoprocessor, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(BackupObserver.class);

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
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
    try (Connection connection = ConnectionFactory.createConnection(cfg);
        BackupSystemTable tbl = new BackupSystemTable(connection)) {
      List<TableName> fullyBackedUpTables = tbl.getTablesForBackupType(BackupType.FULL);
      RegionInfo info = ctx.getEnvironment().getRegionInfo();
      TableName tableName = info.getTable();
      if (!fullyBackedUpTables.contains(tableName)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(tableName + " has not gone thru full backup");
        }
        return;
      }
      tbl.writePathsPostBulkLoad(tableName, info.getEncodedNameAsBytes(), finalPaths);
    } catch (IOException ioe) {
      LOG.error("Failed to get tables which have been fully backed up", ioe);
    }
  }
  @Override
  public void preCommitStoreFile(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final byte[] family, final List<Pair<Path, Path>> pairs) throws IOException {
    Configuration cfg = ctx.getEnvironment().getConfiguration();
    if (pairs == null || pairs.isEmpty() || !BackupManager.isBackupEnabled(cfg)) {
      LOG.debug("skipping recording bulk load in preCommitStoreFile since backup is disabled");
      return;
    }
    try (Connection connection = ConnectionFactory.createConnection(cfg);
        BackupSystemTable tbl = new BackupSystemTable(connection)) {
      List<TableName> fullyBackedUpTables = tbl.getTablesForBackupType(BackupType.FULL);
      RegionInfo info = ctx.getEnvironment().getRegionInfo();
      TableName tableName = info.getTable();
      if (!fullyBackedUpTables.contains(tableName)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(tableName + " has not gone thru full backup");
        }
        return;
      }
      tbl.writeFilesForBulkLoadPreCommit(tableName, info.getEncodedNameAsBytes(), family, pairs);
      return;
    }
  }
}
