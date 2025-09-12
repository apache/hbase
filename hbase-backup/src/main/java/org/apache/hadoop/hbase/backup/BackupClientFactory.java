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
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.IncrementalBackupsDisallowedException;
import org.apache.hadoop.hbase.backup.impl.IncrementalTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class BackupClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BackupClientFactory.class);

  private BackupClientFactory() {
  }

  public static TableBackupClient create(Connection conn, String backupId, BackupRequest request)
    throws IOException {
    Configuration conf = conn.getConfiguration();
    try {
      String clsName = conf.get(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS);
      if (clsName != null) {
        Class<? extends TableBackupClient> clientImpl;
        clientImpl = Class.forName(clsName).asSubclass(TableBackupClient.class);
        TableBackupClient client = clientImpl.getDeclaredConstructor().newInstance();
        client.init(conn, backupId, request);
        return client;
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    BackupType type = request.getBackupType();
    if (type == BackupType.FULL) {
      return new FullTableBackupClient(conn, backupId, request);
    }

    String latestFullBackup = getLatestFullBackupId(conn, request);

    try (BackupAdmin admin = new BackupAdminImpl(conn)) {
      boolean disallowFurtherIncrementals =
        admin.getBackupInfo(latestFullBackup).isDisallowFurtherIncrementals();

      if (!disallowFurtherIncrementals) {
        return new IncrementalTableBackupClient(conn, backupId, request);
      }

      if (request.getFailOnDisallowedIncrementals()) {
        throw new IncrementalBackupsDisallowedException(request);
      }

      LOG.info("Incremental backups disallowed for backupId {}, creating a full backup",
        latestFullBackup);
      return new FullTableBackupClient(conn, backupId, request);
    }
  }

  private static String getLatestFullBackupId(Connection conn, BackupRequest request)
    throws IOException {
    try (BackupManager backupManager = new BackupManager(conn, conn.getConfiguration())) {
      // Sorted in desc order by time
      List<BackupInfo> backups = backupManager.getBackupHistory(true);

      for (BackupInfo info : backups) {
        if (
          info.getType() == BackupType.FULL
            && Objects.equals(info.getBackupRootDir(), request.getTargetRootDir())
        ) {
          return info.getBackupId();
        }
      }
    }
    throw new RuntimeException(
      "Could not find a valid full backup for incremental request for tables"
        + request.getTableList());
  }
}
