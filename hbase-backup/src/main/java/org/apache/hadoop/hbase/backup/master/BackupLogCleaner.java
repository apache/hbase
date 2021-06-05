/**
 *
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
package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for incremental backup
 * before deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupLogCleaner extends BaseLogCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(BackupLogCleaner.class);

  private boolean stopped = false;
  private Connection conn;
  private MasterServices master;
  private RSGroupInfoManager rsGroupInfoManager;
  private ServerManager serverManager;

  public BackupLogCleaner() {
  }

  @Override
  public void init(Map<String, Object> params) {
    MasterServices master = (MasterServices) MapUtils.getObject(params,
      HMaster.MASTER);
    if (master != null) {
      conn = master.getConnection();
      if (getConf() == null) {
        super.setConf(conn.getConfiguration());
      }
      this.master = master;
      rsGroupInfoManager = this.master.getRSGroupInfoManager();
      serverManager = this.master.getServerManager();
    }
    if (conn == null) {
      try {
        conn = ConnectionFactory.createConnection(getConf());
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create connection", ioe);
      }
    }
  }


  private Map<Address, Long> getServersToOldestBackupMapping(List<BackupInfo> backups)
    throws IOException {
    Map<Address, Long> serverAddressToLastBackupMap = new HashMap<>();

    Set<Address> allServers =
      serverManager.getOnlineServersList().stream().map(serverName -> serverName.getAddress())
        .collect(Collectors.toSet());
    for (BackupInfo backupInfo : backups) {
      for (TableName tableName : backupInfo.getTables()) {
        Optional<RSGroupInfo> rsGroupInfoOptional =
          RSGroupUtil.getRSGroupInfo(master, rsGroupInfoManager, tableName);

        Set<Address> servers;
        if (rsGroupInfoOptional.isPresent()) {
          servers = rsGroupInfoOptional.get().getServers();
          LOG.debug("RSgroup servers picked for table {} are {}", servers.size(), tableName);
        } else {
          servers = allServers;
          LOG.debug("All servers picked for table {} are {}", servers.size(), tableName);
        }



      }
    }

    return serverAddressToLastBackupMap;
  }

  private Map<Address, Long> getServersToOldestBackupMapping(
    Map<TableName, Long> tableToLastBackupMap) throws IOException {
    Map<Address, Long> addressToLastBackupMap = new HashMap<>();
    for (Map.Entry<TableName, Long> entry : tableToLastBackupMap.entrySet()) {
      TableName tableName = entry.getKey();
      Long lastTableBackupTimestamp = entry.getValue();

      Optional<RSGroupInfo> rsGroupInfoOptional =
        RSGroupUtil.getRSGroupInfo(master, rsGroupInfoManager, tableName);
      Set<Address> servers;
      if (rsGroupInfoOptional.isPresent()) {
        servers = rsGroupInfoOptional.get().getServers();
        LOG.debug("RSgroup servers picked for table {} are {}", servers.size(), tableName);
      } else {
        servers =
          serverManager.getOnlineServersList().stream().map(serverName -> serverName.getAddress())
            .collect(Collectors.toSet());
        LOG.debug("All servers picked for table {} are {}", servers.size(), tableName);
      }


      for (Address address : servers) {
        addressToLastBackupMap.putIfAbsent(address, lastTableBackupTimestamp);
        if (addressToLastBackupMap.get(address) > lastTableBackupTimestamp) {
          addressToLastBackupMap.put(address, lastTableBackupTimestamp);
          LOG.debug("Server {} backup for table {} with ts {}", address, tableName,
            lastTableBackupTimestamp);
        }
      }
    }

    return addressToLastBackupMap;
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    List<FileStatus> filteredFiles = new ArrayList<>();

    // all members of this class are null if backup is disabled,
    // so we cannot filter the files
    if (this.getConf() == null || !BackupManager.isBackupEnabled(getConf())) {
      LOG.debug("Backup is not enabled. Check your {} setting",
        BackupRestoreConstants.BACKUP_ENABLE_KEY);
      return files;
    }

    Map<Address, Long> addressToLastBackupMap;
    Map<TableName, Long> tableToLastBackupMap;
    try {
      try (BackupManager backupManager = new BackupManager(conn, getConf())) {
        tableToLastBackupMap =
          BackupUtils.getTableToLastStartCodeMapping(backupManager.getBackupHistory(true));
        addressToLastBackupMap = getServersToOldestBackupMapping(backupManager.getBackupHistory(true));
      }
    } catch (IOException ex) {
      LOG.error("Failed to analyse backup history with exception: {}. Retaining all logs",
        ex.getMessage(), ex);
      return Collections.emptyList();
    }
    for (FileStatus file : files) {
      String fn = file.getPath().getName();
      if (fn.startsWith(WALProcedureStore.LOG_PREFIX)) {
        filteredFiles.add(file);
        continue;
      }

      ServerName walServer =
        AbstractFSWALProvider.getServerNameFromWALDirectoryName(file.getPath());
      long walTimestamp = AbstractFSWALProvider.getTimestamp(file.getPath().toString());

      if (!addressToLastBackupMap.containsKey(walServer.getAddress())
        || addressToLastBackupMap.get(walServer.getAddress()) > walTimestamp) {
        filteredFiles.add(file);
      }
    }
    return filteredFiles;
  }

  @Override
  public void setConf(Configuration config) {
    // If backup is disabled, keep all members null
    super.setConf(config);
    if (!config.getBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY,
      BackupRestoreConstants.BACKUP_ENABLE_DEFAULT)) {
      LOG.warn("Backup is disabled - allowing all wals to be deleted");
    }
  }

  @Override
  public void stop(String why) {
    if (!this.stopped) {
      this.stopped = true;
      LOG.info("Stopping BackupLogCleaner");
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}
