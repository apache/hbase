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
package org.apache.hadoop.hbase.backup.replication;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationResult;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ContinuousBackupReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG =
    LoggerFactory.getLogger(ContinuousBackupReplicationEndpoint.class);
  public static final String CONF_PEER_UUID = "hbase.backup.wal.replication.peerUUID";
  public static final String WAL_FILE_PREFIX = "wal_file.";
  public static final String CONF_BACKUP_ROOT_DIR = "hbase.backup.root.dir";
  public static final String CONF_BACKUP_MAX_WAL_SIZE = "hbase.backup.max.wal.size";
  public static final long DEFAULT_MAX_WAL_SIZE = 128 * 1024 * 1024;

  private Configuration conf;
  private BackupFileSystemManager backupFileSystemManager;
  private FSHLogProvider.Writer walWriter;
  private UUID peerUUID;
  private String peerId;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    LOG.info("{} Initializing ContinuousBackupReplicationEndpoint.",
      Utils.logPeerId(ctx.getPeerId()));

    this.peerId = this.ctx.getPeerId();

    Configuration peerConf = this.ctx.getConfiguration();

    setPeerUUID(peerConf);

    this.conf = HBaseConfiguration.create(peerConf);

    String backupRootDirStr = conf.get(CONF_BACKUP_ROOT_DIR);
    if (backupRootDirStr == null || backupRootDirStr.isEmpty()) {
      String errorMsg = Utils.logPeerId(peerId)
        + " Backup root directory not specified. Set it using " + CONF_BACKUP_ROOT_DIR;
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
    LOG.debug("{} Backup root directory: {}", Utils.logPeerId(peerId), backupRootDirStr);

    try {
      this.backupFileSystemManager = new BackupFileSystemManager(peerId, conf, backupRootDirStr);
      LOG.info("{} BackupFileSystemManager initialized successfully.", Utils.logPeerId(peerId));
    } catch (IOException e) {
      String errorMsg = Utils.logPeerId(peerId) + " Failed to initialize BackupFileSystemManager";
      LOG.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }

    walWriter = null;
  }

  @Override
  public UUID getPeerUUID() {
    return peerUUID;
  }

  @Override
  public void start() {
    LOG.info("{} Starting ContinuousBackupReplicationEndpoint...",
      Utils.logPeerId(ctx.getPeerId()));
    startAsync();
  }

  @Override
  protected void doStart() {
    LOG.info("{} ContinuousBackupReplicationEndpoint started successfully.",
      Utils.logPeerId(ctx.getPeerId()));
    notifyStarted();
  }

  @Override
  public ReplicationResult replicate(ReplicateContext replicateContext) {
    final List<WAL.Entry> entries = replicateContext.getEntries();
    if (entries.isEmpty()) {
      LOG.debug("{} No WAL entries to backup.", Utils.logPeerId(ctx.getPeerId()));
      return ReplicationResult.SUBMITTED;
    }

    try {
      List<Path> bulkLoadFiles = BulkLoadProcessor.processBulkLoadFiles(entries);
      return backupEntries(entries, bulkLoadFiles);
    } catch (IOException e) {
      LOG.error("{} Backup failed. Error details: {}", Utils.logPeerId(peerId), e.getMessage(), e);
      return ReplicationResult.FAILED;
    }
  }

  private ReplicationResult backupEntries(List<WAL.Entry> walEntries, List<Path> bulkLoadFiles)
    throws IOException {
    if (walWriter == null) {
      walWriter = createWalWriter();
    }

    for (WAL.Entry entry : walEntries) {
      walWriter.append(entry);
    }
    walWriter.sync(true);
    uploadBulkLoadFiles(bulkLoadFiles);

    if (isWriterFull(walWriter)) {
      walWriter.close();
      walWriter = null;
      return ReplicationResult.COMMITTED;
    }

    return ReplicationResult.SUBMITTED;
  }

  private boolean isWriterFull(FSHLogProvider.Writer writer) {
    long maxWalSize = conf.getLong(CONF_BACKUP_MAX_WAL_SIZE, DEFAULT_MAX_WAL_SIZE);
    return writer.getLength() >= maxWalSize;
  }

  private FSHLogProvider.Writer createWalWriter() throws IOException {
    FileSystem fs = backupFileSystemManager.getBackupFs();
    Path walsDir = backupFileSystemManager.getWalsDir();
    String walFileName = WAL_FILE_PREFIX + EnvironmentEdgeManager.getDelegate().currentTime();
    Path walFilePath = new Path(walsDir, walFileName);

    try {
      FSHLogProvider.Writer writer =
        ObjectStoreProtobufWalWriter.class.getDeclaredConstructor().newInstance();
      writer.init(fs, walFilePath, conf, true, WALUtil.getWALBlockSize(conf, fs, walFilePath),
        StreamSlowMonitor.create(conf, walFileName));
      return writer;
    } catch (Exception e) {
      throw new IOException("Cannot initialize WAL Writer", e);
    }
  }

  public void close() {
    if (walWriter != null) {
      try {
        walWriter.close();
        LOG.info("{} Closed WAL writer", Utils.logPeerId(peerId));
      } catch (IOException e) {
        LOG.error("{} Failed to close WAL writer", Utils.logPeerId(peerId), e);
      }
    }
  }

  @Override
  public void stop() {
    LOG.info("{} Stopping ContinuousBackupReplicationEndpoint...",
      Utils.logPeerId(ctx.getPeerId()));
    stopAsync();
  }

  @Override
  protected void doStop() {
    LOG.info("{} ContinuousBackupReplicationEndpoint stopped successfully.",
      Utils.logPeerId(ctx.getPeerId()));
    notifyStopped();
  }

  private void setPeerUUID(Configuration conf) throws IOException {
    String peerUUIDStr = conf.get(CONF_PEER_UUID);
    if (peerUUIDStr == null || peerUUIDStr.isEmpty()) {
      LOG.error("{} Peer UUID is missing. Please specify it with the {} configuration.",
        Utils.logPeerId(ctx.getPeerId()), CONF_PEER_UUID);
      throw new IOException("Peer UUID not specified in configuration");
    }
    try {
      peerUUID = UUID.fromString(peerUUIDStr);
      LOG.info("{} Peer UUID set to {}", Utils.logPeerId(ctx.getPeerId()), peerUUID);
    } catch (IllegalArgumentException e) {
      LOG.error("{} Invalid Peer UUID format: {}", Utils.logPeerId(ctx.getPeerId()), peerUUIDStr,
        e);
      throw new IOException("Invalid Peer UUID format", e);
    }
  }

  private void uploadBulkLoadFiles(List<Path> bulkLoadFiles) throws IOException {
    for (Path file : bulkLoadFiles) {
      Path sourcePath = getBulkloadFileStagingPath(file);
      Path destPath = new Path(backupFileSystemManager.getBulkLoadFilesDir(), file);

      try {
        FileUtil.copy(CommonFSUtils.getRootDirFileSystem(conf), sourcePath,
          backupFileSystemManager.getBackupFs(), destPath, false, conf);
        LOG.info("{} Bulk load file {} successfully backed up to {}", Utils.logPeerId(peerId), file,
          destPath);
      } catch (IOException e) {
        LOG.error("{} Failed to back up bulk load file: {}", Utils.logPeerId(peerId), file, e);
        throw e;
      }
    }
  }

  private Path getBulkloadFileStagingPath(Path file) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
    Path baseNamespaceDir = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR));
    Path hFileArchiveDir =
      new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNamespaceDir));
    return findExistingPath(rootFs, baseNamespaceDir, hFileArchiveDir, file);
  }

  private static Path findExistingPath(FileSystem rootFs, Path baseNamespaceDir,
    Path hFileArchiveDir, Path filePath) throws IOException {
    for (Path candidate : new Path[] { new Path(baseNamespaceDir, filePath),
      new Path(hFileArchiveDir, filePath) }) {
      if (rootFs.exists(candidate)) {
        return candidate;
      }
    }
    return null;
  }
}
