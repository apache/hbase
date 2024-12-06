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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ContinuousBackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupManager.class);
  public static final String WAL_FILE_PREFIX = "wal_file.";
  public static final String CONF_BACKUP_ROOT_DIR = "hbase.backup.root.dir";
  public static final String CONF_BACKUP_MAX_WAL_SIZE = "hbase.backup.max.wal.size";
  public static final long DEFAULT_MAX_WAL_SIZE = 128 * 1024 * 1024;

  private final Configuration conf;
  private final String peerId;
  private final BackupFileSystemManager backupFileSystemManager;
  private final Map<TableName, FSHLogProvider.Writer> walWriters = new HashMap<>();

  /**
   * Constructs a {@code ContinuousBackupManager} instance with the specified peer ID and
   * configuration.
   * @param peerId the unique identifier of the replication peer
   * @param conf   the HBase configuration object
   * @throws ContinuousBackupConfigurationException if the backup configuration is invalid
   */
  public ContinuousBackupManager(String peerId, Configuration conf)
    throws ContinuousBackupConfigurationException {
    this.peerId = peerId;
    this.conf = conf;

    String backupRootDirStr = conf.get(CONF_BACKUP_ROOT_DIR);
    if (backupRootDirStr == null || backupRootDirStr.isEmpty()) {
      String errorMsg = Utils.logPeerId(peerId)
        + " Backup root directory not specified. Set it using " + CONF_BACKUP_ROOT_DIR;
      LOG.error(errorMsg);
      throw new ContinuousBackupConfigurationException(errorMsg);
    }
    LOG.debug("{} Backup root directory: {}", Utils.logPeerId(peerId), backupRootDirStr);

    try {
      this.backupFileSystemManager = new BackupFileSystemManager(peerId, conf, backupRootDirStr);
      LOG.info("{} BackupFileSystemManager initialized successfully.", Utils.logPeerId(peerId));
    } catch (IOException e) {
      String errorMsg = Utils.logPeerId(peerId) + " Failed to initialize BackupFileSystemManager";
      LOG.error(errorMsg, e);
      throw new ContinuousBackupConfigurationException(errorMsg, e);
    }
  }

  public void backup(Map<TableName, List<WAL.Entry>> tableToEntriesMap) throws IOException {
    LOG.debug("{} Starting backup process for {} table(s)", Utils.logPeerId(peerId),
      tableToEntriesMap.size());

    for (Map.Entry<TableName, List<WAL.Entry>> entry : tableToEntriesMap.entrySet()) {
      TableName tableName = entry.getKey();
      List<WAL.Entry> walEntries = entry.getValue();

      LOG.debug("{} Processing {} WAL entries for table: {}", Utils.logPeerId(peerId),
        walEntries.size(), tableName);

      List<Path> bulkLoadFiles = BulkLoadProcessor.processBulkLoadFiles(tableName, walEntries);
      LOG.debug("{} Identified {} bulk load file(s) for table: {}", Utils.logPeerId(peerId),
        bulkLoadFiles.size(), tableName);

      backupEntries(tableName, walEntries, bulkLoadFiles);
      LOG.debug("{} Backed WAL entries and bulk load files for table: {}", Utils.logPeerId(peerId),
        tableName);
    }

    LOG.debug("{} Backup process completed for all tables.", Utils.logPeerId(peerId));
  }

  private void backupEntries(TableName tableName, List<WAL.Entry> walEntries,
    List<Path> bulkLoadFiles) throws IOException {
    FSHLogProvider.Writer writer = getWalWriter(tableName);
    for (WAL.Entry entry : walEntries) {
      writer.append(entry);
    }
    writer.sync(true);
    uploadBulkLoadFiles(bulkLoadFiles);

    if (isWriterFull(writer)) {
      writer.close();
      walWriters.remove(tableName);
    }
  }

  private boolean isWriterFull(FSHLogProvider.Writer writer) {
    long maxWalSize = conf.getLong(ContinuousBackupManager.CONF_BACKUP_MAX_WAL_SIZE,
      ContinuousBackupManager.DEFAULT_MAX_WAL_SIZE);
    return writer.getLength() >= maxWalSize;
  }

  private FSHLogProvider.Writer getWalWriter(TableName tableName) throws IOException {
    try {
      return walWriters.computeIfAbsent(tableName, this::createNewWalWriter);
    } catch (UncheckedIOException e) {
      String errorMsg =
        Utils.logPeerId(peerId) + " Failed to get or create WAL Writer for " + tableName;
      throw new IOException(errorMsg, e);
    }
  }

  private FSHLogProvider.Writer createNewWalWriter(TableName tableName) {
    FileSystem fs = backupFileSystemManager.getBackupFs();
    Path walsDir = backupFileSystemManager.getWalsDir();
    String namespace = tableName.getNamespaceAsString();
    String table = tableName.getQualifierAsString();
    String walFileName = WAL_FILE_PREFIX + EnvironmentEdgeManager.getDelegate().currentTime();
    Path walFilePath = new Path(new Path(walsDir, namespace), new Path(table, walFileName));

    try {
      return createWriter(conf, fs, walFilePath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create WAL Writer for " + table, e);
    }
  }

  private FSHLogProvider.Writer createWriter(Configuration conf, FileSystem fs, Path path)
    throws IOException {
    try {
      FSHLogProvider.Writer writer =
        ObjectStoreProtobufWalWriter.class.getDeclaredConstructor().newInstance();
      writer.init(fs, path, conf, true, WALUtil.getWALBlockSize(conf, fs, path),
        StreamSlowMonitor.create(conf, path.getName()));
      return writer;
    } catch (Exception e) {
      throw new IOException("Cannot initialize WAL Writer", e);
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

  public void close() {
    walWriters.forEach((tableName, writer) -> closeWriter(tableName));
    LOG.info("{} All WAL writers closed.", Utils.logPeerId(peerId));
  }

  private void closeWriter(TableName tableName) {
    FSHLogProvider.Writer writer = walWriters.remove(tableName);
    if (writer != null) {
      try {
        writer.close();
        LOG.info("{} Closed WAL writer for table: {}", Utils.logPeerId(peerId), tableName);
      } catch (IOException e) {
        LOG.error("{} Failed to close WAL writer for table: {}", Utils.logPeerId(peerId), tableName,
          e);
      }
    }
  }
}
