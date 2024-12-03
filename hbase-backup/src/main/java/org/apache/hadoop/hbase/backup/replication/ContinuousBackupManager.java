package org.apache.hadoop.hbase.backup.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
public class ContinuousBackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupManager.class);
  public static final String CONF_BACKUP_ROOT_DIR = "hbase.backup.root.dir";
  public static final String CONF_BACKUP_MAX_WAL_SIZE = "hbase.backup.max.wal.size";
  public static final long DEFAULT_MAX_WAL_SIZE = 128 * 1024 * 1024;
  private final Configuration conf;
  private final String peerId;
  private final BackupFileSystemManager backupFileSystemManager;
  private final ContinuousBackupStagingManager stagingManager;

  public ContinuousBackupManager(String peerId, Configuration conf) throws BackupConfigurationException {
    this.peerId = peerId;
    this.conf = conf;
    String backupRootDirStr = conf.get(CONF_BACKUP_ROOT_DIR);
    if (backupRootDirStr == null || backupRootDirStr.isEmpty()) {
      String errorMsg = Utils.logPeerId(peerId) + " Backup root directory not specified. Set it using " + CONF_BACKUP_ROOT_DIR;
      LOG.error(errorMsg);
      throw new BackupConfigurationException(errorMsg);
    }
    LOG.debug("{} Backup root directory: {}", Utils.logPeerId(peerId), backupRootDirStr);

    try {
      this.backupFileSystemManager = new BackupFileSystemManager(peerId, conf, backupRootDirStr);
      LOG.info("{} BackupFileSystemManager initialized successfully.", Utils.logPeerId(peerId));
    } catch (IOException e) {
      String errorMsg = Utils.logPeerId(peerId) + " Failed to initialize BackupFileSystemManager";
      LOG.error(errorMsg, e);
      throw new BackupConfigurationException(errorMsg, e);
    }

    try {
      this.stagingManager = new ContinuousBackupStagingManager(conf, this);
      LOG.info("{} ContinuousBackupStagingManager initialized successfully.", Utils.logPeerId(peerId));
    } catch (IOException e) {
      String errorMsg = "Failed to initialize ContinuousBackupStagingManager";
      LOG.error(errorMsg, e);
      throw new BackupConfigurationException(errorMsg, e);
    }
  }

  public void backup(Map<TableName, List<WAL.Entry>> tableToEntriesMap) throws IOException {
    LOG.debug("{} Starting backup process for {} table(s)", Utils.logPeerId(peerId), tableToEntriesMap.size());

    for (Map.Entry<TableName, List<WAL.Entry>> entry : tableToEntriesMap.entrySet()) {
      TableName tableName = entry.getKey();
      List<WAL.Entry> walEntries = entry.getValue();

      LOG.debug("{} Processing {} WAL entries for table: {}", Utils.logPeerId(peerId), walEntries.size(), tableName);

      List<Path> bulkLoadFiles = BulkLoadProcessor.processBulkLoadFiles(tableName, walEntries);
      LOG.debug("{} Identified {} bulk load file(s) for table: {}", Utils.logPeerId(peerId), bulkLoadFiles.size(), tableName);

      stagingManager.stageEntries(tableName, walEntries, bulkLoadFiles);
      LOG.debug("{} Staged WAL entries and bulk load files for table: {}", Utils.logPeerId(peerId), tableName);
    }

    LOG.debug("{} Backup process completed for all tables.", Utils.logPeerId(peerId));
  }

  public void commitBackup(FileSystem sourceFs, Path walFile, List<Path> bulkLoadFiles) throws IOException {
    LOG.debug("{} Starting commit for WAL file: {}", Utils.logPeerId(peerId), walFile);

    Path sourcePath = stagingManager.getWalFileStagingPath(walFile);
    Path backupWalPath = new Path(backupFileSystemManager.getWalsDir(), walFile);

    try {
      FileUtil.copy(sourceFs, sourcePath, backupFileSystemManager.getBackupFs(), backupWalPath, false, conf);
      LOG.info("{} WAL file {} successfully backed up to {}", Utils.logPeerId(peerId), walFile, backupWalPath);
    } catch (IOException e) {
      LOG.error("{} Failed to back up WAL file: {}", Utils.logPeerId(peerId), walFile, e);
      throw e;
    }

    uploadBulkLoadFiles(sourceFs, bulkLoadFiles);
    LOG.debug("{} Commit completed for WAL file: {}", Utils.logPeerId(peerId), walFile);
  }

  private void uploadBulkLoadFiles(FileSystem sourceFs, List<Path> bulkLoadFiles) throws IOException {
    for (Path file : bulkLoadFiles) {
      Path sourcePath = stagingManager.getBulkloadFileStagingPath(file);
      Path destPath = new Path(backupFileSystemManager.getBulkLoadFilesDir(), file);

      try {
        FileUtil.copy(sourceFs, sourcePath, backupFileSystemManager.getBackupFs(), destPath, false, conf);
        LOG.info("{} Bulk load file {} successfully backed up to {}", Utils.logPeerId(peerId), file, destPath);
      } catch (IOException e) {
        LOG.error("{} Failed to back up bulk load file: {}", Utils.logPeerId(peerId), file, e);
        throw e;
      }
    }
  }

  public void close() {
    stagingManager.close();
  }

  public String getPeerId() {
    return peerId;
  }
}
