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

import static org.apache.hadoop.hbase.master.cleaner.HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.*;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the staging and backup of Write-Ahead Logs (WALs) and bulk-loaded files as part of the
 * continuous backup process in HBase. This class ensures that WALs are staged, flushed, and backed
 * up safely to support backup and recovery workflows.
 */
@InterfaceAudience.Private
public class ContinuousBackupStagingManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupStagingManager.class);

  public static final String WALS_BACKUP_STAGING_DIR = "wal-backup-staging";
  public static final String CONF_STAGED_WAL_FLUSH_INITIAL_DELAY =
    "hbase.backup.staged.wal.flush.initial.delay.seconds";
  public static final int DEFAULT_STAGED_WAL_FLUSH_INITIAL_DELAY_SECONDS = 5 * 60; // 5 minutes
  public static final String CONF_STAGED_WAL_FLUSH_INTERVAL =
    "hbase.backup.staged.wal.flush.interval.seconds";
  public static final int DEFAULT_STAGED_WAL_FLUSH_INTERVAL_SECONDS = 5 * 60; // 5 minutes
  public static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 60; // TODO: configurable??

  private final Configuration conf;
  private final FileSystem walStagingFs;
  private final Path walStagingDir;
  private final ConcurrentHashMap<Path, ContinuousBackupWalWriter> walWriterMap =
    new ConcurrentHashMap<>();
  private final ContinuousBackupManager continuousBackupManager;
  private ScheduledExecutorService flushExecutor;
  private ExecutorService backupExecutor;
  private final Set<Path> filesCurrentlyBeingBackedUp = ConcurrentHashMap.newKeySet();
  private final ReentrantLock lock = new ReentrantLock();
  private final StagedBulkloadFileRegistry stagedBulkloadFileRegistry;

  /**
   * Constructs a ContinuousBackupStagingManager with the specified configuration and backup
   * manager.
   * @param conf                    the HBase configuration
   * @param continuousBackupManager the backup manager for continuous backup
   * @throws IOException if there is an error initializing the WAL staging directory or related
   *                     resources
   */
  public ContinuousBackupStagingManager(Configuration conf,
    ContinuousBackupManager continuousBackupManager) throws IOException {
    this.conf = conf;
    this.continuousBackupManager = continuousBackupManager;
    // TODO: configurable??
    this.walStagingFs = CommonFSUtils.getRootDirFileSystem(conf);
    this.walStagingDir = new Path(CommonFSUtils.getRootDir(conf),
      new Path(WALS_BACKUP_STAGING_DIR, continuousBackupManager.getPeerId()));

    ensureHFileCleanerPluginConfigured();
    initWalStagingDir();
    startWalFlushExecutor();
    startBackupExecutor();

    Connection conn = ConnectionFactory.createConnection(conf);
    this.stagedBulkloadFileRegistry =
      new StagedBulkloadFileRegistry(conn, continuousBackupManager.getPeerId());
  }

  private void ensureHFileCleanerPluginConfigured() throws IOException {
    String plugins = conf.get(MASTER_HFILE_CLEANER_PLUGINS);
    String cleanerClass = ContinuousBackupStagedHFileCleaner.class.getCanonicalName();
    if (plugins == null || !plugins.contains(cleanerClass)) {
      String errorMsg = Utils.logPeerId(continuousBackupManager.getPeerId())
        + " Continuous Backup Bulk-loaded HFile's Cleaner plugin is invalid or missing: "
        + cleanerClass;
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }

  private void initWalStagingDir() throws IOException {
    if (walStagingFs.exists(walStagingDir)) {
      LOG.debug("{} WALs staging directory already exists: {}",
        Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);
    } else {
      walStagingFs.mkdirs(walStagingDir);
      LOG.debug("{} WALs staging directory created: {}",
        Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);
    }
  }

  private void startWalFlushExecutor() {
    int initialDelay = conf.getInt(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY,
      DEFAULT_STAGED_WAL_FLUSH_INITIAL_DELAY_SECONDS);
    int flushInterval =
      conf.getInt(CONF_STAGED_WAL_FLUSH_INTERVAL, DEFAULT_STAGED_WAL_FLUSH_INTERVAL_SECONDS);

    flushExecutor = Executors.newSingleThreadScheduledExecutor();
    flushExecutor.scheduleAtFixedRate(this::flushAndBackupSafely, initialDelay, flushInterval,
      TimeUnit.SECONDS);
  }

  private void flushAndBackupSafely() {
    try {
      LOG.info("{} Periodic WAL flush triggered...",
        Utils.logPeerId(continuousBackupManager.getPeerId()));
      flushWalFiles();
      backupWalFiles();
    } catch (IOException e) {
      LOG.error("{} Error during periodic WAL flush: {}",
        Utils.logPeerId(continuousBackupManager.getPeerId()), e.getMessage(), e);
    }
  }

  private void flushWalFiles() {
    lock.lock();
    try {
      for (Map.Entry<Path, ContinuousBackupWalWriter> entry : walWriterMap.entrySet()) {
        flushWalData(entry.getKey(), entry.getValue());
      }
    } finally {
      lock.unlock();
    }
  }

  private void flushWalData(Path walDir, ContinuousBackupWalWriter writer) {
    if (writer.hasAnyEntry()) {
      LOG.debug("{} Flushing WAL data for {}", Utils.logPeerId(continuousBackupManager.getPeerId()),
        walDir);
      closeWriter(writer);
      walWriterMap.put(walDir, createNewContinuousBackupWalWriter(walDir));
    } else {
      LOG.debug("{} No WAL data to flush for {}",
        Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);
    }
  }

  private void closeWriter(ContinuousBackupWalWriter writer) {
    try {
      writer.close();
    } catch (IOException e) {
      LOG.error("{} Error occurred while closing WAL writer: ",
        Utils.logPeerId(continuousBackupManager.getPeerId()), e);
    }
  }

  private ContinuousBackupWalWriter createNewContinuousBackupWalWriter(Path walDir) {
    try {
      return new ContinuousBackupWalWriter(walStagingFs, walStagingDir, walDir, conf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create WAL Writer for " + walDir, e);
    }
  }

  private void startBackupExecutor() {
    // TODO: more threads maybe. based on some configuration
    backupExecutor = Executors.newFixedThreadPool(1);
  }

  /**
   * Stages WAL entries and bulk-load files for a specific table. Ensures that WAL entries are
   * written and bulk-loaded files are registered to prevent deletion by the HFileCleaner thread.
   * @param tableName     the name of the table
   * @param walEntries    the list of WAL entries to stage
   * @param bulkLoadFiles the list of bulk-load files to stage
   * @throws IOException if there is an error staging the entries or files
   */
  public void stageEntries(TableName tableName, List<WAL.Entry> walEntries,
    List<Path> bulkLoadFiles) throws IOException {
    lock.lock();
    try {
      String namespace = tableName.getNamespaceAsString();
      String table = tableName.getQualifierAsString();

      Path walDir = WALUtils.getWalDir(namespace, table);
      ContinuousBackupWalWriter continuousBackupWalWriter = getContinuousBackupWalWriter(walDir);

      continuousBackupWalWriter.write(walEntries, bulkLoadFiles);

      // prevent bulk-loaded files from deleting HFileCleaner thread
      stagedBulkloadFileRegistry.addStagedFiles(bulkLoadFiles);

      LOG.info("{} {} WAL entries staged for table {}:{}",
        Utils.logPeerId(continuousBackupManager.getPeerId()), walEntries.size(), namespace, table);
    } finally {
      lock.unlock();
    }
  }

  private ContinuousBackupWalWriter getContinuousBackupWalWriter(Path walDir) throws IOException {
    try {
      ContinuousBackupWalWriter writer =
        walWriterMap.computeIfAbsent(walDir, this::createNewContinuousBackupWalWriter);
      if (shouldRollOver(writer)) {
        LOG.debug("{} WAL Writer for {} is being rolled over.",
          Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);
        closeWriter(writer);
        writer = createNewContinuousBackupWalWriter(walDir);
        walWriterMap.put(walDir, writer);
      }
      return writer;
    } catch (UncheckedIOException e) {
      String errorMsg = Utils.logPeerId(continuousBackupManager.getPeerId())
        + " Failed to get or create WAL Writer for " + walDir;
      throw new IOException(errorMsg, e);
    }
  }

  private boolean shouldRollOver(ContinuousBackupWalWriter writer) {
    long maxWalSize = conf.getLong(ContinuousBackupManager.CONF_BACKUP_MAX_WAL_SIZE,
      ContinuousBackupManager.DEFAULT_MAX_WAL_SIZE);
    return writer.getLength() >= maxWalSize;
  }

  /**
   * Returns the staging path for a bulk-load file, given its relative path from the namespace.
   * @param relativePathFromNamespace the relative path of the bulk-load file
   * @return the resolved staging path for the bulk-load file
   * @throws IOException if there is an error resolving the staging path
   */
  public Path getBulkloadFileStagingPath(Path relativePathFromNamespace) throws IOException {
    return WALUtils.getBulkloadFileStagingPath(conf, relativePathFromNamespace);
  }

  /**
   * Returns the staging path for a WAL file, given its relative path.
   * @param relativeWalPath the relative path of the WAL file
   * @return the resolved staging path for the WAL file
   */
  public Path getWalFileStagingPath(Path relativeWalPath) {
    return WALUtils.getWalFileStagingPath(walStagingDir, relativeWalPath);
  }

  private void backupWalFiles() throws IOException {
    LOG.info("{} Starting backup of WAL files from staging directory: {}",
      Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);

    RemoteIterator<LocatedFileStatus> fileStatusIterator =
      walStagingFs.listFiles(walStagingDir, true);
    while (fileStatusIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIterator.next();
      Path filePath = fileStatus.getPath();
      LOG.trace("{} Processing file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()),
        filePath);

      // Skip directories, context files, or files already backed up
      if (
        fileStatus.isDirectory()
          || ContinuousBackupWalWriter.isWalWriterContextFile(filePath.getName())
          || isFileCurrentlyBeingBackedUp(filePath)
      ) {
        LOG.trace("{} Skipping file (directory/context/backed-up): {}",
          Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        continue;
      }

      // Check if the file is currently being written
      if (isFileOpenForWriting(filePath)) {
        LOG.info("{} Skipping file as it is currently being written: {}",
          Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        continue;
      }

      // Backup file asynchronously
      backupFileAsync(filePath);
    }

    LOG.info("{} Completed backup process for WAL files from staging directory: {}",
      Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);
  }

  private boolean isFileCurrentlyBeingBackedUp(Path filePath) {
    return filesCurrentlyBeingBackedUp.contains(filePath);
  }

  private boolean isFileOpenForWriting(Path filePath) {
    for (ContinuousBackupWalWriter context : walWriterMap.values()) {
      if (context.isWritingToFile(filePath)) {
        return true;
      }
    }
    return false;
  }

  private void backupFileAsync(Path filePath) {
    // Mark the file as currently being backed up
    filesCurrentlyBeingBackedUp.add(filePath);

    backupExecutor.submit(() -> {
      try {
        backupFile(filePath);
      } catch (IOException e) {
        LOG.error("Backup failed for {}", filePath, e);
      } finally {
        // Remove the file from the "being backed up" set once processing is done
        filesCurrentlyBeingBackedUp.remove(filePath);
      }
    });
  }

  // Backups a single file
  private void backupFile(Path filePath) throws IOException {
    LOG.info("{} Starting backup for WAL file: {}",
      Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);

    String tableName = filePath.getParent().getName();
    String namespace = filePath.getParent().getParent().getName();
    Path walDir = WALUtils.getWalDir(namespace, tableName);
    String walFileName = filePath.getName();
    Path walFilePath = new Path(walDir, walFileName);
    String walWriterContextFileName =
      walFileName + ContinuousBackupWalWriter.WAL_WRITER_CONTEXT_FILE_SUFFIX;
    Path walWriterContextFileFullPath =
      new Path(walStagingDir, new Path(walDir, walWriterContextFileName));

    LOG.debug("{} Resolving bulkload files for WAL writer context: {}",
      Utils.logPeerId(continuousBackupManager.getPeerId()), walWriterContextFileFullPath);
    List<Path> bulkLoadFiles = ContinuousBackupWalWriter.getBulkloadFilesFromProto(walStagingFs,
      walWriterContextFileFullPath);

    continuousBackupManager.commitBackup(walStagingFs, walFilePath, bulkLoadFiles);

    stagedBulkloadFileRegistry.removeStagedFiles(bulkLoadFiles);

    LOG.debug("{} Cleaning up WAL and metadata files for WAL: {}",
      Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
    deleteFile(filePath);
    deleteFile(walWriterContextFileFullPath);

    LOG.info("{} Backup completed successfully for WAL: {}",
      Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
  }

  private void deleteFile(Path filePath) {
    try {
      if (walStagingFs.exists(filePath)) {
        if (walStagingFs.delete(filePath, false)) {
          LOG.debug("{} Deleted file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()),
            filePath);
        } else {
          LOG.warn("{} Failed to delete file: {}",
            Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        }
      }
    } catch (IOException e) {
      LOG.error("{} Error while deleting file: {}",
        Utils.logPeerId(continuousBackupManager.getPeerId()), filePath, e);
    }
  }

  /**
   * Closes the manager, ensuring that all executors are properly terminated and resources are
   * cleaned up.
   */
  public void close() {
    // Shutdown the flush executor
    if (flushExecutor != null) {
      flushExecutor.shutdown();
      try {
        if (
          !flushExecutor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        ) {
          flushExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        flushExecutor.shutdownNow();
        LOG.warn("Flush executor shutdown was interrupted.", e);
      }
      LOG.info("{} WAL flush thread stopped.",
        Utils.logPeerId(continuousBackupManager.getPeerId()));
    }

    // Shutdown the backup executor
    if (backupExecutor != null) {
      backupExecutor.shutdown();
      try {
        if (
          !backupExecutor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        ) {
          backupExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        backupExecutor.shutdownNow();
        LOG.warn("Backup executor shutdown was interrupted.", e);
      }
      LOG.info("{} Backup executor stopped.", Utils.logPeerId(continuousBackupManager.getPeerId()));
    }

    // Flush remaining writers safely
    for (Map.Entry<Path, ContinuousBackupWalWriter> entry : walWriterMap.entrySet()) {
      Path walDir = entry.getKey();
      ContinuousBackupWalWriter writer = entry.getValue();

      if (writer.hasAnyEntry()) {
        LOG.debug("{} Flushing WAL data for {}",
          Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);
        closeWriter(writer);
      } else {
        LOG.debug("{} No WAL data to flush for {}",
          Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);

        // Remove the empty writer and delete associated files
        closeWriter(writer);
        deleteEmptyWalFile(writer, walDir);
      }
    }
  }

  private void deleteEmptyWalFile(ContinuousBackupWalWriter writer, Path walDir) {
    Path walFilePath = writer.getWalFullPath();
    String walFileName = walFilePath.getName();
    String walWriterContextFileName =
      walFileName + ContinuousBackupWalWriter.WAL_WRITER_CONTEXT_FILE_SUFFIX;
    Path walWriterContextFileFullPath =
      new Path(walStagingDir, new Path(walDir, walWriterContextFileName));

    try {
      deleteFile(walFilePath);
    } catch (Exception e) {
      LOG.warn("Failed to delete WAL file: {}", walFilePath, e);
    }

    try {
      deleteFile(walWriterContextFileFullPath);
    } catch (Exception e) {
      LOG.warn("Failed to delete WAL writer context file: {}", walWriterContextFileFullPath, e);
    }
  }

  public static class WALUtils {
    public static Path getWalDir(String namespace, String table) {
      return new Path(namespace, table);
    }

    public static Path getWalFileStagingPath(Path walsStagingDir, Path relativeWalPath) {
      return new Path(walsStagingDir, relativeWalPath);
    }

    public static Path getBulkloadFileStagingPath(Configuration conf,
      Path relativePathFromNamespace) throws IOException {
      Path rootDir = CommonFSUtils.getRootDir(conf);
      FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
      Path baseNamespaceDir = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR));
      Path hFileArchiveDir =
        new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNamespaceDir));
      return findExistingPath(rootFs, baseNamespaceDir, hFileArchiveDir, relativePathFromNamespace);
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

}
