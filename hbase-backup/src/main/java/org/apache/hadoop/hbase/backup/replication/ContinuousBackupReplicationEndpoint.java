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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.EmptyEntriesPolicy;
import org.apache.hadoop.hbase.replication.ReplicationResult;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ContinuousBackupReplicationEndpoint is responsible for replicating WAL entries to a backup
 * storage. It organizes WAL entries by day and periodically flushes the data, ensuring that WAL
 * files do not exceed the configured size. The class includes mechanisms for handling the WAL
 * files, performing bulk load backups, and ensuring that the replication process is safe.
 */
@InterfaceAudience.Private
public class ContinuousBackupReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG =
    LoggerFactory.getLogger(ContinuousBackupReplicationEndpoint.class);
  public static final String CONF_PEER_UUID = "hbase.backup.wal.replication.peerUUID";
  public static final String CONF_BACKUP_ROOT_DIR = "hbase.backup.root.dir";
  public static final String CONF_BACKUP_MAX_WAL_SIZE = "hbase.backup.max.wal.size";
  public static final long DEFAULT_MAX_WAL_SIZE = 128 * 1024 * 1024;

  public static final String CONF_STAGED_WAL_FLUSH_INITIAL_DELAY =
    "hbase.backup.staged.wal.flush.initial.delay.seconds";
  public static final int DEFAULT_STAGED_WAL_FLUSH_INITIAL_DELAY_SECONDS = 5 * 60; // 5 minutes
  public static final String CONF_STAGED_WAL_FLUSH_INTERVAL =
    "hbase.backup.staged.wal.flush.interval.seconds";
  public static final int DEFAULT_STAGED_WAL_FLUSH_INTERVAL_SECONDS = 5 * 60; // 5 minutes
  public static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 60; // TODO: configurable??

  private final Map<Long, FSHLogProvider.Writer> walWriters = new ConcurrentHashMap<>();
  private final ReentrantLock lock = new ReentrantLock();

  private ReplicationSourceInterface replicationSource;
  private Configuration conf;
  private BackupFileSystemManager backupFileSystemManager;
  private UUID peerUUID;
  private String peerId;
  private ScheduledExecutorService flushExecutor;

  private long latestWALEntryTimestamp = -1L;

  public static final long ONE_DAY_IN_MILLISECONDS = TimeUnit.DAYS.toMillis(1);
  public static final String WAL_FILE_PREFIX = "wal_file.";

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.replicationSource = context.getReplicationSource();
    this.peerId = context.getPeerId();
    this.conf = HBaseConfiguration.create(context.getConfiguration());

    initializePeerUUID();
    initializeBackupFileSystemManager();
    startWalFlushExecutor();
    LOG.info("{} Initialization complete", Utils.logPeerId(peerId));
  }

  private void initializePeerUUID() throws IOException {
    String peerUUIDStr = conf.get(CONF_PEER_UUID);
    if (peerUUIDStr == null || peerUUIDStr.isEmpty()) {
      throw new IOException("Peer UUID is not specified. Please configure " + CONF_PEER_UUID);
    }
    try {
      this.peerUUID = UUID.fromString(peerUUIDStr);
      LOG.info("{} Peer UUID initialized to {}", Utils.logPeerId(peerId), peerUUID);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid Peer UUID format: " + peerUUIDStr, e);
    }
  }

  private void initializeBackupFileSystemManager() throws IOException {
    String backupRootDir = conf.get(CONF_BACKUP_ROOT_DIR);
    if (backupRootDir == null || backupRootDir.isEmpty()) {
      throw new IOException(
        "Backup root directory is not specified. Configure " + CONF_BACKUP_ROOT_DIR);
    }

    try {
      this.backupFileSystemManager = new BackupFileSystemManager(peerId, conf, backupRootDir);
      LOG.info("{} BackupFileSystemManager initialized successfully for {}",
        Utils.logPeerId(peerId), backupRootDir);
    } catch (IOException e) {
      throw new IOException("Failed to initialize BackupFileSystemManager", e);
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
    LOG.info("{} Scheduled WAL flush executor started with initial delay {}s and interval {}s",
      Utils.logPeerId(peerId), initialDelay, flushInterval);
  }

  private void flushAndBackupSafely() {
    lock.lock();
    try {
      LOG.info("{} Periodic WAL flush triggered", Utils.logPeerId(peerId));
      flushWriters();
      replicationSource.persistOffsets();
      LOG.info("{} Periodic WAL flush and offset persistence completed successfully",
        Utils.logPeerId(peerId));
    } catch (IOException e) {
      LOG.error("{} Error during WAL flush: {}", Utils.logPeerId(peerId), e.getMessage(), e);
    } finally {
      lock.unlock();
    }
  }

  private void flushWriters() throws IOException {
    LOG.info("{} Flushing {} WAL writers", Utils.logPeerId(peerId), walWriters.size());
    for (Map.Entry<Long, FSHLogProvider.Writer> entry : walWriters.entrySet()) {
      FSHLogProvider.Writer writer = entry.getValue();
      if (writer != null) {
        LOG.debug("{} Closing WAL writer for day: {}", Utils.logPeerId(peerId), entry.getKey());
        try {
          writer.close();
          LOG.debug("{} Successfully closed WAL writer for day: {}", Utils.logPeerId(peerId),
            entry.getKey());
        } catch (IOException e) {
          LOG.error("{} Failed to close WAL writer for day: {}. Error: {}", Utils.logPeerId(peerId),
            entry.getKey(), e.getMessage(), e);
          throw e;
        }
      }
    }
    walWriters.clear();

    // All received WAL entries have been flushed and persisted successfully.
    // At this point, it's safe to record the latest replicated timestamp,
    // as we are guaranteed that all entries up to that timestamp are durably stored.
    // This checkpoint is essential for enabling consistent Point-in-Time Restore (PITR).
    updateLastReplicatedTimestampForContinuousBackup();

    LOG.info("{} WAL writers flushed and cleared", Utils.logPeerId(peerId));
  }

  @Override
  public UUID getPeerUUID() {
    return peerUUID;
  }

  @Override
  public void start() {
    LOG.info("{} Starting ContinuousBackupReplicationEndpoint", Utils.logPeerId(peerId));
    startAsync();
  }

  @Override
  protected void doStart() {
    LOG.info("{} ContinuousBackupReplicationEndpoint started successfully.",
      Utils.logPeerId(peerId));
    notifyStarted();
  }

  @Override
  public EmptyEntriesPolicy getEmptyEntriesPolicy() {
    // Since this endpoint writes to S3 asynchronously, an empty entry batch
    // does not guarantee that all previously submitted entries were persisted.
    // Hence, avoid committing the WAL position.
    return EmptyEntriesPolicy.SUBMIT;
  }

  @Override
  public ReplicationResult replicate(ReplicateContext replicateContext) {
    final List<WAL.Entry> entries = replicateContext.getEntries();
    if (entries.isEmpty()) {
      LOG.debug("{} No WAL entries to replicate", Utils.logPeerId(peerId));
      return ReplicationResult.SUBMITTED;
    }

    LOG.debug("{} Received {} WAL entries for replication", Utils.logPeerId(peerId),
      entries.size());

    Map<Long, List<WAL.Entry>> groupedEntries = groupEntriesByDay(entries);
    LOG.debug("{} Grouped WAL entries by day: {}", Utils.logPeerId(peerId),
      groupedEntries.keySet());

    lock.lock();
    try {
      for (Map.Entry<Long, List<WAL.Entry>> entry : groupedEntries.entrySet()) {
        LOG.debug("{} Backing up {} WAL entries for day {}", Utils.logPeerId(peerId),
          entry.getValue().size(), entry.getKey());
        backupWalEntries(entry.getKey(), entry.getValue());
      }

      // Capture the timestamp of the last WAL entry processed. This is used as the replication
      // checkpoint so that point-in-time restores know the latest consistent time up to which
      // replication has
      // occurred.
      latestWALEntryTimestamp = entries.get(entries.size() - 1).getKey().getWriteTime();

      if (isAnyWriterFull()) {
        LOG.debug("{} Some WAL writers reached max size, triggering flush",
          Utils.logPeerId(peerId));
        flushWriters();
        LOG.debug("{} Replication committed after WAL flush", Utils.logPeerId(peerId));
        return ReplicationResult.COMMITTED;
      }

      LOG.debug("{} Replication submitted successfully", Utils.logPeerId(peerId));
      return ReplicationResult.SUBMITTED;
    } catch (IOException e) {
      LOG.error("{} Replication failed. Error details: {}", Utils.logPeerId(peerId), e.getMessage(),
        e);
      return ReplicationResult.FAILED;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Persists the latest replicated WAL entry timestamp in the backup system table. This checkpoint
   * is critical for Continuous Backup and Point-in-Time Restore (PITR) to ensure restore operations
   * only go up to a known safe point. The value is stored per region server using its ServerName as
   * the key.
   * @throws IOException if the checkpoint update fails
   */
  private void updateLastReplicatedTimestampForContinuousBackup() throws IOException {
    try (final Connection conn = ConnectionFactory.createConnection(conf);
      BackupSystemTable backupSystemTable = new BackupSystemTable(conn)) {
      backupSystemTable.updateBackupCheckpointTimestamp(replicationSource.getServerWALsBelongTo(),
        latestWALEntryTimestamp);
    }
  }

  private Map<Long, List<WAL.Entry>> groupEntriesByDay(List<WAL.Entry> entries) {
    return entries.stream().collect(
      Collectors.groupingBy(entry -> (entry.getKey().getWriteTime() / ONE_DAY_IN_MILLISECONDS)
        * ONE_DAY_IN_MILLISECONDS));
  }

  private boolean isAnyWriterFull() {
    return walWriters.values().stream().anyMatch(this::isWriterFull);
  }

  private boolean isWriterFull(FSHLogProvider.Writer writer) {
    long maxWalSize = conf.getLong(CONF_BACKUP_MAX_WAL_SIZE, DEFAULT_MAX_WAL_SIZE);
    return writer.getLength() >= maxWalSize;
  }

  private void backupWalEntries(long day, List<WAL.Entry> walEntries) throws IOException {
    LOG.debug("{} Starting backup of {} WAL entries for day {}", Utils.logPeerId(peerId),
      walEntries.size(), day);

    try {
      FSHLogProvider.Writer walWriter = walWriters.computeIfAbsent(day, this::createWalWriter);

      for (WAL.Entry entry : walEntries) {
        walWriter.append(entry);
      }

      walWriter.sync(true);
    } catch (UncheckedIOException e) {
      String errorMsg = Utils.logPeerId(peerId) + " Failed to get or create WAL Writer for " + day;
      LOG.error("{} Backup failed for day {}. Error: {}", Utils.logPeerId(peerId), day,
        e.getMessage(), e);
      throw new IOException(errorMsg, e);
    }

    List<Path> bulkLoadFiles = BulkLoadProcessor.processBulkLoadFiles(walEntries);

    if (LOG.isTraceEnabled()) {
      LOG.trace("{} Processed {} bulk load files for WAL entries", Utils.logPeerId(peerId),
        bulkLoadFiles.size());
      LOG.trace("{} Bulk load files: {}", Utils.logPeerId(peerId),
        bulkLoadFiles.stream().map(Path::toString).collect(Collectors.joining(", ")));
    }

    uploadBulkLoadFiles(day, bulkLoadFiles);
  }

  private FSHLogProvider.Writer createWalWriter(long dayInMillis) {
    String dayDirectoryName = BackupUtils.formatToDateString(dayInMillis);

    FileSystem fs = backupFileSystemManager.getBackupFs();
    Path walsDir = backupFileSystemManager.getWalsDir();

    try {
      // Create a directory for the day
      Path dayDir = new Path(walsDir, dayDirectoryName);
      fs.mkdirs(dayDir);

      // Generate a unique WAL file name
      long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
      String walFileName = WAL_FILE_PREFIX + currentTime + "." + UUID.randomUUID();
      Path walFilePath = new Path(dayDir, walFileName);

      // Initialize the WAL writer
      FSHLogProvider.Writer writer =
        ObjectStoreProtobufWalWriter.class.getDeclaredConstructor().newInstance();
      writer.init(fs, walFilePath, conf, true, WALUtil.getWALBlockSize(conf, fs, walFilePath),
        StreamSlowMonitor.create(conf, walFileName));

      LOG.info("{} WAL writer created: {}", Utils.logPeerId(peerId), walFilePath);
      return writer;
    } catch (Exception e) {
      throw new UncheckedIOException(
        Utils.logPeerId(peerId) + " Failed to initialize WAL Writer for day: " + dayDirectoryName,
        new IOException(e));
    }
  }

  @Override
  public void stop() {
    LOG.info("{} Stopping ContinuousBackupReplicationEndpoint...", Utils.logPeerId(peerId));
    stopAsync();
  }

  @Override
  protected void doStop() {
    close();
    LOG.info("{} ContinuousBackupReplicationEndpoint stopped successfully.",
      Utils.logPeerId(peerId));
    notifyStopped();
  }

  private void close() {
    LOG.info("{} Closing WAL replication component...", Utils.logPeerId(peerId));
    shutdownFlushExecutor();
    lock.lock();
    try {
      flushWriters();
      replicationSource.persistOffsets();
    } catch (IOException e) {
      LOG.error("{} Failed to Flush Open Wal Writers: {}", Utils.logPeerId(peerId), e.getMessage(),
        e);
    } finally {
      lock.unlock();
      LOG.info("{} WAL replication component closed.", Utils.logPeerId(peerId));
    }
  }

  @RestrictedApi(
      explanation = "Package-private for test visibility only. Do not use outside tests.",
      link = "",
      allowedOnPath = "(.*/src/test/.*|.*/org/apache/hadoop/hbase/backup/replication/ContinuousBackupReplicationEndpoint.java)")
  void uploadBulkLoadFiles(long dayInMillis, List<Path> bulkLoadFiles)
    throws BulkLoadUploadException {
    if (bulkLoadFiles.isEmpty()) {
      LOG.debug("{} No bulk load files to upload for {}", Utils.logPeerId(peerId), dayInMillis);
      return;
    }

    LOG.debug("{} Starting upload of {} bulk load files", Utils.logPeerId(peerId),
      bulkLoadFiles.size());

    if (LOG.isTraceEnabled()) {
      LOG.trace("{} Bulk load files to upload: {}", Utils.logPeerId(peerId),
        bulkLoadFiles.stream().map(Path::toString).collect(Collectors.joining(", ")));
    }
    String dayDirectoryName = BackupUtils.formatToDateString(dayInMillis);
    Path bulkloadDir = new Path(backupFileSystemManager.getBulkLoadFilesDir(), dayDirectoryName);
    try {
      backupFileSystemManager.getBackupFs().mkdirs(bulkloadDir);
    } catch (IOException e) {
      throw new BulkLoadUploadException(
        String.format("%s Failed to create bulkload directory in backupFS: %s",
          Utils.logPeerId(peerId), bulkloadDir),
        e);
    }

    for (Path file : bulkLoadFiles) {
      Path sourcePath;
      try {
        sourcePath = getBulkLoadFileStagingPath(file);
      } catch (FileNotFoundException fnfe) {
        throw new BulkLoadUploadException(
          String.format("%s Bulk load file not found: %s", Utils.logPeerId(peerId), file), fnfe);
      } catch (IOException ioe) {
        throw new BulkLoadUploadException(
          String.format("%s Failed to resolve source path for: %s", Utils.logPeerId(peerId), file),
          ioe);
      }

      Path destPath = new Path(bulkloadDir, file);

      try {
        LOG.debug("{} Copying bulk load file from {} to {}", Utils.logPeerId(peerId), sourcePath,
          destPath);

        copyWithCleanup(CommonFSUtils.getRootDirFileSystem(conf), sourcePath,
          backupFileSystemManager.getBackupFs(), destPath, conf);

        LOG.info("{} Bulk load file {} successfully backed up to {}", Utils.logPeerId(peerId), file,
          destPath);
      } catch (IOException e) {
        throw new BulkLoadUploadException(
          String.format("%s Failed to copy bulk load file %s to %s on day %s",
            Utils.logPeerId(peerId), file, destPath, BackupUtils.formatToDateString(dayInMillis)),
          e);
      }
    }

    LOG.debug("{} Completed upload of bulk load files", Utils.logPeerId(peerId));
  }

  /**
   * Copy a file with cleanup logic in case of failure. Always overwrite destination to avoid
   * leaving corrupt partial files.
   */
  @RestrictedApi(
      explanation = "Package-private for test visibility only. Do not use outside tests.",
      link = "",
      allowedOnPath = "(.*/src/test/.*|.*/org/apache/hadoop/hbase/backup/replication/ContinuousBackupReplicationEndpoint.java)")
  static void copyWithCleanup(FileSystem srcFS, Path src, FileSystem dstFS, Path dst,
    Configuration conf) throws IOException {
    try {
      if (dstFS.exists(dst)) {
        FileStatus srcStatus = srcFS.getFileStatus(src);
        FileStatus dstStatus = dstFS.getFileStatus(dst);

        if (srcStatus.getLen() == dstStatus.getLen()) {
          LOG.info("Destination file {} already exists with same length ({}). Skipping copy.", dst,
            dstStatus.getLen());
          return; // Skip upload
        } else {
          LOG.warn(
            "Destination file {} exists but length differs (src={}, dst={}). " + "Overwriting now.",
            dst, srcStatus.getLen(), dstStatus.getLen());
        }
      }

      // Always overwrite in case previous copy left partial data
      FileUtil.copy(srcFS, src, dstFS, dst, false, true, conf);
    } catch (IOException e) {
      try {
        if (dstFS.exists(dst)) {
          dstFS.delete(dst, true);
          LOG.warn("Deleted partial/corrupt destination file {} after copy failure", dst);
        }
      } catch (IOException cleanupEx) {
        LOG.warn("Failed to cleanup destination file {} after copy failure", dst, cleanupEx);
      }
      throw e;
    }
  }

  private Path getBulkLoadFileStagingPath(Path relativePathFromNamespace) throws IOException {
    FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path baseNSDir = new Path(HConstants.BASE_NAMESPACE_DIR);
    Path baseNamespaceDir = new Path(rootDir, baseNSDir);
    Path hFileArchiveDir =
      new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNSDir));

    LOG.debug("{} Searching for bulk load file: {} in paths: {}, {}", Utils.logPeerId(peerId),
      relativePathFromNamespace, baseNamespaceDir, hFileArchiveDir);

    Path result =
      findExistingPath(rootFs, baseNamespaceDir, hFileArchiveDir, relativePathFromNamespace);
    LOG.debug("{} Bulk load file found at {}", Utils.logPeerId(peerId), result);
    return result;
  }

  private static Path findExistingPath(FileSystem rootFs, Path baseNamespaceDir,
    Path hFileArchiveDir, Path filePath) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Checking for bulk load file at: {} and {}", new Path(baseNamespaceDir, filePath),
        new Path(hFileArchiveDir, filePath));
    }

    for (Path candidate : new Path[] { new Path(baseNamespaceDir, filePath),
      new Path(hFileArchiveDir, filePath) }) {
      if (rootFs.exists(candidate)) {
        return candidate;
      }
    }

    throw new FileNotFoundException("Bulk load file not found at either: "
      + new Path(baseNamespaceDir, filePath) + " or " + new Path(hFileArchiveDir, filePath));
  }

  private void shutdownFlushExecutor() {
    if (flushExecutor != null) {
      LOG.info("{} Initiating WAL flush executor shutdown.", Utils.logPeerId(peerId));

      flushExecutor.shutdown();
      try {
        if (
          !flushExecutor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        ) {
          LOG.warn("{} Flush executor did not terminate within timeout, forcing shutdown.",
            Utils.logPeerId(peerId));
          flushExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        flushExecutor.shutdownNow();
        LOG.warn("{} Flush executor shutdown was interrupted.", Utils.logPeerId(peerId), e);
      }
      LOG.info("{} WAL flush thread stopped.", Utils.logPeerId(peerId));
    }
  }
}
