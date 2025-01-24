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
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationResult;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.util.CommonFSUtils;
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

  private static final long ONE_DAY_IN_MILLISECONDS = TimeUnit.DAYS.toMillis(1);
  public static final String WAL_FILE_PREFIX = "wal_file.";
  private static final String DATE_FORMAT = "yyyy-MM-dd";

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.replicationSource = context.getReplicationSource();
    this.peerId = context.getPeerId();
    this.conf = HBaseConfiguration.create(context.getConfiguration());

    initializePeerUUID();
    initializeBackupFileSystemManager();
    startWalFlushExecutor();
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
    } catch (IOException e) {
      LOG.error("{} Error during WAL flush: {}", Utils.logPeerId(peerId), e.getMessage(), e);
    } finally {
      lock.unlock();
    }
  }

  private void flushWriters() throws IOException {
    for (Map.Entry<Long, FSHLogProvider.Writer> entry : walWriters.entrySet()) {
      FSHLogProvider.Writer writer = entry.getValue();
      if (writer != null) {
        writer.close();
      }
    }
    walWriters.clear();
    LOG.debug("{} WAL writers flushed and cleared", Utils.logPeerId(peerId));
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
  public ReplicationResult replicate(ReplicateContext replicateContext) {
    final List<WAL.Entry> entries = replicateContext.getEntries();
    if (entries.isEmpty()) {
      LOG.debug("{} No WAL entries to replicate", Utils.logPeerId(peerId));
      return ReplicationResult.SUBMITTED;
    }

    Map<Long, List<WAL.Entry>> groupedEntries = groupEntriesByDay(entries);
    lock.lock();

    try {
      for (Map.Entry<Long, List<WAL.Entry>> entry : groupedEntries.entrySet()) {
        backupWalEntries(entry.getKey(), entry.getValue());
      }

      if (isAnyWriterFull()) {
        flushWriters();
        return ReplicationResult.COMMITTED;
      }

      return ReplicationResult.SUBMITTED;
    } catch (IOException e) {
      LOG.error("{} Replication failed. Error details: {}", Utils.logPeerId(peerId), e.getMessage(),
        e);
      return ReplicationResult.FAILED;
    } finally {
      lock.unlock();
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
    try {
      FSHLogProvider.Writer walWriter = walWriters.computeIfAbsent(day, this::createWalWriter);
      List<Path> bulkLoadFiles = BulkLoadProcessor.processBulkLoadFiles(walEntries);
      for (WAL.Entry entry : walEntries) {
        walWriter.append(entry);
      }
      walWriter.sync(true);
      uploadBulkLoadFiles(bulkLoadFiles);
    } catch (UncheckedIOException e) {
      String errorMsg = Utils.logPeerId(peerId) + " Failed to get or create WAL Writer for " + day;
      throw new IOException(errorMsg, e);
    }
  }

  private FSHLogProvider.Writer createWalWriter(long dayInMillis) {
    // Convert dayInMillis to "yyyy-MM-dd" format
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    String dayDirectoryName = dateFormat.format(new Date(dayInMillis));

    FileSystem fs = backupFileSystemManager.getBackupFs();
    Path walsDir = backupFileSystemManager.getWalsDir();

    try {
      // Create a directory for the day
      Path dayDir = new Path(walsDir, dayDirectoryName);
      fs.mkdirs(dayDir);

      // Generate a unique WAL file name
      String walFileName = WAL_FILE_PREFIX + dayInMillis + "." + UUID.randomUUID();
      Path walFilePath = new Path(dayDir, walFileName);

      // Initialize the WAL writer
      FSHLogProvider.Writer writer =
        ObjectStoreProtobufWalWriter.class.getDeclaredConstructor().newInstance();
      writer.init(fs, walFilePath, conf, true, WALUtil.getWALBlockSize(conf, fs, walFilePath),
        StreamSlowMonitor.create(conf, walFileName));
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
    shutdownFlushExecutor();
    try {
      flushWriters();
      replicationSource.persistOffsets();
    } catch (IOException e) {
      LOG.error("{} Failed to Flush Open Wal Writers: {}", Utils.logPeerId(peerId), e.getMessage(),
        e);
    }
  }

  private void uploadBulkLoadFiles(List<Path> bulkLoadFiles) throws IOException {
    for (Path file : bulkLoadFiles) {
      Path sourcePath = getBulkLoadFileStagingPath(file);
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

  public Path getBulkLoadFileStagingPath(Path relativePathFromNamespace) throws IOException {
    FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path baseNSDir = new Path(HConstants.BASE_NAMESPACE_DIR);
    Path baseNamespaceDir = new Path(rootDir, baseNSDir);
    Path hFileArchiveDir =
      new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNSDir));

    Path result =
      findExistingPath(rootFs, baseNamespaceDir, hFileArchiveDir, relativePathFromNamespace);
    if (result == null) {
      throw new IOException(
        "No Bulk loaded file found in relative path: " + relativePathFromNamespace);
    }
    return result;
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

  private void shutdownFlushExecutor() {
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
        LOG.warn("{} Flush executor shutdown was interrupted.", Utils.logPeerId(peerId), e);
      }
      LOG.info("{} WAL flush thread stopped.", Utils.logPeerId(peerId));
    }
  }
}
