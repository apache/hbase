package org.apache.hadoop.hbase.backup.replication;

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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import static org.apache.hadoop.hbase.master.cleaner.HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS;

@InterfaceAudience.Private
public class ContinuousBackupStagingManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupStagingManager.class);

  public static final String WALS_BACKUP_STAGING_DIR = "wal-backup-staging";
  public static final String CONF_STAGED_WAL_FLUSH_INITIAL_DELAY = "hbase.backup.staged.wal.flush.initial.delay.seconds";
  public static final int DEFAULT_STAGED_WAL_FLUSH_INITIAL_DELAY_SECONDS = 1 * 60; // 5 minutes
  public static final String CONF_STAGED_WAL_FLUSH_INTERVAL = "hbase.backup.staged.wal.flush.interval.seconds";
  public static final int DEFAULT_STAGED_WAL_FLUSH_INTERVAL_SECONDS = 1 * 60; // 5 minutes

  private final Configuration conf;
  private final FileSystem walStagingFs;
  private final Path walStagingDir;
  private final ConcurrentHashMap<Path, ContinuousBackupWalWriter> walWriterMap = new ConcurrentHashMap<>();
  private final ContinuousBackupManager continuousBackupManager;
  private ScheduledExecutorService flushExecutor;
  private ExecutorService backupExecutor;
  private final Set<Path> filesCurrentlyBeingBackedUp = ConcurrentHashMap.newKeySet();
  private final ReentrantLock lock = new ReentrantLock();
  private final StagedBulkloadFileRegistry stagedBulkloadFileRegistry;

  public ContinuousBackupStagingManager(Configuration conf, ContinuousBackupManager continuousBackupManager)
    throws IOException {
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
    this.stagedBulkloadFileRegistry = new StagedBulkloadFileRegistry(conn,
      continuousBackupManager.getPeerId());
  }

  private void ensureHFileCleanerPluginConfigured() throws IOException {
    String plugins = conf.get(MASTER_HFILE_CLEANER_PLUGINS);
    String cleanerClass = ContinuousBackupStagedHFileCleaner.class.getCanonicalName();
    if (plugins == null || !plugins.contains(cleanerClass)) {
      String errorMsg = Utils.logPeerId(continuousBackupManager.getPeerId()) +
        " Continuous Backup Bulk-loaded HFile's Cleaner plugin is invalid or missing: " + cleanerClass;
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }

  private void initWalStagingDir() throws IOException {
    if (walStagingFs.exists(walStagingDir)) {
      LOG.debug("{} WALs staging directory already exists: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);
    } else {
      walStagingFs.mkdirs(walStagingDir);
      LOG.debug("{} WALs staging directory created: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);
    }
  }

  private void startWalFlushExecutor() {
    int initialDelay = conf.getInt(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY, DEFAULT_STAGED_WAL_FLUSH_INITIAL_DELAY_SECONDS);
    int flushInterval = conf.getInt(CONF_STAGED_WAL_FLUSH_INTERVAL, DEFAULT_STAGED_WAL_FLUSH_INTERVAL_SECONDS);

    flushExecutor = Executors.newSingleThreadScheduledExecutor();
    flushExecutor.scheduleAtFixedRate(this::flushAndBackupSafely, initialDelay, flushInterval, TimeUnit.SECONDS);
  }

  private void flushAndBackupSafely() {
    try {
      LOG.info("{} Periodic WAL flush triggered...", Utils.logPeerId(continuousBackupManager.getPeerId()));
      flushWalFiles();
      backupWalFiles();
    } catch (IOException e) {
      LOG.error("{} Error during periodic WAL flush: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), e.getMessage(), e);
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
      LOG.debug("{} Flushing WAL data for {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);
      closeWriter(writer);
      walWriterMap.put(walDir, createNewContinuousBackupWalWriter(walDir));
    } else {
      LOG.debug("{} No WAL data to flush for {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);
    }
  }

  private void closeWriter(ContinuousBackupWalWriter writer) {
    try {
      writer.close();
    } catch (IOException e) {
      LOG.error("{} Error occurred while closing WAL writer: ", Utils.logPeerId(continuousBackupManager.getPeerId()), e);
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

  public void stageEntries(TableName tableName, List<WAL.Entry> walEntries, List<Path> bulkLoadFiles) throws IOException {
    lock.lock();
    try {
      String namespace = tableName.getNamespaceAsString();
      String table = tableName.getQualifierAsString();

      Path walDir = WALUtils.getWalDir(namespace, table);
      ContinuousBackupWalWriter continuousBackupWalWriter = getContinuousBackupWalWriter(walDir);

      continuousBackupWalWriter.writer(walEntries, bulkLoadFiles);

      // TODO: prevent bulk-loaded files from deleting HFileCleaner thread
      stagedBulkloadFileRegistry.addStagedFiles(bulkLoadFiles);

      LOG.info("{} {} WAL entries staged for table {}:{}",
              Utils.logPeerId(continuousBackupManager.getPeerId()),
              walEntries.size(),
              namespace,
              table);
    } finally {
      lock.unlock();
    }
  }

  private ContinuousBackupWalWriter getContinuousBackupWalWriter(Path walDir) throws IOException {
    try {
      ContinuousBackupWalWriter writer = walWriterMap.computeIfAbsent(walDir, this::createNewContinuousBackupWalWriter);
      if (shouldRollOver(writer)) {
        LOG.debug("{} WAL Writer for {} is being rolled over.", Utils.logPeerId(continuousBackupManager.getPeerId()), walDir);
        closeWriter(writer);
        writer = createNewContinuousBackupWalWriter(walDir);
        walWriterMap.put(walDir, writer);
      }
      return writer;
    } catch (UncheckedIOException e) {
      String errorMsg = Utils.logPeerId(continuousBackupManager.getPeerId()) +
              " Failed to get or create WAL Writer for " + walDir;
      throw new IOException(errorMsg, e);
    }
  }

  private boolean shouldRollOver(ContinuousBackupWalWriter writer) {
    long maxWalSize = conf.getLong(ContinuousBackupManager.CONF_BACKUP_MAX_WAL_SIZE, ContinuousBackupManager.DEFAULT_MAX_WAL_SIZE);
    return writer.getLength() >= maxWalSize;
  }

  public Path getBulkloadFileStagingPath(Path relativePathFromNamespace) throws IOException {
    return WALUtils.getBulkloadFileStagingPath(conf, relativePathFromNamespace);
  }

    public Path getWalFileStagingPath(Path relativeWalPath) {
    return WALUtils.getWalFileStagingPath(walStagingDir, relativeWalPath);
  }

  private void backupWalFiles() throws IOException {
    LOG.info("{} Starting backup of WAL files from staging directory: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);

    RemoteIterator<LocatedFileStatus> fileStatusIterator = walStagingFs.listFiles(walStagingDir, true);
    while (fileStatusIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusIterator.next();
      Path filePath = fileStatus.getPath();
      LOG.trace("{} Processing file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);

      // Skip directories, context files, or files already backed up
      if (fileStatus.isDirectory() || ContinuousBackupWalWriter.isWalWriterContextFile(filePath.getName()) ||
        isFileCurrentlyBeingBackedUp(filePath)) {
        LOG.trace("{} Skipping file (directory/context/backed-up): {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        continue;
      }

      // Check if the file is currently being written
      if (isFileOpenForWriting(filePath)) {
        LOG.info("{} Skipping file as it is currently being written: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        continue;
      }

      // Backup file asynchronously
      backupFileAsync(filePath);
    }

    LOG.info("{} Completed backup process for WAL files from staging directory: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walStagingDir);
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

  public void backupFileAsync(Path filePath) {
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
    LOG.info("{} Starting backup for WAL file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);

    String tableName = filePath.getParent().getName();
    String namespace = filePath.getParent().getParent().getName();
    Path walDir = WALUtils.getWalDir(namespace, tableName);
    String walFileName = filePath.getName();
    Path walFilePath = new Path(walDir, walFileName);
    String walWriterContextFileName = walFileName + ContinuousBackupWalWriter.WAL_WRITER_CONTEXT_FILE_SUFFIX;
    Path walWriterContextFileFullPath = new Path(walStagingDir, new Path(walDir, walWriterContextFileName));

    LOG.debug("{} Resolving bulkload files for WAL writer context: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), walWriterContextFileFullPath);
    List<Path> bulkLoadFiles = ContinuousBackupWalWriter.getBulkloadFilesFromProto(walStagingFs, walWriterContextFileFullPath);

    continuousBackupManager.commitBackup(walStagingFs, walFilePath, bulkLoadFiles);

    stagedBulkloadFileRegistry.removeStagedFiles(bulkLoadFiles);

    LOG.debug("{} Cleaning up WAL and metadata files for WAL: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
    deleteFile(filePath);
    deleteFile(walWriterContextFileFullPath);

    LOG.info("{} Backup completed successfully for WAL: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
  }

  private void deleteFile(Path filePath) {
    try {
      if (walStagingFs.exists(filePath)) {
        if (walStagingFs.delete(filePath, false)) {
          LOG.debug("{} Deleted file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        } else {
          LOG.warn("{} Failed to delete file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath);
        }
      }
    } catch (IOException e) {
      LOG.error("{} Error while deleting file: {}", Utils.logPeerId(continuousBackupManager.getPeerId()), filePath, e);
    }
  }

  // Shutdown method to properly terminate the executors
  public void close() {
    if (flushExecutor != null) {
      flushExecutor.shutdown();
      try {
        if (!flushExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          flushExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        flushExecutor.shutdownNow();
      }
      LOG.info("{} WAL flush thread stopped.", Utils.logPeerId(continuousBackupManager.getPeerId()));
    }

    if (backupExecutor != null) {
      backupExecutor.shutdown();
      try {
        if (!backupExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          backupExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        backupExecutor.shutdownNow();
      }
      LOG.info("{} Backup executor stopped.", Utils.logPeerId(continuousBackupManager.getPeerId()));
    }
  }


  public static class WALUtils {
    public static Path getWalDir(String namespace, String table) {
      return new Path(namespace, table);
    }

    public static Path getWalFileStagingPath(Path walsStagingDir, Path relativeWalPath) {
      return new Path(walsStagingDir, relativeWalPath);
    }

    public static Path getBulkloadFileStagingPath(Configuration conf, Path relativePathFromNamespace) throws IOException {
      Path rootDir = CommonFSUtils.getRootDir(conf);
      FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
      Path baseNamespaceDir = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR));
      Path hFileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNamespaceDir));
      return findExistingPath(rootFs, baseNamespaceDir, hFileArchiveDir, relativePathFromNamespace);
    }

    private static Path findExistingPath(FileSystem rootFs, Path baseNamespaceDir, Path hFileArchiveDir, Path filePath) throws IOException {
      for (Path candidate : new Path[]{new Path(baseNamespaceDir, filePath), new Path(hFileArchiveDir, filePath)}) {
        if (rootFs.exists(candidate)) {
          return candidate;
        }
      }
      return null;
    }
  }
}
