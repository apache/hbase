package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.exceptions.NotEnoughMemoryException;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;
import org.apache.hadoop.hbase.consensus.rpc.LogState;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.io.hfile.bucket.CacheFullException;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.BucketAllocatorException;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the logs for the Consensus Protocol and provides interfaces to
 * perform read/write/truncate operations to the logs.
 * <p/>
 * This is and should be the single point of entry to access any consensus logs.
 * <p/>
 * Layout:
 * <p/>
 * All the logs will be stored in the 'TRANSACTION_LOG_DIRECTORY/quorum-name/'
 * directory. The directory will have 2 folders:
 * 1. /current : contains the current random access log
 * 2. /finalized : contains all the logs  for which the entries have been
 *                 committed to the data-store
 * <p/>
 * Life cycle of a log file:
 * <p/>
 * 1. Every log (apart from recovery) will start in a Random-Access mode in
 * '/current' directory. All the append() operations will be going to this log.
 * The log could be potentially truncated during this phase.
 * 2. Once the size of the log reaches the LOG_ROLL_SIZE and all the entries
 * are committed to the data-store the log will be finalized and moved to the
 * 'finalized' directory. Now the log is available in ReadOnly mode and cannot
 * be truncated, but still can be deleted.
 * 3. Once all the entries from the log are flushed by the data store, the log
 * will be then a part of retiredLogs and will be eventually deleted depending
 * upon the LOG_RETENTION_POLICY.
 *
 * There are two sets of logs maintained by the log manager:
 * 1. uncommittedLogs => log files which reside in the /current dir and contains
 * all the uncommitted edits.
 * 2. committedLogs => logs files which reside in the /finalized dir and contains
 * all the committed edits.
 *
 * * The log roller will always grab the write lock on the logRollerLock before
 * moving the file from /current to /finalized directory.
 *
 * * All the read operations grab the read lock across the entire operation to
 * prevent the log rolling from happening.
 *
 * * append/truncate will always be writing to files which will never be rolled
 * at the same time. Hence, they do not grab any logRollLock.
 *
 * The invariants are:
 * 1. For any given index, there should be only ONE entry in ONE log file.
 * 2. There should never be a gap in index and hence it should always be
 * monotonically increasing.
 *
 * CANDIDATE LOGS
 *
 * In addition there is an optional list of "Candidate Logs" which hold committed
 * transactions and are retrieved from other members during recovery. These logs
 * files are held by candidateLogsManager. On every append/truncate operation,
 * candidateLogsManager will be asked to check if some files it holds can be
 * incorporated into the uncommittedLogs set.
 *
 * Please refer to the comments before incorporateCandidateLogs for the correctness
 * analysis.
 */
public class TransactionLogManager implements CommitLogManagerInterface {

  private final Logger LOG = LoggerFactory.getLogger(
    TransactionLogManager.class);

  private String logDirectory;

  /** Helps debugging. */
  private final String contextName;

  private final Configuration conf;

  private final boolean isSync;
  private volatile boolean isAccessible = true;
  private ImmutableRaftContext context;

  /** RW lock to manage the log roll and read access to the log */
  private final ReentrantReadWriteLock logRollLock;

  /** Log roller thread pool */
  private final static ScheduledExecutorService logRollerThreadPool =
    Executors.newScheduledThreadPool(HConstants.RAFT_LOG_ROLL_POOL_SIZE, new DaemonThreadFactory("logRoller-"));

  /** Log deleter thread pool */
  private final static ScheduledExecutorService logDeleteThreadPool =
    Executors.newScheduledThreadPool(HConstants.RAFT_LOG_ROLL_POOL_SIZE, new DaemonThreadFactory("logDeleter-"));

  // TODO: make it volatile if we want to update it via online configuration change
  /** If current log exceeds this value, roll to a new log */
  private long raftLogRollSize;

  /** last known committed index */
  private volatile long committedIndex;

  /** Seed point for the transaction log manager. The transaction log manager
   * will bootstrap from this index. It will basically create a empty file, which
   * represents the missing indexes.
   *
   * In case any attempt is made to fetch the transaction for index from this
   * empty log file will lead into an error.
   */
  private long seedIndex;

  private long currentFileSize;

  private boolean isCheckInvariants = false;

  /**
   * LogReader may prefetch logs to reduce the latency of reading entries.
   * This specifies how much to prefetch.
   */
  private static int logPrefetchSize;
  /**
   * Map of log files which have been rolled but the entries might not have
   * been committed to the data store.
   */
  ConcurrentSkipListMap<Long, LogFileInterface> uncommittedLogs;

  /**
   * Map of logs for which entries have been committed by the DataStore.
   */
  ConcurrentSkipListMap<Long, LogFileInterface> committedLogs;

  /**
   *  Candidate transaction logs that hold already committed transactions;
   *  they are fetched from other hosts.
   */
  CandidateLogsManager candidateLogsManager;

  /** Log creator for this log manager. */
  TransactionLogCreator logCreator;

  ConcurrentSkipListMap<Long, ByteBuffer> uncommittedTransactions;

  /** Retention time for committed logs */
  private long transactionLogRetentionTime;

  /** Deletion Task */
  private ScheduledFuture logDeletionTask;

  /** Log roll task */
  private ScheduledFuture logRollTask;

  public static enum FindLogType {
    UNCOMMITTED_ONLY, COMMITTED_ONLY, ALL
  }

  public TransactionLogManager(Configuration conf, String contextName, long seedIndex) {
    this.conf = conf;
    String [] logDirectories = conf.get(
        HConstants.RAFT_TRANSACTION_LOG_DIRECTORY_KEY,
        HConstants.DEFAULT_TRANSACTION_LOG_DIRECTORY
    ).split(",");
    int idx = Math.abs(contextName.hashCode()) % logDirectories.length;


    // need to jump down to decide on a logDirectory

    logDirectory = logDirectories[idx];
    if (!this.logDirectory.endsWith(HConstants.PATH_SEPARATOR)) {
      logDirectory = logDirectory + HConstants.PATH_SEPARATOR;
    }

    logDirectory += contextName + "/";

    transactionLogRetentionTime = conf.getLong(
      HConstants.CONSENSUS_TRANCTION_LOG_RETENTION_TIME_KEY,
      HConstants.CONSENSUS_TRANCTION_LOG_RETENTION_TIME_DEFAULT_VALUE);

    isSync = conf.getBoolean(HConstants.RAFT_TRANSACTION_LOG_IS_SYNC_KEY,
        HConstants.RAFT_TRANSACTION_LOG_IS_SYNC_DEFAULT);
    isCheckInvariants = conf.getBoolean("hbase.consensus.log.isCheckInvariants",
      false);
    this.contextName = contextName;

    logRollLock = new ReentrantReadWriteLock();
    uncommittedLogs = new ConcurrentSkipListMap<>();
    committedLogs = new ConcurrentSkipListMap<>();

    raftLogRollSize = conf.getLong(HConstants.RAFT_LOG_ROLL_SIZE_KEY, HConstants.DEFAULT_RAFT_LOG_ROLL_SIZE);

    uncommittedTransactions = new ConcurrentSkipListMap<>();

    long logRollInterval = conf.getInt(
      HConstants.RAFT_LOG_ROLL_INTERVAL_KEY,
      HConstants.DEFAULT_RAFT_LOG_ROLL_INTERVAL);

    logPrefetchSize = conf.getInt(HConstants.RAFT_LOG_READER_PREFETCH_KEY,
        HConstants.DEFAULT_RAFT_LOG_READER_PREFETCH_SIZE);

    logRollTask = logRollerThreadPool.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        rollCommittedLogs();
      }
    }, logRollInterval, logRollInterval, TimeUnit.MILLISECONDS);

    long logDeletionInterval = conf.getInt(
      HConstants.RAFT_LOG_DELETION_INTERVAL_KEY,
      HConstants.DEFAULT_RAFT_LOG_DELETION_INTERVAL);

    logDeletionTask = logDeleteThreadPool.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        deleteOldLogs();
      }
    }, logDeletionInterval, logDeletionInterval, TimeUnit.MILLISECONDS);

    candidateLogsManager = createCandidateLogsManager(conf, contextName,
      logDirectory);

    this.seedIndex = seedIndex;
  }

  public CandidateLogsManager getCandidateLogsManager() {
    return candidateLogsManager;
  }

  @Override
  public String toString() {
    return "@" + hashCode() + "[" + contextName + "]";
  }

  @Override
  public void initialize(final ImmutableRaftContext c) {
    try {
      context = c;
      initializeDirectories();
      scanFinalizedLogs();
      scanCurrentLogs();
      logCreator = createTransactionLogCreator(logDirectory, isSync);
//      scanCandidateLogs();
    } catch (IOException e) {
      isAccessible = false;
      error("Initialization failed. Error ", e);
    }
  }

  /**
   * Will check if there is any gap between the seed index and the latest index
   * present in the logs.
   */
  @Override
  public void fillLogGap(long seedIndex) throws IOException {
    LOG.info("Filling the log gap upto " + seedIndex + " for " + contextName);

    this.seedIndex = seedIndex;

    long latestIndex = this.getLastEditID().getIndex();

    if (latestIndex >= seedIndex) {
      // It might happen that the old logs are deleted and we are just left with
      // the last file, which might not have any entries committed. In that case
      // the committed index is -1, and we can assume that the whatever the data
      // store returns the seedIndex is the minimum committed index
      if (committedIndex < seedIndex) {
        committedIndex = seedIndex;
      }
      return;
    }

    // At this point the seedIndex is greater than the latest index in the
    // transaction logs.
    //
    // We can safely assume that all the entries up to lastValidTransaction are
    // committed successfully.

    // 1. Truncate all the entries beyond lastValidTransaction.
    truncate(getLastValidTransactionId());

    latestIndex = this.getLastEditID().getIndex();

    // 2. Roll the uncommitted logs as all the entries are committed.
    Iterator<Map.Entry<Long, LogFileInterface>> itr =
      uncommittedLogs.entrySet().iterator();

    Map.Entry<Long, LogFileInterface> entry;
    while (itr.hasNext()) {
      entry = itr.next();
      performRollLog((RandomAccessLog)entry.getValue());
    }
    if (!uncommittedLogs.isEmpty()) {
      throw new IOException("Cannot roll all the uncommitted logs");
    }

    // 3. Create a SeedLogFile with range {lastIndex + 1, seedIndex}

    // Create a parse seed file with the range of latestIndex + 1 to seedIndex.
    // The Automatic Log File Fetcher will fetch the entries over time and fill
    // in this gap.
    StringBuilder fileName = new StringBuilder();
    fileName.append(HConstants.SEED_TERM);
    fileName.append("_");
    fileName.append(latestIndex + 1);
    fileName.append("_");
    fileName.append(seedIndex);

    File seedFile = new File(logDirectory +
      HConstants.RAFT_FINALIZED_LOG_DIRECTORY_NAME + HConstants.PATH_SEPARATOR +
      fileName.toString());
    if (!seedFile.createNewFile()) {
      LOG.error("Cannot create a seed file " + fileName);
      throw new IOException("Cannot create a seed file " + fileName);
    }

    if (!seedFile.exists()) {
      LOG.error("Cannot verify the seed file " + fileName);
      throw new IOException("Seed file not present " + fileName);
    }

    seedFile.setLastModified(System.currentTimeMillis());

    // Make the entry for the dummy file
    committedLogs.put(latestIndex + 1, new SeedLogFile(seedFile));

    // Set the committed index to be the seed index
    committedIndex = seedIndex;

    if (LOG.isDebugEnabled()) {
      LOG.error("Created a seed file " + fileName);
    }
  }

  protected void initializeDirectories() throws IOException {
    createLogDirectory(logDirectory);
    createLogDirectory(logDirectory +
      HConstants.RAFT_CURRENT_LOG_DIRECTORY_NAME);
    createLogDirectory(logDirectory +
        HConstants.RAFT_FINALIZED_LOG_DIRECTORY_NAME);
  }

  /**
   *  createLogDirectory
   *
   *  Creates the directory (including the parent dir).
   *
   *  Mockable
   */
  protected void createLogDirectory(String path) throws IOException {
    File currentDirectory = new File (path);

    if (!currentDirectory.exists()) {
      if (!currentDirectory.mkdirs()) {
        throw new IOException("Cannot create dir at " +
          currentDirectory.getAbsolutePath());
      }
    }
  }

  /**
   * Scans the finalized directory for this quorum and updates the maps
   * accordingly.
   * @throws IOException
   */
  private void scanFinalizedLogs() throws IOException {
    List<File> files = null;
    try {
      files = getFinalizedLogsList();
    } catch (IOException x) {
      error("Cannot read the finalized directory. Error ", x);
    }
    if (files == null) {
      return;
    }
    for (File f : files) {
      try {
        LogFileInterface logFile = createReadOnlyLog(f, this.toString());
        long index = logFile.getInitialIndex();
        assert index != HConstants.UNDEFINED_TERM_INDEX;

        committedLogs.put(index, logFile);
      } catch (Exception ex) {
        error("failed to add finalized log " + f.getAbsolutePath(), ex);
      }
    }
  }

  /**
   *  scanCurrentLogs
   *
   *  Scans the logs in the current directory and updates the in-memory maps.
   *  @throws IOException
   */
  private void scanCurrentLogs() throws IOException {
    List<File> files = null;
    try {
      files = getCurrentLogsList();
    } catch (IOException x) {
      error("Cannot read the current directory. Error ", x);
    }
    if (files == null) {
      return;
    }
    for (File f : files) {
      try {
        // Close the files which are not initialized
        RandomAccessLog logFile = createRandomAccessLog(f, isSync);
        try {
          // Rebuild the index by scanning the log file
          logFile.rebuild(toString());
        } catch (IOException e) {
          LOG.warn(this.contextName + " is unable to rebuild the current log file " +
            logFile.getFileName() + ". Deleting it..");
        }

        if (logFile.getInitialIndex() == HConstants.UNDEFINED_TERM_INDEX) {
          logFile.closeAndDelete();
        } else {
          uncommittedLogs.put(logFile.getInitialIndex(), logFile);
        }
      } catch (IOException ex) {
        error("Cannot add current log file " + f.getAbsolutePath(), ex);
      }
    }
    committedIndex = findLastValidTransactionId().getIndex();
  }

  /**
   *  getFinalizedLogsList
   *
   *  Returns a list of files in finalized logs directory.
   *
   *  Mockable
   */
  protected List<File> getFinalizedLogsList() throws IOException {
    Path finalizedDirectory = Paths.get(logDirectory +
      HConstants.RAFT_FINALIZED_LOG_DIRECTORY_NAME);

    DirectoryStream<Path> stream = Files.newDirectoryStream(finalizedDirectory);
    List<File> files = new ArrayList<>();
    for (Path entry: stream) {
      files.add(entry.toFile());
    }
    return files;
  }

  /**
   *
   * getCurrentLogsList, Mockable
   *
   * @return Returns a list of files in the current logs directory.
   * @throws IOException
   */
  protected List<File> getCurrentLogsList() throws IOException {
    Path currentDirectory = Paths.get(logDirectory +
      HConstants.RAFT_CURRENT_LOG_DIRECTORY_NAME);
    // Gets the list of files in the current directory
    DirectoryStream<Path> stream = Files.newDirectoryStream(currentDirectory);
    List<File> files = new ArrayList<>();
    for (Path entry: stream) {
      files.add(entry.toFile());
    }
    return files;
  }

  /**
   * Append the requested entry to the current log
   * @param editId Edit Id to append
   * @param commitIndex the new commit index
   * @param txns transaction for the commit index
   * @return true if successful
   */
  @Override
  public boolean append(final EditId editId, long commitIndex,
                        final ByteBuffer txns) {
    try {
      append(editId.getTerm(), editId.getIndex(), commitIndex, txns);
    } catch (IOException | InterruptedException | IllegalArgumentException e) {
      error("Failed to append " + editId + ". Reason: ", e);
      isAccessible = false;
    }
    return isAccessible;
  }

  /**
   * Tells whether the log is accessible or not.
   * @return true if the log is still accessible
   */
  public boolean isAccessible() {
    return this.isAccessible;
  }

  /**
   * Returns the previous edit id.
   * @param editId return the id previous to the given edit id
   * @return UNDEFINED_EDIT_ID, if no previous edit id
   */
  @Override
  public EditId getPreviousEditID(EditId editId) {

    logRollLock.readLock().lock();
    try {
      long term = getTermInternal(editId.getIndex() - 1);
      if (term == HConstants.UNDEFINED_TERM_INDEX) {
        return UNDEFINED_EDIT_ID;
      }
      return new EditId(term, editId.getIndex() - 1);
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  /**
   *  getNextEditIdTransaction
   *
   *  Returns the next EditId and the transaction associated with it.
   *
   *  @param  sessionKey  String reader's session key
   *  @param  currentIndex   EditId edit id
   *  @param  arena       Arena to use to allocate Memory. If Null is passed it
   *  @return Pair<EditId, MemoryBuffer> the edit id and the corresponding
   *          transaction
   *
   */
  @Override
  public Pair<EditId, MemoryBuffer> getNextEditIdTransaction(
    final String sessionKey,
    final long currentIndex,
    final Arena arena) throws IOException {
    EditId nextId = null;
    LogFileInterface log = null;
    ByteBuffer transaction = null;

    logRollLock.readLock().lock();
    long start = System.nanoTime();
    try {
      long term = getTermInternal(currentIndex + 1);

      // return a valid transaction only. In case, it's an edit in Seed File
      // return null
      if (term >= 0) {
        nextId = new EditId(term, currentIndex + 1);
        if ((transaction =
          uncommittedTransactions.get(nextId.getIndex())) == null) {
          log = getLogFileForEdit(nextId.getIndex());
        }
      }

      if (transaction != null) {
        MemoryBuffer buffer = allocateBuffer(arena, transaction.remaining());

        buffer.getBuffer().put(transaction.array(),
          transaction.arrayOffset() + transaction.position(),
          transaction.remaining());
        buffer.flip();
        return new Pair<>(nextId, buffer);
      }

      if (log == null) {
        return null;
      }

      // At this point, the entry you are reading is supposed to be
      // committed entry

      return new Pair<>(nextId, getTransactionFromLog(log, nextId, sessionKey, arena));
    } finally {
      long end = System.nanoTime();
      logRollLock.readLock().unlock();
      long timeTaken = end - start;
      long threshold = 10000000; // 10 ms
      if (timeTaken > threshold) {
        LOG.debug("Fetching nextEditId took too long. It took {} ns. currentIndex {}", timeTaken, currentIndex);
      }
    }
  }

  /**
   * Returns the last valid transaction from the log.
   *
   * @return the last valid transaction
   * @throws IOException
   */
  @Override
  public EditId getLastValidTransactionId() {
    return getEditId(committedIndex);
  }

  /**
   * It tries to find the latest term for which there are at least two entries
   * committed to the log and returns the second last entry for that term.
   *
   * @return
   */
  private EditId findLastValidTransactionId() {
    logRollLock.readLock().lock();
    try {
      // Check the current log first.
      // Get the latest log for which there are more than 1 entries.
      for (long index : uncommittedLogs.descendingKeySet()) {
        LogFileInterface file = uncommittedLogs.get(index);
        if (file.getTxnCount() > 1) {
          return new EditId(file.getCurrentTerm(), file.getLastIndex() - 1);
        }
      }

      // Since all the entries in committedLogs are already committed to the
      // data store, return the last entry of the latest log file in this
      // list.
      if (!committedLogs.isEmpty()) {
        LogFileInterface file = committedLogs.lastEntry().getValue();
        return new EditId(file.getCurrentTerm(), file.getLastIndex());
      }
      return UNDEFINED_EDIT_ID;
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  /**
   * @return the first index in the log
   * @throws IOException
   */
  @Override
  public long getFirstIndex() {
    logRollLock.readLock().lock();
    try {
      ConcurrentSkipListMap.Entry<Long, ? extends LogFileInterface> entry;
      ConcurrentSkipListMap.Entry<Long, ByteBuffer> txn;

      if ((entry = committedLogs.firstEntry()) != null) {
        return entry.getValue().getInitialIndex();
      } else if ((entry = uncommittedLogs.firstEntry()) != null) {
        return entry.getValue().getInitialIndex();
      } else if ((txn = this.uncommittedTransactions.firstEntry()) != null) {
        return txn.getKey();
      }

      return HConstants.UNDEFINED_TERM_INDEX;
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  /**
   * Checks whether the given transaction is present in the log or not.
   * @param editId
   * @return
   */
  @Override
  public boolean isExist(EditId editId) {
    LogFileInterface log = null;
    logRollLock.readLock().lock();
    try {
      log = getLogFileForEdit(editId.getIndex());
      if (log != null) {
        return log.getCurrentTerm() == editId.getTerm();
      }
      return false;
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  public boolean exists(EditId editId, FindLogType findType) {
    logRollLock.readLock().lock();
    try {
      LogFileInterface log = getLogFileForEdit(editId.getIndex(), findType);
      if (log != null) {
        return log.getCurrentTerm() == editId.getTerm();
      } else {
        return false;
      }
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  /**
   * Truncates all the log entries from the current editId. Currently, this API
   * assumes that there is no interleaving between the append() and truncate()
   * call.
   *
   * There can be a read operation trying to read the same uncommitted transactions
   * which we are trying to truncate. The order is as follows:
   *
   * truncate() -> file.truncate() -> file.delete() -> uncommittedTransactions.delete()
   *
   * read() -> uncommittedTransactions.get() -> file.getTransaction()
   *
   * So, the uncommittedTransactions should be able to satisfy all the queries
   * and the entries will be deleted ONLY after they are deleted from the log file.
   *
   * Returning a transaction which is truncate should be fine.
   *
   * @param editId
   * @return
   * @throws IOException
   */
  @Override
  public boolean truncate(EditId editId) throws IOException {
    final long toBeTruncatedIndex = editId.getIndex() + 1;
    try {
      if (toBeTruncatedIndex <= committedIndex &&
        !UNDEFINED_EDIT_ID.equals(editId)) {
        LOG.error("The entry " + editId +
          " is already committed. Current commit index: " + committedIndex);
        return false;
      }
      // Get the RandomAccessFile which has the edit.
      RandomAccessLog log = getRandomAccessFile(toBeTruncatedIndex);

      if (log == null) {
        return true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Truncate the WAL %s up to: %d",
            log.getFileName(), toBeTruncatedIndex));
      }

      // If the current log has no entries left after truncate or if current
      // log has entries that should be truncated then roll the current log
      log.truncate(toBeTruncatedIndex);

      // Delete the rest of the logs with index >= toBeTruncatedIndex
      Iterator<Map.Entry<Long, LogFileInterface>> filesToDelete =
        uncommittedLogs.tailMap(toBeTruncatedIndex).entrySet().iterator();

      Map.Entry<Long, LogFileInterface> fileEntry;
      while(filesToDelete.hasNext()) {
        fileEntry = filesToDelete.next();
        if (LOG.isDebugEnabled()) {
          debug("Deleting because of truncate, log file " +
            fileEntry.getValue().getFileName());
        }
        filesToDelete.remove();
        fileEntry.getValue().closeAndDelete();
      }

      // Update the uncommitted index cache
      final Iterator<Map.Entry<Long, ByteBuffer>> invalidEdits =
        uncommittedTransactions.tailMap(toBeTruncatedIndex).entrySet().iterator();
      Map.Entry<Long, ByteBuffer> invalidEditEntry;
      while(invalidEdits.hasNext()) {
        invalidEditEntry = invalidEdits.next();
        if (LOG.isDebugEnabled()) {
          debug("Removing " + invalidEditEntry.getKey() + " from the" +
            " uncommitted transactions map. Truncate Index " +
            toBeTruncatedIndex);
        }
        invalidEdits.remove();
      }
    } finally {
      if (isCheckInvariants) {
        checkInvariants();
      }
    }
    return true;
  }

  /**
   * Returns the last edit id successfully written to the log.
   * @return
   */
  @Override
  public EditId getLastEditID() {

    logRollLock.readLock().lock();
    try {
      if (!uncommittedLogs.isEmpty()) {
        LogFileInterface file = uncommittedLogs.lastEntry().getValue();
        return new EditId (file.getCurrentTerm(), file.getLastIndex());
      } else if (!committedLogs.isEmpty()) {
        LogFileInterface file = committedLogs.lastEntry().getValue();
        return new EditId (file.getCurrentTerm(), file.getLastIndex());
      }
      return UNDEFINED_EDIT_ID;
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  @Override
  public LogState getLogState() {
    LogState logState = new LogState(null);

    logState.setLastCommittedEdit(getEditId(committedIndex));

    // Make sure its a consistent view of the logs at this point in time.

    logRollLock.readLock().lock();
    try {
      if (!uncommittedLogs.isEmpty()) {
        for (LogFileInterface file : uncommittedLogs.values()) {
          logState.addUncommittedLogFile(new LogFileInfo(
            file.getFileAbsolutePath(),
            file.getFileSize(),
            new EditId(file.getCurrentTerm(), file.getInitialIndex()),
            new EditId(file.getCurrentTerm(), file.getLastIndex()),
            file.getLastModificationTime(), file.getCreationTime()));
        }
      }

      if (!committedLogs.isEmpty()) {
        for (LogFileInterface file : committedLogs.values()) {
          logState.addCommittedLogFile(new LogFileInfo(
            file.getFileAbsolutePath(),
            file.getFileSize(),
            new EditId(file.getCurrentTerm(), file.getInitialIndex()),
            new EditId(file.getCurrentTerm(), file.getLastIndex()),
            file.getLastModificationTime(), file.getCreationTime()));
        }
      }
    } finally {
      logRollLock.readLock().unlock();
    }

    return logState;
  }

  @Override
  public List<LogFileInfo> getCommittedLogStatus(long minIndex) {
    List<LogFileInfo> logFileInfos = new ArrayList<>();

    for (LogFileInterface rol : committedLogs.values()) {
      if (rol.getLastIndex() > minIndex) {
        logFileInfos.add(new LogFileInfo(
            rol.getFileAbsolutePath(),
            rol.getFileSize(),
            new EditId(rol.getCurrentTerm(), rol.getInitialIndex()),
            new EditId(rol.getCurrentTerm(), rol.getLastIndex()),
            rol.getLastModificationTime(),
            rol.getCreationTime()
        ));
      }
    }

    return logFileInfos;
  }

  public String dumpLogs(int n) {
    if (n <= 0) {
      n = Integer.MAX_VALUE;
    }
    List<String> ids = new ArrayList<>();
    for (EditId id = getLastEditID();
         n > 0 && !id.equals(UNDEFINED_EDIT_ID);
         n--, id = getPreviousEditID(id)) {
      ids.add("" + id.getTerm() + "/" + id.getIndex());
    }
    Collections.reverse(ids);
    return RaftUtil.listToString(ids);
  }

  /**
   * Returns the log file which has the requested edit
   * @param index
   * @return
   */
  private LogFileInterface getLogFileForEdit(final long index, FindLogType findType) {

    LogFileInterface log = null;

    assert logRollLock.getReadHoldCount() > 0;

    if (findType == null) {
      findType = FindLogType.ALL;
    }

    switch (findType) {
      case ALL:
      case UNCOMMITTED_ONLY:
        log = getRandomAccessFile(index);
        break;
    }
    if (log != null) {
      return log;
    }
    switch (findType) {
      case ALL:
      case COMMITTED_ONLY:
        log = getReadOnlyFile(index);
        break;
    }
    return log;
  }

  private LogFileInterface getLogFileForEdit(final long index) {
    return getLogFileForEdit(index, FindLogType.ALL);
  }

  /**
   * Returns the actual transaction by reading it from the log
   * @param log log file to read from
   * @param editId edit id of the transaction
   * @param sessionKey reader's session key
   * @return
   * @throws IOException
   */
  private MemoryBuffer getTransactionFromLog(
    final LogFileInterface log, final EditId editId, final String sessionKey,
    final Arena arena)
    throws IOException {
    try {
      return log.getTransaction(editId.getTerm(),
        editId.getIndex(), sessionKey, arena);
    } catch (NoSuchElementException e) {
      // This is not an IO error. We are not disabling the log manager.
      error("Failed to get transaction " + editId + " from " + log, e);
      // We re-throw this exception as an IOException in order not to
      // change the caller.
      throw new IOException("Failed to get transaction " + editId +
        " from readonly logs", e);
    } catch (NotEnoughMemoryException e) {
      error("Not enough memory to read the transaction.", e);
      throw e;
    } catch (Exception e) {
      error("Failed to get transaction " + editId + " from " + log, e);
      isAccessible = false;
      throw e;
    }
  }

  /**
   * Updates the in-memory map to move the given log file from unrolledLogs to
   * unflushedLogs map.
   * @param log
   */
  private void logRollCompleted(final ReadOnlyLog log) {
    committedLogs.put(log.getInitialIndex(), log);
    uncommittedLogs.remove(log.getInitialIndex());
  }

  /**
   * Performs the append to the log.
   * @param term
   * @param index
   * @param commitIndex
   * @param data
   * @throws IOException
   * @throws InterruptedException
   */
  private void append(long term, long index, long commitIndex,
                      final ByteBuffer data)
    throws IOException, InterruptedException {

    boolean mustCheckInvariants = false;

    try {
      LogFileInterface currentLog;

      if (!uncommittedLogs.isEmpty()) {
        currentLog = uncommittedLogs.lastEntry().getValue();
      } else {
        currentLog = logCreator.getNewLogFile();
      }

      if ((currentLog.getCurrentTerm() != HConstants.UNDEFINED_TERM_INDEX &&
        currentLog.getCurrentTerm() < term) ||
        currentFileSize > raftLogRollSize) {
        try {
          mustCheckInvariants = true;
          debug(String.format("Moving away from current log %s for quorum %s. new edit " +
              "{term=%d, index=%d}, current edit {term=%d,index=%d}",
              currentLog.getFileName(), contextName,
              term, index, currentLog.getCurrentTerm(), currentLog.getLastIndex()));
          currentLog = rollCurrentLog(currentLog);
          debug("The new log file is " + currentLog.getFileAbsolutePath());
          currentFileSize = 0;
        } catch (InterruptedException e) {
          error("Unable to roll log " + currentLog.getFileName(), e);
          isAccessible = false;
          throw e;
        }
      }

      uncommittedTransactions.put(index, data);

      // Append the current edit.
      currentFileSize = ((RandomAccessLog)currentLog).append(term, index, data);

      // If its a new log, add it to the map after a successful append
      if (currentLog.getTxnCount() == 1) {
        uncommittedLogs.put(index, currentLog);
      }

      committedIndex = commitIndex;

      // At this point, all the edits upto the committed index should be synced
      // to disk. Adding TODO to track this!

      final Iterator<Map.Entry<Long, ByteBuffer>> newlyCommittedEdits =
        uncommittedTransactions.headMap(committedIndex, true).entrySet().iterator();
      while(newlyCommittedEdits.hasNext()) {
        newlyCommittedEdits.next();
        newlyCommittedEdits.remove();
      }
    } finally {
      if (isCheckInvariants && mustCheckInvariants) {
        checkInvariants();
      }
    }
  }

  /**
   * Rolls the current committed log.
   * @param file file to roll the log
   * @return
   */
  private boolean performRollLog(final RandomAccessLog file) {

    try {
      info("Performing log roll for " + file.getFileName());

      long initialIndex = file.getInitialIndex();

      String newName =
        file.getCurrentTerm() + "_" + initialIndex + "_" + file.getLastIndex();

      File renamedFile = new File(logDirectory +
        HConstants.RAFT_FINALIZED_LOG_DIRECTORY_NAME + HConstants.PATH_SEPARATOR +
        newName);

      // There can be instances where the roll request is queued multiple times.
      // Lets check that if the rolled file exists then just return.
      if (renamedFile.exists()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("File " + file.getFileName() + " is already rolled to the " +
            "new file " + renamedFile.getName());
        }
        return true;
      }

      try {
        file.finalizeForWrite();
      } catch (IOException e) {
        error("Unable to finalize the " + file.getFileName() +
          " for roll.", e);
        isAccessible = false;
        return false;
      }

      assert file.isFinalized() == true;

      logRollLock.writeLock().lock();

      if (!isAccessible()) {
        error("Log is not accessible, returning.");
        return false;
      }

      // Close all the reader sessions
      try {
        file.removeAllReaders();
      } catch (IOException e) {
        error("Unable to close the readers for " + file.getFileName() +
          " for roll.", e);
        return false;
      }

      if (!renamedFile.exists() && !renameFile(file.getFile(), renamedFile)) {
        LOG.warn("Cannot rename the log " + file.getFileName() + " to " +
          renamedFile.getAbsoluteFile().getName() + ". Aborting.");
        return false;
      }

      logRollCompleted(
        createReadOnlyLog(renamedFile, file.getCurrentTerm(), initialIndex, file.getLastIndex()));

      info("Log roll for " + file.getFileName() + " completed successfully." +
        " The new file created is at " + renamedFile.getAbsoluteFile().getName());
      return true;
    } finally {
      logRollLock.writeLock().unlock();
    }
  }

  /**
   * Rolls the current RandomAccessLog in case it reached the size limit or
   * there was a change in term.
   *
   * @return The new log file
   *
   * @throws InterruptedException
   */

  private LogFileInterface rollCurrentLog(final LogFileInterface currentLog)
    throws InterruptedException {

    LogFileInterface oldLog = currentLog;
    LogFileInterface newLog;
    try {
      newLog = logCreator.getNewLogFile();
    } catch (InterruptedException e) {
      error("Unable to get a new log file.", e);
      isAccessible = false;
      throw e;
    }

    assert newLog != null;
    uncommittedLogs.put(oldLog.getInitialIndex(), oldLog);
    // we don't check invariants here because checkInvariants is only
    // called by append.
    return newLog;
  }

  /**
   * For unit test only!
   */
  protected void forceRollLog() throws InterruptedException {
    try {
      logCreator.getNewLogFile();
    } catch (InterruptedException e) {
      error("Unable to get a new log file.", e);
      throw e;
    }
  }

  /**
   * Depending upon the committedIndex, get the list of logs which can now be
   * rolled and submit jobs to roll them. Returns the number of logs rolled.
   *
   * For the read operations, the log manager also takes logRollLock read lock
   * and for roll the Log Roller will take the write lock. Hence
   * roll/read operation file cannot happen simultaneously.
   *
   * The log manager grabs the writeLock before making any updates to the file
   * maps. So after the log roll happens it will also have to grab the write lock
   * before updating the file maps. This prevents the race conditions between
   * the log roller and file location lookup operation done by the transaction
   * log manager.
   *
   */
  protected int rollCommittedLogs() {
    int nRolled = 0;
    Long initialIndexOfLastUncommittedLog =
      uncommittedLogs.floorKey(committedIndex);

    if (initialIndexOfLastUncommittedLog != null) {
      Iterator<Map.Entry<Long, LogFileInterface>> iter =
        uncommittedLogs.headMap(
          initialIndexOfLastUncommittedLog).entrySet().iterator();
      while (iter.hasNext()) {
        final LogFileInterface log = iter.next().getValue();
        assert committedIndex >= log.getLastIndex();
        nRolled ++;
        debug("Rolling log file " + log.getFileName());
        if (!performRollLog((RandomAccessLog)log)) {
          debug("Log roll failed for " + log);
        } else {
          debug("Log roll succeeded for " + log);
        }
      }
    }
    return nRolled;
    // no checking of invariants here because we check in the
    // caller append
  }

  /**
   * Deletes the old committed logs.
   */
  protected void deleteOldLogs() {

    if (context == null | !isAccessible()) {
      return;
    }

    long deleteIndex = context.getPurgeIndex();
    long currentTimeInMillis = System.currentTimeMillis();
    Iterator<Map.Entry<Long, LogFileInterface>> iter =
      committedLogs.headMap(deleteIndex).entrySet().iterator();

    LogFileInterface log;
    while (iter.hasNext()) {
      Map.Entry<Long, LogFileInterface> entry = iter.next();
      log = entry.getValue();

      if (deleteIndex >= log.getLastIndex() &&
        (currentTimeInMillis - log.getLastModificationTime()) >
          transactionLogRetentionTime) {
        LOG.info("Deleting log file " + entry.getValue().getFileName() +
          ", term= " + log.getCurrentTerm() + ", startIndex=" +
          log.getInitialIndex() + ", lastIndex=" +
          log.getLastIndex() + " deleteIndex=" + deleteIndex + " mod time " +
          log.getLastModificationTime());
        logRollLock.readLock().lock();
        try {
          iter.remove();
          log.closeAndDelete();
        } catch (IOException e) {
          LOG.error("Cannot delete log file " + entry.getValue(), e);
        } finally {
          logRollLock.readLock().unlock();
        }
      }
    }
  }

  /**
   * Get the EditId from the log for the given index
   * @param index
   * @return the EditId from the log for the given index;
   *         The term will be set to -1 in case there is no entry in the log.
   */
  @Override
  public EditId getEditId(final long index) {
    logRollLock.readLock().lock();
    try {
      return new EditId(getTermInternal(index), index);
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  /**
   * Returns the term for the given index, assuming the caller has acquired the
   * read lock of logRollLock.
   * @param index
   * @return
   */
  private long getTermInternal(final long index) {
    long term = HConstants.UNDEFINED_TERM_INDEX;
    LogFileInterface log = getLogFileForEdit(index);
    if (log != null) {
      term = log.getCurrentTerm();
    }

    return term;
  }

  /**
   * Goes through all the ReadOnlyFiles and returns the file which should have
   * the edit.
   *
   * @param index
   * @return ReadOnlyLog file if edit id exists, else null;
   */
  private ReadOnlyLog getReadOnlyFile(long index) {

    LogFileInterface file = null;
    long oldestIndex = HConstants.UNDEFINED_TERM_INDEX;
    long newestIndex = HConstants.UNDEFINED_TERM_INDEX;

    // Asking for a very old index, which has been already rolled and committed
    if (committedLogs.firstEntry() != null) {
      oldestIndex = committedLogs.firstKey();
      newestIndex = committedLogs.lastEntry().getValue().getLastIndex();
    }

    if (oldestIndex == HConstants.UNDEFINED_TERM_INDEX ||
      newestIndex == HConstants.UNDEFINED_TERM_INDEX) {
      return (ReadOnlyLog)file;
    }

    if (index >= oldestIndex && index <= newestIndex) {
      file = committedLogs.floorEntry(index).getValue();
    }
    return (ReadOnlyLog)file;
  }

  public static MemoryBuffer allocateBuffer(final Arena arena, final int size) {
    MemoryBuffer buffer;
    try {
      if (arena != null) {
        buffer = arena.allocateByteBuffer(size);
      } else {
        buffer = new MemoryBuffer(ByteBuffer.allocate(size));
      }
    } catch (CacheFullException | BucketAllocatorException e) {
      buffer = new MemoryBuffer(ByteBuffer.allocate(size));
    }
    return buffer;
  }

  /**
   * Goes through all the RandomAccessFiles and returns the file which should
   * have the edit.
   *
   * This assumes that the caller as held the read lock.
   *
   * @param index
   * @return RandomAccessLog file if edit id exists, else null;
   */
  private RandomAccessLog getRandomAccessFile(long index) {

    long oldestIndex = HConstants.UNDEFINED_TERM_INDEX;
    long newestIndex = HConstants.UNDEFINED_TERM_INDEX;
    LogFileInterface file = null;

    // Get oldest index
    if (!uncommittedLogs.isEmpty()) {
      oldestIndex = uncommittedLogs.firstKey();
      newestIndex = uncommittedLogs.lastEntry().getValue().getLastIndex();
    }

    if (oldestIndex == HConstants.UNDEFINED_TERM_INDEX ||
      newestIndex == HConstants.UNDEFINED_TERM_INDEX) {
      return (RandomAccessLog)file;
    }

    if (index >= oldestIndex && index <= newestIndex) {
      file = uncommittedLogs.floorEntry(index).getValue();
    }

    return (RandomAccessLog)file;
  }

  public long getSeedIndex() {
    return seedIndex;
  }

  private void debug(String str) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(this.toString() + ":" + str);
    }
  }

  private void info(String str) {
    LOG.info(this.toString() + ":" + str);
  }

  private void error(String str) {
    LOG.error(this.toString() + ":" + str);
  }

  private void error(String str, Throwable stone) {
    LOG.error(this.toString() + ":" + str, stone);
  }

  private void warn(String str) {
    LOG.warn(this.toString() + ":" + str);
  }

  private void warn(String str, Throwable stone) {
    LOG.warn(this.toString() + ":" + str, stone);
  }

  private <T> Long validate(String name, ConcurrentSkipListMap<Long, T> logs,
                            Long expectedNextIndex) throws IOException {
    for (Long key : logs.descendingKeySet()) {
      LogFileInterface log = (LogFileInterface) logs.get(key);
      if (log.getLastIndex() < log.getInitialIndex()) {
        throw new IOException(
            "Incorrect index range ["
            + log.getInitialIndex() + ", " + log.getLastIndex()
            + "] found in " + name + " log " + log
        );
      }
      if (expectedNextIndex != null && expectedNextIndex != log.getLastIndex()) {
        throw new IOException(
            "A gap is found between " + expectedNextIndex + " and the " + name +
              " log " + log + " [" + log.getInitialIndex() + ", "
              + log.getLastIndex()+ "]"
        );
      }
      expectedNextIndex = log.getInitialIndex() - 1;
    }
    return expectedNextIndex;
  }

  public void checkInvariants() throws IOException {
    // we assume we have the write lock
    long t0 = System.nanoTime();

    logRollLock.readLock().lock();
    try {
      Long expectedNextIndex = null;

      if (!uncommittedTransactions.isEmpty() &&
        uncommittedTransactions.firstKey() <= committedIndex) {
        throw new IOException("Stale committed edit left in uncommitted list." +
          " First Key: " + uncommittedLogs.firstKey() + ", committed index: " +
        committedIndex);
      }

      expectedNextIndex = validate("uncommitted", uncommittedLogs, expectedNextIndex);
      expectedNextIndex = validate("committed", committedLogs, expectedNextIndex);
    } finally {
      logRollLock.readLock().unlock();
      debug("checkInvariants() took " + ((System.nanoTime() - t0)/1000) + " us");
    }
  }

  @Override
  public String getPath() {
    return this.logDirectory;
  }

  public void scanCandidateLogs() throws IOException {
    candidateLogsManager.scan();
  }

  /**
   *  greedyIncorporateCandidateLogs
   *
   *  @param    sessionKey            String
   *  @return   Pair<EditId, EditId>  The pivot Id and the last EditId in the
   *                                  promoted candidate logs
   *
   *  We try to keep as much of uncommitted logs intact possible by finding a
   *  usable pivot log from the most recent logs. We will stop incorporating any logs
   *  beyond lastLogIndex if it's set.
   */
  public Pair<EditId, EditId> greedyIncorporateCandidateLogs(
      String    sessionKey
  ) {
    return greedyIncorporateCandidateLogs(sessionKey, Long.MAX_VALUE);
  }

  public Pair<EditId, EditId> greedyIncorporateCandidateLogs(
      String    sessionKey,
      long      lastLogIndex
  ) {
    EditId pivotLogId = null;
    pivotLogId = getLastValidTransactionId();

    if (pivotLogId != null && pivotLogId != UNDEFINED_EDIT_ID) {
      return new Pair<EditId, EditId>(pivotLogId, incorporateCandidateLogs(sessionKey, pivotLogId, lastLogIndex));
    }
    return null;
  }

  /**
   *  incorporateCandidateLogs
   *
   *
   *  @param    sessionKey    String
   *  @param    pivotLogId    EditId
   *  @param    lastLogIndex  long
   *
   *  @return   EditId      the last EditId in the promoted candidate logs
   *
   *  WARNING:
   *
   *  We must be very cautious here because a false leader may send AppendRequest
   *  that triggers incorporateCandidateLogs().
   *
   *  Pre-conditions
   *  P1. A set of contiguous candidate logs S with index range [S_1, S_2] contain
   *      all valid RAFT transactions in the range [S_1, S_2]. These transactions
   *      may not be committed yet.
   *  P2. The local logs state is valid with respect to the quorum up to a certain
   *      head subset, which may be empty, of uncommitted logs.
   *
   *  Input
   *      pivotLogId <= last valid transaction Id
   *
   *  Incorporation Criteria
   *  We only incorporate a set of contiguous candidate logs S with index range
   *  [A, B] iff
   *  C1. The pivotLogId is found in both uncommittedLogs and S.
   *
   *  What this means is that
   *    1) uncommitedLogs.headSet(pivotLogId) is valid
   *    2) uncommitedLogs.headSet(pivotLogId) + S is valid
   *
   *  Procedure
   *  1. We find a set of contiguous candidate logs that cover the pivot ID as well
   *     as pivotID.next().
   *
   *  2. We truncate the head log at pivotId.next() and the existing uncommitted logs
   *     up to and including pivotId.next().
   *
   *  3. We then merge the set of candidate logs with existing uncommitted logs.
   */
  private EditId incorporateCandidateLogs(
      String    sessionKey,
      EditId    pivotLogId,
      long      lastLogIndex
  ) {
    if (candidateLogsManager.isEmpty()) {
      return null;
    }
    String whoami = "incorporateCandidateLogs("
      + sessionKey
      + ", pivot=" + pivotLogId
      + ") ";

    // a quick test
    if (!exists(pivotLogId, FindLogType.UNCOMMITTED_ONLY)) {
      warn(whoami + " failed to locate pivotLogId " + pivotLogId + " in local logs");
      return null;
    }

    if (!candidateLogsManager.exists(pivotLogId)) {
      warn(whoami + " failed to locate pivotLogId " + pivotLogId + " in candidate logs");
      return null;
    }

    // This variable records the last edit id of the most recent promotion
    EditId lastIncorporatedEditId = null;

    // In future we may want to change the locking discipline to reduce the amount of time
    // spent in the critical section.
    logRollLock.writeLock().lock();
    try {
      // Checking criterion C1
      if (!exists(pivotLogId, FindLogType.UNCOMMITTED_ONLY)) {
        warn(whoami + " failed to locate pivotLogId " + pivotLogId);
        return null;
      }

      List<LogFileInfo> candidates = candidateLogsManager.getContiguousLogsContaining(pivotLogId, lastLogIndex);
      if (candidates == null || candidates.isEmpty()) {
        debug(whoami + " found no suitable candidate logs containing [" + pivotLogId + ", " + lastLogIndex + "]");
        return null;
      }

      // sanity-check the list
      for (int i=1; i<candidates.size(); i++) {
        if (candidates.get(i).getInitialIndex() != candidates.get(i-1).getLastIndex() + 1) {
          error(whoami + " found a gap or overlap between candidates " + candidates.get(i-1) + " and " + candidates.get(i));
          return null;
        }
      }

      boolean foundContainingLog = false;
loop_over_candidates:
      for (LogFileInfo candidateInfo : candidates) {
        RandomAccessLog candidateLog = createRandomAccessLog(new File(candidateInfo.getAbsolutePath()), isSync);
        debug(whoami + " locking file " + candidateLog.getFile().getAbsolutePath());
        FileLock filelock = lockRandomAccessLog(candidateLog);
        try {
          RandomAccessLog targetLog = candidateLog;

          // check the modification time
          if (getModificationTime(candidateLog.getFile()) != candidateInfo.getLastVerifiedModificationTime()) {
            error(whoami + " found file " + candidateLog.getFile().getAbsolutePath()
                + " modified since " + candidateInfo.getLastVerifiedModificationTime()
                + "; the lastest modification time is " +
                getModificationTime(candidateLog.getFile()));
            return lastIncorporatedEditId;
          }

          // the file is already locked; it's safe to rebuild
          candidateLog.rebuild(toString());

          if (candidateLog.getInitialIndex() != candidateInfo.getInitialIndex()) {
            error("InitialIndex mismatch between RandomAccessLog " + candidateLog
                + " and LogFileInfo " + candidateInfo);
            return lastIncorporatedEditId;
          } else if (candidateLog.getLastIndex() != candidateInfo.getLastIndex()) {
            error("LastIndex mismatch between RandomAccessLog " + candidateLog
                + " and LogFileInfo " + candidateInfo);
            return lastIncorporatedEditId;
          }

          // again, sanity check
          if (candidateLog.getLastIndex() <= pivotLogId.getIndex()) {
            candidateLogsManager.removeFromCollection(candidateInfo);
            continue loop_over_candidates;
          } else if (candidateLog.getInitialIndex() > pivotLogId.getIndex()) {
            if (!foundContainingLog) {
              error(whoami + " failed to find a candidateInfo log containing " + pivotLogId + " before " + candidateInfo);
              return null;
            }
            // else fall through
          } else {
            // FACT: candidateLog.getInitialIndex() <= pivotLogId.getIndex() < candidateLog.getLastIndex()

            if (foundContainingLog) {
              // this should never ever happen because we have already checked the list.
              // but it doesn't hurt to check here.
              error(whoami + " found more than one candidates containing " + pivotLogId);
              return null;
            }
            try {
              MemoryBuffer buffer = candidateLog.getTransaction(
                pivotLogId.getTerm(), pivotLogId.getIndex(), sessionKey, null);
            } catch (BufferOverflowException ex) {
              // it's OK; we should really let the reader go through the entire message to ascertain
              // its integrity. Perhaps LogReader.seekAndRead() can handle null buffer.
            } catch (NoSuchElementException ex) {
              error(whoami + " failed to find pivotLogId " + pivotLogId + " in " + candidateLog);
              return null;
            } catch (IOException ex) {
              error(whoami + " failed to find pivotLogId " + pivotLogId + " in " + candidateLog);
              return null;
            }

            foundContainingLog = true;

            Pair<EditId, MemoryBuffer> pair = getNextEditIdTransaction(sessionKey,
              pivotLogId.getIndex(), null);
            if (pair == null || pair.getFirst() == null) {
              error("Failed to find the next edit of the pivot " + pivotLogId);
              return null;
            }

            // We use a very strict criterion
            EditId nextLogId = pair.getFirst();
            if (candidateLog.getLastIndex() < nextLogId.getIndex() || candidateLog.getCurrentTerm() != nextLogId.getTerm()) {
              warn(whoami + " The edit ID after the pivot (" + pivotLogId + " -> " + nextLogId
                  + " does not exist in the same log file " + candidateLog);
              return null;
            }
            if (!nextLogId.equals(getLastEditID())) {
              debug(whoami + " the nextLogId " + nextLogId + " is not the last one; truncating logs up to " + nextLogId);
              if (!truncate(nextLogId)) {
                error(whoami + " failed to truncate @ " + nextLogId);
                return null;
              }
            }

            // we may have to truncate this file
            if (candidateLog.getInitialIndex() < nextLogId.getIndex()) {
              long newInitialIndex = nextLogId.getIndex()+1;
              info(whoami + " We have to head-truncate " + candidateLog + " @ " + newInitialIndex);
              File newFile = new File(candidateInfo.getAbsolutePath() + ".subcopy_to_"
                  + candidateLog.getCurrentTerm() + "_" + newInitialIndex + "_" + candidateLog.getLastIndex());

              RandomAccessLog newLog = candidateLogsManager.subCopyRandomAccessLog(
                  candidateLog, newInitialIndex, candidateLog.getLastIndex(), newFile);
              if (newLog == null) {
                warn(whoami + " Failed to subcopy " + candidateLog + " to " + newFile.getAbsolutePath());
                return null;
              }
              newLog.rebuild(toString());
              targetLog = newLog;
            }
          }

          // the big job
          // candidateLogsManager.removeFromCollection(candidateInfo);

          // TODO: use the appropriate naming scheme
          String newName = getCanidatePromotionFilename(
              targetLog.getFile().getName(),
              targetLog.getCurrentTerm(),
              targetLog.getInitialIndex(),
              targetLog.getLastIndex()
          );
          File newFile = new File(logDirectory + HConstants.RAFT_CURRENT_LOG_DIRECTORY_NAME + "/" + newName);
          debug(whoami + " moving " + targetLog.getFile().getAbsolutePath() + " to " + newFile.getAbsolutePath());
          if (!renameFile(targetLog.getFile(), newFile)) {
            error(whoami + " failed to move " + targetLog.getFile().getAbsolutePath() + " to " + newFile.getAbsolutePath());
            return lastIncorporatedEditId;
          }

          // candidateLog.getFile() should point to the new path now
          info(whoami + " moved " + targetLog.getFile().getAbsolutePath() + " to " + newFile.getAbsolutePath());

          uncommittedLogs.put(targetLog.getInitialIndex(), targetLog);
          lastIncorporatedEditId = new EditId(targetLog.getCurrentTerm(), targetLog.getLastIndex());
          targetLog = null;
        } finally {
          unlockRandomAccessLog(filelock);
        }
      }
    } catch (IOException ex) {
      error(whoami + " caught an exception", ex);
    } finally {
      logRollLock.writeLock().unlock();
      if (lastIncorporatedEditId != null) {
        try {
          checkInvariants();
        } catch (IOException ex) {
          error(whoami + " failed to check invariants after incorporating candidate logs up till " + lastIncorporatedEditId, ex);
          isAccessible = false;
        }
        candidateLogsManager.prune(lastIncorporatedEditId.getIndex());
      }
    }
    return lastIncorporatedEditId;
  }

  protected String getCanidatePromotionFilename(
      final String      oldFilename,
      final long        term,
      final long        initialIndex,
      final long        lastIndex
  ) {
    return oldFilename + ".promoted_to_" + term + "_" + initialIndex + "_" + lastIndex;
  }

  /*  lockRandomAccessLog
   *
   *  @param  log     RandomAccessLog
   *  @return         FileLock
   *
   *  Mockable
   */
  protected FileLock lockRandomAccessLog(RandomAccessLog logFile) throws IOException {
    RandomAccessFile raf = logFile.getRandomAccessFile();
    return raf.getChannel().lock();
  }

  /**
   *  unlockRandomAccessLog
   *
   *  @param  lock    FileLock
   *
   *  Mockable
   */
  protected void unlockRandomAccessLog(FileLock lock) throws IOException {
    lock.release();
  }

  /**
   *  getModificationTime
   *
   *  Mockable
   */
  protected long getModificationTime(File file) {
    return file.lastModified();
  }

  /**
   *  renameFile
   *
   *  Mockable
   */
  protected boolean renameFile(File oldFile, File newFile) {
    return oldFile.renameTo(newFile);
  }

  /**
   *  createReadOnlyLog
   *
   *  Mockable
   */
  protected ReadOnlyLog createReadOnlyLog(File f, long t, long initialIndex, long lastIndex) {
    return new ReadOnlyLog(f, t, initialIndex);
  }

  /**
   *  createReadOnlyLog
   *
   *  Mockable
   */
  protected ReadOnlyLog createReadOnlyLog(File f, String sessionKey) throws IOException {
    if (SeedLogFile.isSeedFile(f)) {
      return new SeedLogFile(f);
    } else {
      return new ReadOnlyLog(f, sessionKey);
    }
  }

  /**
   *  createRandomAccessLog
   *
   *  Mockable
   */
  protected RandomAccessLog createRandomAccessLog(File f, boolean isSync) throws IOException {
    return new RandomAccessLog(f, isSync);
  }

  /**
   *  createTransactionLogCreator
   *
   *  Mockable
   */
  protected TransactionLogCreator createTransactionLogCreator(
      String            logDirectory,
      boolean           isSync
  ) {
    return new TransactionLogCreator(logDirectory, isSync, conf);
  }

  /**
   * createCandidateLogsManager
   *
   * Mockable
   */
  protected CandidateLogsManager createCandidateLogsManager(
      Configuration     conf,
      String            contextName,
      String            logsDirectory
  ) {
    return new CandidateLogsManager(conf, contextName, logDirectory);
  }

  public void setRaftLogRollSize(long size) {
    raftLogRollSize = size;
  }

  public void stop() {
    logRollLock.readLock().lock();
    try {
      isAccessible = false;
      logRollTask.cancel(true);
      logDeletionTask.cancel(true);
    } finally {
      logRollLock.readLock().unlock();
    }
  }

  public static int getLogPrefetchSize() {
    return logPrefetchSize;
  }

  /**
   * Unit test only
   */
  public ConcurrentSkipListMap<Long, LogFileInterface> getUncommittedLogs() {
    return uncommittedLogs;
  }
}
