package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The RandomAccessLog provides the random access interface for the transaction
 * log. This class is not thread-safe in general.
 *
 * This class holds one LogWriter instance, which delegates the write operation.
 * And assume there is one thread accessing these write APIs.
 * In addition, if the log file has been finalized for write, the file will be
 * immutable.
 *
 * Also this class maintains a map of LogReader instance, which delegates the
 * read operation. The map is indexed by the session key from the client, and
 * the client will use its own session key to access these read APIs.
 * Concurrent access from different session key is thread safe. But concurrent
 * access from the same session key is NOT thread safe.
 */
@NotThreadSafe
public class RandomAccessLog implements LogFileInterface {

  public static long UNKNOWN_CREATION_TIME = -1;

  private final Logger LOG = LoggerFactory.getLogger(RandomAccessLog.class);

  /** It maps from the index to its starting file offset. */
  private final ConcurrentHashMap<Long, Long> indexToOffsetMap;

  private volatile long firstIndex = Long.MAX_VALUE;

  private volatile long lastIndex = Long.MIN_VALUE;

  /** The actual file */
  private File file;

  /** The LogWriter instance, which delegates the write operation. */
  private final LogWriter writer;

  /** Indicates whether the log has been finalized for write operation. */
  private boolean isFinalized = false;

  /** It maps from the reader session key to the LogReader instance. */
  private final Map<String, LogReader> readerMap = new ConcurrentHashMap<>();

  /** The current term of the log file. Each log file only has one term. */
  private volatile long currentTerm = HConstants.UNDEFINED_TERM_INDEX;

  /** Creation Time */
  private final long creationTime;

  /** only here to support mocking */
  protected RandomAccessLog() {
    file = null;
    writer = null;
    indexToOffsetMap = new ConcurrentHashMap<Long, Long>();
    creationTime = UNKNOWN_CREATION_TIME;
  }

  public RandomAccessLog(File file, boolean isSync) throws IOException {
    this(file, new RandomAccessFile(file, "rw"), isSync);
  }

  public RandomAccessLog(File file, RandomAccessFile raf, boolean isSync)
    throws IOException {
    this.creationTime = populateCreationTime(file);
    this.file = file;
    RandomAccessFile writeRAF = raf;
    this.writer = new LogWriter(writeRAF, isSync);
    this.indexToOffsetMap = new ConcurrentHashMap<Long, Long>();
  }

  public RandomAccessFile getRandomAccessFile() {
    return writer.getRandomAccessFile();
  }

  /**
   * Append the term, index and its transactions into the commit log.
   * If this is the first entry of the log, it will write a file header as well.
   * And return the starting offset for this commit entry
   *
   * @param term
   * @param index
   * @param data
   * @return offset the start offset for this commit entry
   * @throws IOException
   */
  public long append(long term, long index, final ByteBuffer data) throws IOException {
    try {
      // initialize the file
      if (!isInitialized(term)) {
        initialize(term, index);
      }

      // Append transactions and get the offset.
      long offset = this.writer.append(index, data);
      updateIndexMap(index, offset);

      // Return the starting offset for this entry
      return offset;
    } catch (Exception e) {
      // TODO:
      LOG.error("Cannot append to the transaction log ", e);
      throw e;
    }
  }

  private void updateIndexMap(long index, long offset) {
    if (index < this.firstIndex) {
      this.firstIndex = index;
    }

    if (index > this.lastIndex) {
      this.lastIndex = index;
    }

    // Update the index to offset map
    indexToOffsetMap.put(index, offset);
  }

  /**
   * Truncate the log by removing all the entry with larger or the same index.
   *
   * @param index
   * @throws IOException
   */
  public void truncate(long index) throws IOException {
    // Verify the term and index
    Long offset = indexToOffsetMap.get(index);
    if (offset == null) {
      throw new IOException("No such index " + index + "in the current log");
    }

    // Truncate the file
    try {
      this.writer.truncate(offset);
    } catch (IOException e) {
      LOG.error("Cannot truncate to the transaction log ", e);
      throw e;
    }

    // Update the meta data
    removeIndexUpTo(index);
  }

  /**
   * Remove all the indexes which is equal or larger than this index
   * @param index
   */
  private void removeIndexUpTo(long index) {
    if (index == this.firstIndex) {
      // Reset all the meta data
      this.indexToOffsetMap.clear();
      firstIndex = Long.MAX_VALUE;
      lastIndex = Long.MIN_VALUE;
      return;
    }

    // Iterate over the indexToOffsetMap
    for (Long key : this.indexToOffsetMap.keySet()) {
      if (key >= index) {
        this.indexToOffsetMap.remove(key);
      }
    }

    // Update the lastIndex correctly
    this.lastIndex = index - 1;
  }

  /**
   *  getTransactionFileOffset
   *
   *  Get the file offset of a transaction.
   *
   *  @param  term
   *  @param  index
   *  @return long  offset
   */
  public long getTransactionFileOffset(long term, long index) throws NoSuchElementException {
    // Sanity check the term and index
    if (term != this.currentTerm || !this.indexToOffsetMap.containsKey(index)) {
      throw new NoSuchElementException("No such index " + index +
        " and term " + term + " in the current log " + toString());
    }
    return this.indexToOffsetMap.get(index);
  }

  /**
   * Get the transactions for the given term, index and session key.
   *
   * @param term The term of the queried transaction.
   * @param index The index of the quereid transaction.
   * @param sessionKey The session key of the reader.
   * @return transactions
   * @throws IOException if the term does not match with the term of the log,
   * or no such index in the log.
   */
  public MemoryBuffer getTransaction(long term, long index, String sessionKey,
                             final Arena arena)
    throws IOException, NoSuchElementException {
    // Sanity check the term and index
    if (term != this.currentTerm || !this.indexToOffsetMap.containsKey(index)) {
      throw new NoSuchElementException("No such index " + index +
        " and term " + term + " in the current log " + toString());
    }

    // Get the file offset from the map
    long offset = this.indexToOffsetMap.get(index);

    // Get the LogReader for this sessionKey
    LogReader reader = this.getReader(sessionKey);

    // Seek to the offset and read the transaction
    return reader.seekAndRead(offset, index, arena);
  }

  @Override public long getLastModificationTime() {
    return file.lastModified();
  }

  /**
   * @return the absolute path of this log
   */
  public String getFileName() {
    return file.getAbsolutePath();
  }

  /**
   * Finalize the log file. The log file is immutable since then.
   * @throws IOException
   */
  public void finalizeForWrite() throws IOException {
    // Close the writer
    this.writer.close();

    // Mark the file as finalized
    isFinalized = true;
  }

  /**
   * Delete the current log file
   * @throws IOException
   */
  @Override
  public void closeAndDelete() throws IOException {
    try{
      finalizeForWrite();
      removeAllReaders();
    } catch (IOException e) {
      LOG.error("Cannot close to the transaction log ", e);
    } finally {
      file.delete();
    }
  }

  /**
   * @return true if the log file has been finalized
   */
  public boolean isFinalized() {
    return isFinalized;
  }

  /**   *
   * @return the number of entries in the log;
   */
  public long getTxnCount() {
    return this.indexToOffsetMap.isEmpty() ? 0 : (lastIndex - firstIndex + 1);
  }

  /**
   * @return the initial index in the log
   */
  @Override
  public long getInitialIndex() {
    if (indexToOffsetMap.isEmpty()) {
      return HConstants.UNDEFINED_TERM_INDEX;
    }
    return this.firstIndex;
  }

  /**
   * @return the last index in the log
   */
  @Override
  public long getLastIndex() {
    if (indexToOffsetMap.isEmpty()) {
      return HConstants.UNDEFINED_TERM_INDEX;
    }
    return this.lastIndex;
  }

  /**
   * @return the current term of the log file
   */
  public long getCurrentTerm() {
    return currentTerm;
  }

  public File getFile() {
    return file;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RandomAccessLog{");
    sb.append("term=").append(getCurrentTerm())
      .append(", index=[").append(getInitialIndex())
        .append(", ").append(getLastIndex()).append("]")
      .append(", path=").append(getFileName())
      .append(", ntxns=").append(getTxnCount())
      .append(", finalized=").append(isFinalized())
    ;
    return sb.toString();
  }


  /**
   * Remove and close the registered reader from the reader map by the session key
   * @param sessionKey
   */
  public void removeReader(String sessionKey) throws IOException {
    LogReader reader = this.readerMap.remove(sessionKey);
    if (reader != null) {
      reader.close();
    }
  }

  public void removeAllReaders() throws IOException {
    synchronized (this) {
      for (LogReader reader : readerMap.values()) {
        reader.close();
      }
      readerMap.clear();
    }
  }

  /**
   * Rebuilds the in-memory index=>offset map by scanning the file.
   * @param sessionKey
   * @throws IOException
   */
  public void rebuild(final String sessionKey) throws IOException {
    LogReader reader = getReader(sessionKey);
    long index, lastKnownGoodOffset;

    currentTerm = reader.getCurrentTerm();
    if (reader.getVersion() != HConstants.RAFT_LOG_VERSION) {
      throw new IOException("Unable to verify the version.");
    }
    while (reader.hasMore()) {
      index = reader.next();
      if (index == HConstants.UNDEFINED_TERM_INDEX) {
        break;
      }

      updateIndexMap(index, reader.getCurrentIndexFileOffset());
    }

    lastKnownGoodOffset = reader.getCurrentPosition();
    LOG.debug("Resetting the write offset for " + reader.getFile().getAbsoluteFile() +
      " to " + lastKnownGoodOffset);

    // truncate the entries from the last known index
    writer.truncate(lastKnownGoodOffset);

    // Reset the reader position as we are done with the rebuild
    reader.resetPosition();
  }

  /**
   * A utility function to retrieve the log reader by the sessionKey
   * @param sessionKey
   * @return
   * @throws IOException
   */
  private LogReader getReader(String sessionKey) throws IOException {
    LogReader reader = this.readerMap.get(sessionKey);
    if (reader == null) {
      synchronized (this) {
        reader = this.readerMap.get(sessionKey);
        if (reader == null) {
          // Initialize the reader
          reader = new LogReader(this.file);
          reader.initialize();

          // Add the reader to the map
          this.readerMap.put(sessionKey, reader);
        }
      }
    }
    return reader;
  }

  private boolean isInitialized(long term) {
    if (currentTerm == HConstants.UNDEFINED_TERM_INDEX) {
      return false;
    } else if (currentTerm == term) {
      return true;
    } else {
      throw new IllegalArgumentException("Expect the same currentTerm (" + currentTerm
        + " ) for each commit log file. Requested term : " + term);
    }
  }

  private void initialize(long term, long index) throws IOException {
    this.writer.writeFileHeader(term, index);
    // Update the meta data;
    this.currentTerm = term;
  }

  @Override
  public long getFileSize() {
    return file.length();
  }

  @Override
  public String getFileAbsolutePath() {
    return file.getAbsolutePath();
  }

  @Override public long getCreationTime() {
    return creationTime;
  }

  public static long populateCreationTime(final File file) {
    try {
      BasicFileAttributes attributes =
        Files.readAttributes(file.toPath(), BasicFileAttributes.class);
      return attributes.creationTime().toMillis();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return UNKNOWN_CREATION_TIME;
  }
}
