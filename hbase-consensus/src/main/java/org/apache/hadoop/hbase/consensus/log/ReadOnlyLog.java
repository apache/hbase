package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ReadOnly log provides read interface for the transaction log.
 *
 * Also this class maintains a map of LogReader instance, which delegates the read operation.
 * The map is indexed by the session key from the client, and the client will use
 * its own session key to access these read APIs. Concurrent access from different session key is
 * thread safe. But concurrent access from the same session key is NOT thread safe.
 *
 * The naming convention will be "log_<term>_<initial-index>_<last-index>"
 */
public class ReadOnlyLog implements LogFileInterface {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyLog.class);

  protected final long initialIndex;
  protected final long lastIndex;
  protected final long currentTerm;
  protected final File file;
  protected final long creationTime;

  private final Map<String, LogReader> readerMap = new ConcurrentHashMap<>();

  public ReadOnlyLog(File file, long term, long initIndex) {
    this.creationTime = RandomAccessLog.populateCreationTime(file);
    this.file = file;
    this.currentTerm = term;
    this.initialIndex = initIndex;
    this.lastIndex = Long.parseLong(file.getName().split("_")[2]);
  }

  public ReadOnlyLog(File file, String sessionKey) throws IOException {
    this.creationTime = RandomAccessLog.populateCreationTime(file);

    this.file = file;

    // Get the LogReader
    LogReader reader = getReader(sessionKey);

    // Set up the invariants
    this.currentTerm = reader.getCurrentTerm();
    this.initialIndex = reader.getInitialIndex();
    this.lastIndex = Long.parseLong(file.getName().split("_")[2]);
  }

  /**
   * Read the transactions for the given index
   *
   * @param index The query index
   * @param sessionKey The session key of the log reader
   * @return transactions of the given index
   * @throws IOException if the term does not match up or the index is not found.
   */
  public MemoryBuffer getTransaction(long term, long index, String sessionKey,
                                     final Arena arena)
    throws IOException, NoSuchElementException {
    if (term != currentTerm) {
      throw new NoSuchElementException ("The term " + term + " does not exist in this log file");
    }
    // Get the LogReader for this sessionKey
    LogReader reader = this.getReader(sessionKey);
    return reader.seekAndRead(index, arena);
  }

  @Override public long getLastModificationTime() {
    return file.lastModified();
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

  /**
   * @return the initial index of the log
   */
  @Override
  public long getInitialIndex() {
    return initialIndex;
  }

  /**
   * @return the term of the log
   */
  public long getCurrentTerm() {
    return currentTerm;
  }

  /**
   * Close all the LogReader instances and delete the file
   * @throws IOException
   */
  @Override
  public void closeAndDelete() throws IOException {
    for (String key : this.readerMap.keySet()) {
      removeReader(key);
    }
    this.file.delete();
  }

  @Override public long getTxnCount() {
    return this.getLastIndex() - this.getInitialIndex() + 1;
  }

  @Override public String getFileName() {
    return file.getName();
  }

  @Override public File getFile() {
    return file;
  }

  /** 
   * Returns the last index in this log.
   *
   * @return
   * @throws IOException
   */
  @Override
  public long getLastIndex() {
    return lastIndex;
  }

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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReadOnlyLog{");
    sb.append("term=").append(getCurrentTerm())
      .append(", index=[").append(getInitialIndex())
      .append(", ").append(getLastIndex()).append("]")
      .append(", path=").append(getFileName())
      .append(", ntxns=").append(getTxnCount());
    return sb.toString();
  }
}
