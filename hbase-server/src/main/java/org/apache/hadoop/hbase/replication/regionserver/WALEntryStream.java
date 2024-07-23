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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader;
import org.apache.hadoop.hbase.regionserver.wal.WALHeaderEOFException;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.hadoop.hbase.wal.WALTailingReader;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link Path}, and continually
 * iterates through all the WAL {@link Entry} in the queue. When it's done reading from a Path, it
 * dequeues it and starts reading from the next.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class WALEntryStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(WALEntryStream.class);

  private WALTailingReader reader;
  private WALTailingReader.State state;
  private Path currentPath;
  // cache of next entry for hasNext()
  private Entry currentEntry;
  // position for the current entry. As now we support peek, which means that the upper layer may
  // choose to return before reading the current entry, so it is not safe to return the value below
  // in getPosition.
  private long currentPositionOfEntry = 0;
  // position after reading current entry
  private long currentPositionOfReader = 0;
  private final ReplicationSourceLogQueue logQueue;
  private final String walGroupId;
  private final FileSystem fs;
  private final Configuration conf;
  private final WALFileLengthProvider walFileLengthProvider;
  private final MetricsSource metrics;

  // we should be able to skip empty WAL files, but for safety, we still provide this config
  // see HBASE-18137 for more details
  private boolean eofAutoRecovery;

  /**
   * Create an entry stream over the given queue at the given start position
   * @param logQueue              the queue of WAL paths
   * @param conf                  the {@link Configuration} to use to create {@link WALStreamReader}
   *                              for this stream
   * @param startPosition         the position in the first WAL to start reading at
   * @param walFileLengthProvider provides the length of the WAL file
   * @param serverName            the server name which all WALs belong to
   * @param metrics               the replication metrics
   */
  public WALEntryStream(ReplicationSourceLogQueue logQueue, FileSystem fs, Configuration conf,
    long startPosition, WALFileLengthProvider walFileLengthProvider, MetricsSource metrics,
    String walGroupId) {
    this.logQueue = logQueue;
    this.fs = fs;
    this.conf = conf;
    this.currentPositionOfEntry = startPosition;
    this.walFileLengthProvider = walFileLengthProvider;
    this.metrics = metrics;
    this.walGroupId = walGroupId;
    this.eofAutoRecovery = conf.getBoolean("replication.source.eof.autorecovery", false);
  }

  public enum HasNext {
    /** means there is a new entry and you could use peek or next to get current entry */
    YES,
    /**
     * means there are something wrong or we have reached EOF of the current file but it is not
     * closed yet and there is no new file in the replication queue yet, you should sleep a while
     * and try to call hasNext again
     */
    RETRY,
    /**
     * Usually this means we have finished reading a WAL file, and for simplify the implementation
     * of this class, we just let the upper layer issue a new hasNext call again to open the next
     * WAL file.
     */
    RETRY_IMMEDIATELY,
    /**
     * means there is no new entry and stream is end, the upper layer should close this stream and
     * release other resources as well
     */
    NO
  }

  /**
   * Try advance the stream if there is no entry yet. See the javadoc for {@link HasNext} for more
   * details about the meanings of the return values.
   * <p/>
   * You can call {@link #peek()} or {@link #next()} to get the actual {@link Entry} if this method
   * returns {@link HasNext#YES}.
   */
  public HasNext hasNext() {
    if (currentEntry == null) {
      return tryAdvanceEntry();
    } else {
      return HasNext.YES;
    }
  }

  /**
   * Returns the next WAL entry in this stream but does not advance.
   * <p/>
   * Must call {@link #hasNext()} first before calling this method, and if you have already called
   * {@link #next()} to consume the current entry, you need to call {@link #hasNext()} again to
   * advance the stream before calling this method again, otherwise it will always return
   * {@code null}
   * <p/>
   * The reason here is that, we need to use the return value of {@link #hasNext()} to tell upper
   * layer to retry or not, so we can not wrap the {@link #hasNext()} call inside {@link #peek()} or
   * {@link #next()} as they have their own return value.
   * @see #hasNext()
   * @see #next()
   */
  public Entry peek() {
    return currentEntry;
  }

  /**
   * Returns the next WAL entry in this stream and advance the stream. Will throw
   * {@link IllegalStateException} if you do not call {@link #hasNext()} before calling this method.
   * Please see the javadoc of {@link #peek()} method to see why we need this.
   * @throws IllegalStateException Every time you want to call this method, please call
   *                               {@link #hasNext()} first, otherwise a
   *                               {@link IllegalStateException} will be thrown.
   * @see #hasNext()
   * @see #peek()
   */
  public Entry next() {
    if (currentEntry == null) {
      throw new IllegalStateException("Call hasNext first");
    }
    Entry save = peek();
    currentPositionOfEntry = currentPositionOfReader;
    currentEntry = null;
    state = null;
    return save;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    closeReader();
  }

  /** Returns the position of the last Entry returned by next() */
  public long getPosition() {
    return currentPositionOfEntry;
  }

  /** Returns the {@link Path} of the current WAL */
  public Path getCurrentPath() {
    return currentPath;
  }

  private String getCurrentPathStat() {
    StringBuilder sb = new StringBuilder();
    if (currentPath != null) {
      sb.append("currently replicating from: ").append(currentPath).append(" at position: ")
        .append(currentPositionOfEntry).append("\n");
    } else {
      sb.append("no replication ongoing, waiting for new log");
    }
    return sb.toString();
  }

  private void setCurrentPath(Path path) {
    this.currentPath = path;
  }

  private void resetReader() throws IOException {
    if (currentPositionOfEntry > 0) {
      reader.resetTo(currentPositionOfEntry, state.resetCompression());
    } else {
      // we will read from the beginning so we should always clear the compression context
      reader.resetTo(-1, true);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DCN_NULLPOINTER_EXCEPTION",
      justification = "HDFS-4380")
  private HasNext prepareReader() {
    if (reader != null) {
      if (state != null && state != WALTailingReader.State.NORMAL) {
        // reset before reading
        LOG.debug("Reset reader {} to pos {}, reset compression={}", currentPath,
          currentPositionOfEntry, state.resetCompression());
        try {
          resetReader();
          return HasNext.YES;
        } catch (IOException e) {
          LOG.warn("Failed to reset reader {} to pos {}, reset compression={}", currentPath,
            currentPositionOfEntry, state.resetCompression(), e);
          // just leave the state as is, and try resetting next time
          return HasNext.RETRY;
        }
      } else {
        return HasNext.YES;
      }
    }
    // try open next WAL file
    PriorityBlockingQueue<Path> queue = logQueue.getQueue(walGroupId);
    Path nextPath = queue.peek();
    if (nextPath == null) {
      LOG.debug("No more WAL files in queue");
      // no more files in queue, this could happen for recovered queue, or for a wal group of a
      // sync replication peer which has already been transited to DA or S.
      setCurrentPath(null);
      return HasNext.NO;
    }
    setCurrentPath(nextPath);
    // we need to test this prior to create the reader. If not, it is possible that, while
    // opening the file, the file is still being written so its header is incomplete and we get
    // a header EOF, but then while we test whether it is still being written, we have already
    // flushed the data out and we consider it is not being written, and then we just skip over
    // file, then we will lose the data written after opening...
    boolean beingWritten = walFileLengthProvider.getLogFileSizeIfBeingWritten(nextPath).isPresent();
    LOG.debug("Creating new reader {}, startPosition={}, beingWritten={}", nextPath,
      currentPositionOfEntry, beingWritten);
    try {
      reader = WALFactory.createTailingReader(fs, nextPath, conf,
        currentPositionOfEntry > 0 ? currentPositionOfEntry : -1);
      return HasNext.YES;
    } catch (WALHeaderEOFException e) {
      if (!eofAutoRecovery) {
        // if we do not enable EOF auto recovery, just let the upper layer retry
        // the replication will be stuck usually, and need to be fixed manually
        return HasNext.RETRY;
      }
      // we hit EOF while reading the WAL header, usually this means we can just skip over this
      // file, but we need to be careful that whether this file is still being written, if so we
      // should retry instead of skipping.
      LOG.warn("EOF while trying to open WAL reader for path: {}, startPosition={}", nextPath,
        currentPositionOfEntry, e);
      if (beingWritten) {
        // just retry as the file is still being written, maybe next time we could read
        // something
        return HasNext.RETRY;
      } else {
        // the file is not being written so we are safe to just skip over it
        dequeueCurrentLog();
        return HasNext.RETRY_IMMEDIATELY;
      }
    } catch (LeaseNotRecoveredException e) {
      // HBASE-15019 the WAL was not closed due to some hiccup.
      LOG.warn("Try to recover the WAL lease " + nextPath, e);
      AbstractFSWALProvider.recoverLease(conf, nextPath);
      return HasNext.RETRY;
    } catch (IOException | NullPointerException e) {
      // For why we need to catch NPE here, see HDFS-4380 for more details
      LOG.warn("Failed to open WAL reader for path: {}", nextPath, e);
      return HasNext.RETRY;
    }
  }

  private HasNext lastAttempt() {
    LOG.debug("Reset reader {} for the last time to pos {}, reset compression={}", currentPath,
      currentPositionOfEntry, state.resetCompression());
    try {
      resetReader();
    } catch (IOException e) {
      LOG.warn("Failed to reset reader {} to pos {}, reset compression={}", currentPath,
        currentPositionOfEntry, state.resetCompression(), e);
      // just leave the state as is, next time we will try to reset it again, but there is a
      // nasty problem is that, we will still reach here finally and try reset again to see if
      // the log has been fully replicated, which is redundant, can be optimized later
      return HasNext.RETRY;
    }
    Pair<WALTailingReader.State, Boolean> pair = readNextEntryAndRecordReaderPosition();
    state = pair.getFirst();
    // should not be written
    assert !pair.getSecond();
    if (!state.eof()) {
      // we still have something to read after reopen, so return YES. Or there are something wrong
      // and we need to retry
      return state == WALTailingReader.State.NORMAL ? HasNext.YES : HasNext.RETRY;
    }
    // No data available after reopen
    if (checkAllBytesParsed()) {
      // move to the next wal file and read
      dequeueCurrentLog();
      return HasNext.RETRY_IMMEDIATELY;
    } else {
      // see HBASE-15983, if checkAllBytesParsed returns false, we need to try read from
      // beginning again. Here we set position to 0 and state to ERROR_AND_RESET_COMPRESSION
      // so when calling tryAdvanceENtry next time we will reset the reader to the beginning
      // and read.
      currentPositionOfEntry = 0;
      currentPositionOfReader = 0;
      state = WALTailingReader.State.ERROR_AND_RESET_COMPRESSION;
      return HasNext.RETRY;
    }
  }

  private HasNext tryAdvanceEntry() {
    HasNext prepared = prepareReader();
    if (prepared != HasNext.YES) {
      return prepared;
    }

    Pair<WALTailingReader.State, Boolean> pair = readNextEntryAndRecordReaderPosition();
    state = pair.getFirst();
    boolean beingWritten = pair.getSecond();
    LOG.trace("Reading WAL {}; result={}, currently open for write={}", this.currentPath, state,
      beingWritten);
    // The below implementation needs to make sure that when beingWritten == true, we should not
    // dequeue the current WAL file in logQueue.
    switch (state) {
      case NORMAL:
        // everything is fine, just return
        return HasNext.YES;
      case EOF_WITH_TRAILER:
        // in readNextEntryAndRecordReaderPosition, we will acquire rollWriteLock, and we can only
        // schedule a close writer task, in which we will write trailer, under the rollWriteLock, so
        // typically if beingWritten == true, we should not reach here, as we need to reopen the
        // reader after writing the trailer. The only possible way to reach here while beingWritten
        // == true is due to the inflightWALClosures logic in AbstractFSWAL, as if the writer is
        // still in this map, we will consider it as beingWritten, but actually, here we could make
        // sure that the new WAL file has already been enqueued into the logQueue, so here dequeuing
        // the current log file is safe.
        if (beingWritten && logQueue.getQueue(walGroupId).size() <= 1) {
          // As explained above, if we implement everything correctly, we should not arrive here.
          // But anyway, even if we reach here due to some code changes in the future, reading
          // the file again can make sure that we will not accidentally consider the queue as
          // finished, and since there is a trailer, we will soon consider the file as finished
          // and move on.
          LOG.warn(
            "We have reached the trailer while reading the file '{}' which is currently"
              + " beingWritten, but it is the last file in log queue {}. This should not happen"
              + " typically, try to read again so we will not miss anything",
            currentPath, walGroupId);
          return HasNext.RETRY;
        }
        assert !beingWritten || logQueue.getQueue(walGroupId).size() > 1;
        // we have reached the trailer, which means this WAL file has been closed cleanly and we
        // have finished reading it successfully, just move to the next WAL file and let the upper
        // layer start reading the next WAL file
        dequeueCurrentLog();
        return HasNext.RETRY_IMMEDIATELY;
      case EOF_AND_RESET:
      case EOF_AND_RESET_COMPRESSION:
        if (beingWritten) {
          // just sleep a bit and retry to see if there are new entries coming since the file is
          // still being written
          return HasNext.RETRY;
        }
        // no more entries in this log file, and the file is already closed, i.e, rolled
        // Before dequeuing, we should always get one more attempt at reading.
        // This is in case more entries came in after we opened the reader, and the log is rolled
        // while we were reading. See HBASE-6758
        return lastAttempt();
      case ERROR_AND_RESET:
      case ERROR_AND_RESET_COMPRESSION:
        // we have meet an error, just sleep a bit and retry again
        return HasNext.RETRY;
      default:
        throw new IllegalArgumentException("Unknown read next result: " + state);
    }
  }

  private FileStatus getCurrentPathFileStatus() throws IOException {
    try {
      return fs.getFileStatus(currentPath);
    } catch (FileNotFoundException e) {
      // try archived path
      Path archivedWAL = AbstractFSWALProvider.findArchivedLog(currentPath, conf);
      if (archivedWAL != null) {
        return fs.getFileStatus(archivedWAL);
      } else {
        throw e;
      }
    }
  }

  // HBASE-15984 check to see we have in fact parsed all data in a cleanly closed file
  private boolean checkAllBytesParsed() {
    // -1 means the wal wasn't closed cleanly.
    final long trailerSize = currentTrailerSize();
    FileStatus stat = null;
    try {
      stat = getCurrentPathFileStatus();
    } catch (IOException e) {
      LOG.warn("Couldn't get file length information about log {}, it {} closed cleanly {}",
        currentPath, trailerSize < 0 ? "was not" : "was", getCurrentPathStat(), e);
      metrics.incrUnknownFileLengthForClosedWAL();
    }
    // Here we use currentPositionOfReader instead of currentPositionOfEntry.
    // We only call this method when currentEntry is null so usually they are the same, but there
    // are two exceptions. One is we have nothing in the file but only a header, in this way
    // the currentPositionOfEntry will always be 0 since we have no change to update it. The other
    // is that we reach the end of file, then currentPositionOfEntry will point to the tail of the
    // last valid entry, and the currentPositionOfReader will usually point to the end of the file.
    if (stat != null) {
      if (trailerSize < 0) {
        if (currentPositionOfReader < stat.getLen()) {
          final long skippedBytes = stat.getLen() - currentPositionOfReader;
          // See the commits in HBASE-25924/HBASE-25932 for context.
          LOG.warn("Reached the end of WAL {}. It was not closed cleanly,"
            + " so we did not parse {} bytes of data.", currentPath, skippedBytes);
          metrics.incrUncleanlyClosedWALs();
          metrics.incrBytesSkippedInUncleanlyClosedWALs(skippedBytes);
        }
      } else if (currentPositionOfReader + trailerSize < stat.getLen()) {
        LOG.warn(
          "Processing end of WAL {} at position {}, which is too far away from"
            + " reported file length {}. Restarting WAL reading (see HBASE-15983 for details). {}",
          currentPath, currentPositionOfReader, stat.getLen(), getCurrentPathStat());
        metrics.incrRestartedWALReading();
        metrics.incrRepeatedFileBytes(currentPositionOfReader);
        return false;
      }
    }
    LOG.debug("Reached the end of {} and length of the file is {}", currentPath,
      stat == null ? "N/A" : stat.getLen());
    metrics.incrCompletedWAL();
    return true;
  }

  private void dequeueCurrentLog() {
    LOG.debug("EOF, closing {}", currentPath);
    closeReader();
    logQueue.remove(walGroupId);
    setCurrentPath(null);
    currentPositionOfEntry = 0;
    state = null;
  }

  /**
   * Returns whether the file is opened for writing.
   */
  private Pair<WALTailingReader.State, Boolean> readNextEntryAndRecordReaderPosition() {
    OptionalLong fileLength;
    if (logQueue.getQueueSize(walGroupId) > 1) {
      // if there are more than one files in queue, although it is possible that we are
      // still trying to write the trailer of the file and it is not closed yet, we can
      // make sure that we will not write any WAL entries to it any more, so it is safe
      // to just let the upper layer try to read the whole file without limit
      fileLength = OptionalLong.empty();
    } else {
      // if there is only one file in queue, check whether it is still being written to
      // we must call this before actually reading from the reader, as this method will acquire the
      // rollWriteLock. This is very important, as we will enqueue the new WAL file in postLogRoll,
      // and before this happens, we could have already finished closing the previous WAL file. If
      // we do not acquire the rollWriteLock and return whether the current file is being written
      // to, we may finish reading the previous WAL file and start to read the next one, before it
      // is enqueued into the logQueue, thus lead to an empty logQueue and make the shipper think
      // the queue is already ended and quit. See HBASE-28114 and related issues for more details.
      // in the future, if we want to optimize the logic here, for example, do not call this method
      // every time, or do not acquire rollWriteLock in the implementation of this method, we need
      // to carefully review the optimized implementation
      fileLength = walFileLengthProvider.getLogFileSizeIfBeingWritten(currentPath);
    }
    WALTailingReader.Result readResult = reader.next(fileLength.orElse(-1));
    long readerPos = readResult.getEntryEndPos();
    Entry readEntry = readResult.getEntry();
    if (readResult.getState() == WALTailingReader.State.NORMAL) {
      LOG.trace("reading entry: {} ", readEntry);
      metrics.incrLogEditsRead();
      metrics.incrLogReadInBytes(readerPos - currentPositionOfEntry);
      // record current entry and reader position
      currentEntry = readResult.getEntry();
      this.currentPositionOfReader = readerPos;
    } else {
      LOG.trace("reading entry failed with: {}", readResult.getState());
      // set current entry to null
      currentEntry = null;
      try {
        this.currentPositionOfReader = reader.getPosition();
      } catch (IOException e) {
        LOG.warn("failed to get current position of reader", e);
        if (readResult.getState().resetCompression()) {
          return Pair.newPair(WALTailingReader.State.ERROR_AND_RESET_COMPRESSION,
            fileLength.isPresent());
        }
      }
    }
    return Pair.newPair(readResult.getState(), fileLength.isPresent());
  }

  private void closeReader() {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private long currentTrailerSize() {
    long size = -1L;
    if (reader instanceof AbstractProtobufWALReader) {
      final AbstractProtobufWALReader pbwr = (AbstractProtobufWALReader) reader;
      size = pbwr.trailerSize();
    }
    return size;
  }
}
