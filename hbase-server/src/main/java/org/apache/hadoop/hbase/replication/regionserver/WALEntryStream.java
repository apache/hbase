/**
 *
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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.ipc.RemoteException;
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

  private Reader reader;
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
  // which region server the WALs belong to
  private final ServerName serverName;
  private final MetricsSource metrics;

  /**
   * Create an entry stream over the given queue at the given start position
   * @param logQueue the queue of WAL paths
   * @param conf the {@link Configuration} to use to create {@link Reader} for this stream
   * @param startPosition the position in the first WAL to start reading at
   * @param walFileLengthProvider provides the length of the WAL file
   * @param serverName the server name which all WALs belong to
   * @param metrics the replication metrics
   * @throws IOException throw IO exception from stream
   */
  public WALEntryStream(ReplicationSourceLogQueue logQueue, Configuration conf,
      long startPosition, WALFileLengthProvider walFileLengthProvider, ServerName serverName,
      MetricsSource metrics, String walGroupId) throws IOException {
    this.logQueue = logQueue;
    this.fs = CommonFSUtils.getWALFileSystem(conf);
    this.conf = conf;
    this.currentPositionOfEntry = startPosition;
    this.walFileLengthProvider = walFileLengthProvider;
    this.serverName = serverName;
    this.metrics = metrics;
    this.walGroupId = walGroupId;
  }

  /**
   * @return true if there is another WAL {@link Entry}
   */
  public boolean hasNext() throws IOException {
    if (currentEntry == null) {
      tryAdvanceEntry();
    }
    return currentEntry != null;
  }

  /**
   * Returns the next WAL entry in this stream but does not advance.
   */
  public Entry peek() throws IOException {
    return hasNext() ? currentEntry: null;
  }

  /**
   * Returns the next WAL entry in this stream and advance the stream.
   */
  public Entry next() throws IOException {
    Entry save = peek();
    currentPositionOfEntry = currentPositionOfReader;
    currentEntry = null;
    return save;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    closeReader();
  }

  /**
   * @return the position of the last Entry returned by next()
   */
  public long getPosition() {
    return currentPositionOfEntry;
  }

  /**
   * @return the {@link Path} of the current WAL
   */
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

  /**
   * Should be called if the stream is to be reused (i.e. used again after hasNext() has returned
   * false)
   */
  public void reset() throws IOException {
    if (reader != null && currentPath != null) {
      resetReader();
    }
  }

  private void setPosition(long position) {
    currentPositionOfEntry = position;
  }

  private void setCurrentPath(Path path) {
    this.currentPath = path;
  }

  private void tryAdvanceEntry() throws IOException {
    if (checkReader()) {
      boolean beingWritten = readNextEntryAndRecordReaderPosition();
      LOG.trace("Reading WAL {}; currently open for write={}", this.currentPath, beingWritten);
      if (currentEntry == null && !beingWritten) {
        // no more entries in this log file, and the file is already closed, i.e, rolled
        // Before dequeueing, we should always get one more attempt at reading.
        // This is in case more entries came in after we opened the reader, and the log is rolled
        // while we were reading. See HBASE-6758
        resetReader();
        readNextEntryAndRecordReaderPosition();
        if (currentEntry == null) {
          if (checkAllBytesParsed()) { // now we're certain we're done with this log file
            dequeueCurrentLog();
            if (openNextLog()) {
              readNextEntryAndRecordReaderPosition();
            }
          }
        }
      }
      // if currentEntry != null then just return
      // if currentEntry == null but the file is still being written, then we should not switch to
      // the next log either, just return here and try next time to see if there are more entries in
      // the current file
    }
    // do nothing if we don't have a WAL Reader (e.g. if there's no logs in queue)
  }

  // HBASE-15984 check to see we have in fact parsed all data in a cleanly closed file
  private boolean checkAllBytesParsed() throws IOException {
    // -1 means the wal wasn't closed cleanly.
    final long trailerSize = currentTrailerSize();
    FileStatus stat = null;
    try {
      stat = fs.getFileStatus(this.currentPath);
    } catch (IOException exception) {
      LOG.warn("Couldn't get file length information about log {}, it {} closed cleanly {}",
        currentPath, trailerSize < 0 ? "was not" : "was", getCurrentPathStat());
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
          LOG.warn("Reached the end of WAL {}. It was not closed cleanly," +
              " so we did not parse {} bytes of data.", currentPath, skippedBytes);
          metrics.incrUncleanlyClosedWALs();
          metrics.incrBytesSkippedInUncleanlyClosedWALs(skippedBytes);
        }
      } else if (currentPositionOfReader + trailerSize < stat.getLen()) {
        LOG.warn(
          "Processing end of WAL {} at position {}, which is too far away from" +
            " reported file length {}. Restarting WAL reading (see HBASE-15983 for details). {}",
          currentPath, currentPositionOfReader, stat.getLen(), getCurrentPathStat());
        setPosition(0);
        resetReader();
        metrics.incrRestartedWALReading();
        metrics.incrRepeatedFileBytes(currentPositionOfReader);
        return false;
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Reached the end of " + this.currentPath + " and length of the file is " +
        (stat == null ? "N/A" : stat.getLen()));
    }
    metrics.incrCompletedWAL();
    return true;
  }

  private void dequeueCurrentLog() throws IOException {
    LOG.debug("EOF, closing {}", currentPath);
    closeReader();
    logQueue.remove(walGroupId);
    setCurrentPath(null);
    setPosition(0);
  }

  /**
   * Returns whether the file is opened for writing.
   */
  private boolean readNextEntryAndRecordReaderPosition() throws IOException {
    Entry readEntry = reader.next();
    long readerPos = reader.getPosition();
    OptionalLong fileLength = walFileLengthProvider.getLogFileSizeIfBeingWritten(currentPath);
    if (fileLength.isPresent() && readerPos > fileLength.getAsLong()) {
      // See HBASE-14004, for AsyncFSWAL which uses fan-out, it is possible that we read uncommitted
      // data, so we need to make sure that we do not read beyond the committed file length.
      if (LOG.isDebugEnabled()) {
        LOG.debug("The provider tells us the valid length for " + currentPath + " is " +
            fileLength.getAsLong() + ", but we have advanced to " + readerPos);
      }
      resetReader();
      return true;
    }
    if (readEntry != null) {
      LOG.trace("reading entry: {} ", readEntry);
      metrics.incrLogEditsRead();
      metrics.incrLogReadInBytes(readerPos - currentPositionOfEntry);
    }
    currentEntry = readEntry; // could be null
    this.currentPositionOfReader = readerPos;
    return fileLength.isPresent();
  }

  private void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  // if we don't have a reader, open a reader on the next log
  private boolean checkReader() throws IOException {
    if (reader == null) {
      return openNextLog();
    }
    return true;
  }

  // open a reader on the next log in queue
  private boolean openNextLog() throws IOException {
    PriorityBlockingQueue<Path> queue = logQueue.getQueue(walGroupId);
    Path nextPath = queue.peek();
    if (nextPath != null) {
      openReader(nextPath);
      if (reader != null) {
        return true;
      }
    } else {
      // no more files in queue, this could only happen for recovered queue.
      setCurrentPath(null);
    }
    return false;
  }

  private void handleFileNotFound(Path path, FileNotFoundException fnfe) throws IOException {
    // If the log was archived, continue reading from there
    Path archivedLog = AbstractFSWALProvider.findArchivedLog(path, conf);
    // archivedLog can be null if unable to locate in archiveDir.
    if (archivedLog != null) {
      openReader(archivedLog);
    } else {
      throw fnfe;
    }
  }

  private void openReader(Path path) throws IOException {
    try {
      // Detect if this is a new file, if so get a new reader else
      // reset the current reader so that we see the new data
      if (reader == null || !getCurrentPath().equals(path)) {
        closeReader();
        reader = WALFactory.createReader(fs, path, conf);
        seek();
        setCurrentPath(path);
      } else {
        resetReader();
      }
    } catch (FileNotFoundException fnfe) {
      handleFileNotFound(path, fnfe);
    }  catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException(FileNotFoundException.class);
      if (!(ioe instanceof FileNotFoundException)) {
        throw ioe;
      }
      handleFileNotFound(path, (FileNotFoundException)ioe);
    } catch (LeaseNotRecoveredException lnre) {
      // HBASE-15019 the WAL was not closed due to some hiccup.
      LOG.warn("Try to recover the WAL lease " + path, lnre);
      recoverLease(conf, path);
      reader = null;
    } catch (NullPointerException npe) {
      // Workaround for race condition in HDFS-4380
      // which throws a NPE if we open a file before any data node has the most recent block
      // Just sleep and retry. Will require re-reading compressed WALs for compressionContext.
      LOG.warn("Got NPE opening reader, will retry.");
      reader = null;
    }
  }

  // For HBASE-15019
  private void recoverLease(final Configuration conf, final Path path) {
    try {
      final FileSystem dfs = CommonFSUtils.getWALFileSystem(conf);
      RecoverLeaseFSUtils.recoverFileLease(dfs, path, conf, new CancelableProgressable() {
        @Override
        public boolean progress() {
          LOG.debug("recover WAL lease: " + path);
          return true;
        }
      });
    } catch (IOException e) {
      LOG.warn("unable to recover lease for WAL: " + path, e);
    }
  }

  private void resetReader() throws IOException {
    try {
      currentEntry = null;
      reader.reset();
      seek();
    } catch (FileNotFoundException fnfe) {
      // If the log was archived, continue reading from there
      Path archivedLog = AbstractFSWALProvider.findArchivedLog(currentPath, conf);
      // archivedLog can be null if unable to locate in archiveDir.
      if (archivedLog != null) {
        openReader(archivedLog);
      } else {
        throw fnfe;
      }
    } catch (NullPointerException npe) {
      throw new IOException("NPE resetting reader, likely HDFS-4380", npe);
    }
  }

  private void seek() throws IOException {
    if (currentPositionOfEntry != 0) {
      reader.seek(currentPositionOfEntry);
    }
  }

  private long currentTrailerSize() {
    long size = -1L;
    if (reader instanceof ProtobufLogReader) {
      final ProtobufLogReader pblr = (ProtobufLogReader) reader;
      size = pblr.trailerSize();
    }
    return size;
  }
}
