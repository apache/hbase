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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link Path}, and continually
 * iterates through all the WAL {@link Entry} in the queue. When it's done reading from a Path, it
 * dequeues it and starts reading from the next.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class WALEntryStream implements Iterator<Entry>, Closeable, Iterable<Entry> {
  private static final Log LOG = LogFactory.getLog(WALEntryStream.class);

  private Reader reader;
  private Path currentPath;
  // cache of next entry for hasNext()
  private Entry currentEntry;
  // position after reading current entry
  private long currentPosition = 0;
  private PriorityBlockingQueue<Path> logQueue;
  private FileSystem fs;
  private Configuration conf;
  private MetricsSource metrics;

  /**
   * Create an entry stream over the given queue
   * @param logQueue the queue of WAL paths
   * @param fs {@link FileSystem} to use to create {@link Reader} for this stream
   * @param conf {@link Configuration} to use to create {@link Reader} for this stream
   * @param metrics replication metrics
   * @throws IOException
   */
  public WALEntryStream(PriorityBlockingQueue<Path> logQueue, FileSystem fs, Configuration conf,
      MetricsSource metrics)
      throws IOException {
    this(logQueue, fs, conf, 0, metrics);
  }

  /**
   * Create an entry stream over the given queue at the given start position
   * @param logQueue the queue of WAL paths
   * @param fs {@link FileSystem} to use to create {@link Reader} for this stream
   * @param conf {@link Configuration} to use to create {@link Reader} for this stream
   * @param startPosition the position in the first WAL to start reading at
   * @param metrics replication metrics
   * @throws IOException
   */
  public WALEntryStream(PriorityBlockingQueue<Path> logQueue, FileSystem fs, Configuration conf,
      long startPosition, MetricsSource metrics) throws IOException {
    this.logQueue = logQueue;
    this.fs = fs;
    this.conf = conf;
    this.currentPosition = startPosition;
    this.metrics = metrics;
  }

  /**
   * @return true if there is another WAL {@link Entry}
   * @throws WALEntryStreamRuntimeException if there was an Exception while reading
   */
  @Override
  public boolean hasNext() {
    if (currentEntry == null) {
      try {
        tryAdvanceEntry();
      } catch (Exception e) {
        throw new WALEntryStreamRuntimeException(e);
      }
    }
    return currentEntry != null;
  }

  /**
   * @return the next WAL entry in this stream
   * @throws WALEntryStreamRuntimeException if there was an IOException
   * @throws NoSuchElementException if no more entries in the stream.
   */
  @Override
  public Entry next() {
    if (!hasNext()) throw new NoSuchElementException();
    Entry save = currentEntry;
    currentEntry = null; // gets reloaded by hasNext()
    return save;
  }

  /**
   * Not supported.
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    closeReader();
  }

  /**
   * @return the iterator over WAL entries in the queue.
   */
  @Override
  public Iterator<Entry> iterator() {
    return this;
  }

  /**
   * @return the position of the last Entry returned by next()
   */
  public long getPosition() {
    return currentPosition;
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
          .append(currentPosition).append("\n");
    } else {
      sb.append("no replication ongoing, waiting for new log");
    }
    return sb.toString();
  }

  /**
   * Should be called if the stream is to be reused (i.e. used again after hasNext() has returned
   * false)
   * @throws IOException
   */
  public void reset() throws IOException {
    if (reader != null && currentPath != null) {
      resetReader();
    }
  }

  private void setPosition(long position) {
    currentPosition = position;
  }

  private void setCurrentPath(Path path) {
    this.currentPath = path;
  }

  private void tryAdvanceEntry() throws IOException {
    if (checkReader()) {
      readNextEntryAndSetPosition();
      if (currentEntry == null) { // no more entries in this log file - see if log was rolled
        if (logQueue.size() > 1) { // log was rolled
          // Before dequeueing, we should always get one more attempt at reading.
          // This is in case more entries came in after we opened the reader,
          // and a new log was enqueued while we were reading. See HBASE-6758
          resetReader();
          readNextEntryAndSetPosition();
          if (currentEntry == null) {
            if (checkAllBytesParsed()) { // now we're certain we're done with this log file
              dequeueCurrentLog();
              if (openNextLog()) {
                readNextEntryAndSetPosition();
              }
            }
          }
        } // no other logs, we've simply hit the end of the current open log. Do nothing
      }
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
      LOG.warn("Couldn't get file length information about log " + this.currentPath + ", it "
          + (trailerSize < 0 ? "was not" : "was") + " closed cleanly " + getCurrentPathStat());
      metrics.incrUnknownFileLengthForClosedWAL();
    }
    if (stat != null) {
      if (trailerSize < 0) {
        if (currentPosition < stat.getLen()) {
          final long skippedBytes = stat.getLen() - currentPosition;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Reached the end of WAL file '" + currentPath
                + "'. It was not closed cleanly, so we did not parse " + skippedBytes
                + " bytes of data. This is normally ok.");
          }
          metrics.incrUncleanlyClosedWALs();
          metrics.incrBytesSkippedInUncleanlyClosedWALs(skippedBytes);
        }
      } else if (currentPosition + trailerSize < stat.getLen()) {
        LOG.warn("Processing end of WAL file '" + currentPath + "'. At position " + currentPosition
            + ", which is too far away from reported file length " + stat.getLen()
            + ". Restarting WAL reading (see HBASE-15983 for details). " + getCurrentPathStat());
        setPosition(0);
        resetReader();
        metrics.incrRestartedWALReading();
        metrics.incrRepeatedFileBytes(currentPosition);
        return false;
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Reached the end of log " + this.currentPath + ", and the length of the file is "
          + (stat == null ? "N/A" : stat.getLen()));
    }
    metrics.incrCompletedWAL();
    return true;
  }

  private void dequeueCurrentLog() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reached the end of log " + currentPath);
    }
    closeReader();
    logQueue.remove();
    setPosition(0);
    metrics.decrSizeOfLogQueue();
  }

  private void readNextEntryAndSetPosition() throws IOException {
    Entry readEntry = reader.next();
    long readerPos = reader.getPosition();
    if (readEntry != null) {
      metrics.incrLogEditsRead();
      metrics.incrLogReadInBytes(readerPos - currentPosition);
    }
    currentEntry = readEntry; // could be null
    setPosition(readerPos);
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
    Path nextPath = logQueue.peek();
    if (nextPath != null) {
      openReader(nextPath);
      if (reader != null) return true;
    }
    return false;
  }

  private Path getArchivedLog(Path path) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path archivedLogLocation = new Path(oldLogDir, path.getName());
    if (fs.exists(archivedLogLocation)) {
      LOG.info("Log " + path + " was moved to " + archivedLogLocation);
      return archivedLogLocation;
    } else {
      LOG.error("Couldn't locate log: " + path);
      return path;
    }
  }

  private void handleFileNotFound(Path path, FileNotFoundException fnfe) throws IOException {
    // If the log was archived, continue reading from there
    Path archivedLog = getArchivedLog(path);
    if (!path.equals(archivedLog)) {
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
      if (!(ioe instanceof FileNotFoundException)) throw ioe;
      handleFileNotFound(path, (FileNotFoundException)ioe);
    } catch (LeaseNotRecoveredException lnre) {
      // HBASE-15019 the WAL was not closed due to some hiccup.
      LOG.warn("Try to recover the WAL lease " + currentPath, lnre);
      recoverLease(conf, currentPath);
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
      final FileSystem dfs = FSUtils.getCurrentFileSystem(conf);
      FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
      fsUtils.recoverFileLease(dfs, path, conf, new CancelableProgressable() {
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
      reader.reset();
      seek();
    } catch (FileNotFoundException fnfe) {
      // If the log was archived, continue reading from there
      Path archivedLog = getArchivedLog(currentPath);
      if (!currentPath.equals(archivedLog)) {
        openReader(archivedLog);
      } else {
        throw fnfe;
      }
    } catch (NullPointerException npe) {
      throw new IOException("NPE resetting reader, likely HDFS-4380", npe);
    }
  }

  private void seek() throws IOException {
    if (currentPosition != 0) {
      reader.seek(currentPosition);
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

  @InterfaceAudience.Private
  public static class WALEntryStreamRuntimeException extends RuntimeException {
    private static final long serialVersionUID = -6298201811259982568L;

    public WALEntryStreamRuntimeException(Exception e) {
      super(e);
    }
  }

}
