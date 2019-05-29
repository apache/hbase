/**
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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.wal.WALSplitUtil.getCompletedRecoveredEditsFilePath;
import static org.apache.hadoop.hbase.wal.WALSplitUtil.getRegionSplitEditsPath;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * Class that manages the output streams from the log splitting process.
 */
@InterfaceAudience.Private
public class LogRecoveredEditsOutputSink extends OutputSink {
  private static final Logger LOG = LoggerFactory.getLogger(LogRecoveredEditsOutputSink.class);
  private WALSplitter walSplitter;
  private FileSystem walFS;
  private Configuration conf;

  public LogRecoveredEditsOutputSink(WALSplitter walSplitter,
      WALSplitter.PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
    // More threads could potentially write faster at the expense
    // of causing more disk seeks as the logs are split.
    // 3. After a certain setting (probably around 3) the
    // process will be bound on the reader in the current
    // implementation anyway.
    super(controller, entryBuffers, numWriters);
    this.walSplitter = walSplitter;
    this.walFS = walSplitter.walFS;
    this.conf = walSplitter.conf;
  }

  /**
   * @return null if failed to report progress
   */
  @Override
  public List<Path> finishWritingAndClose() throws IOException {
    boolean isSuccessful = false;
    List<Path> result = null;
    try {
      isSuccessful = finishWriting(false);
    } finally {
      result = close();
      List<IOException> thrown = closeLogWriters(null);
      if (CollectionUtils.isNotEmpty(thrown)) {
        throw MultipleIOException.createIOException(thrown);
      }
    }
    if (isSuccessful) {
      splits = result;
    }
    return splits;
  }

  // delete the one with fewer wal entries
  private void deleteOneWithFewerEntries(WALSplitter.WriterAndPath wap, Path dst)
      throws IOException {
    long dstMinLogSeqNum = -1L;
    try (WAL.Reader reader = walSplitter.getWalFactory().createReader(walSplitter.walFS, dst)) {
      WAL.Entry entry = reader.next();
      if (entry != null) {
        dstMinLogSeqNum = entry.getKey().getSequenceId();
      }
    } catch (EOFException e) {
      LOG.debug("Got EOF when reading first WAL entry from {}, an empty or broken WAL file?", dst,
        e);
    }
    if (wap.minLogSeqNum < dstMinLogSeqNum) {
      LOG.warn("Found existing old edits file. It could be the result of a previous failed"
          + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
          + walFS.getFileStatus(dst).getLen());
      if (!walFS.delete(dst, false)) {
        LOG.warn("Failed deleting of old {}", dst);
        throw new IOException("Failed deleting of old " + dst);
      }
    } else {
      LOG.warn("Found existing old edits file and we have less entries. Deleting " + wap.path
          + ", length=" + walFS.getFileStatus(wap.path).getLen());
      if (!walFS.delete(wap.path, false)) {
        LOG.warn("Failed deleting of {}", wap.path);
        throw new IOException("Failed deleting of " + wap.path);
      }
    }
  }

  /**
   * Close all of the output streams.
   * @return the list of paths written.
   */
  List<Path> close() throws IOException {
    Preconditions.checkState(!closeAndCleanCompleted);

    final List<Path> paths = new ArrayList<>();
    final List<IOException> thrown = Lists.newArrayList();
    ThreadPoolExecutor closeThreadPool =
        Threads.getBoundedCachedThreadPool(numThreads, 30L, TimeUnit.SECONDS, new ThreadFactory() {
          private int count = 1;

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "split-log-closeStream-" + count++);
            return t;
          }
        });
    CompletionService<Void> completionService = new ExecutorCompletionService<>(closeThreadPool);
    boolean progress_failed;
    try {
      progress_failed = executeCloseTask(completionService, thrown, paths);
    } catch (InterruptedException e) {
      IOException iie = new InterruptedIOException();
      iie.initCause(e);
      throw iie;
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      closeThreadPool.shutdownNow();
    }
    if (!thrown.isEmpty()) {
      throw MultipleIOException.createIOException(thrown);
    }
    writersClosed = true;
    closeAndCleanCompleted = true;
    if (progress_failed) {
      return null;
    }
    return paths;
  }

  /**
   * @param completionService threadPool to execute the closing tasks
   * @param thrown store the exceptions
   * @param paths arrayList to store the paths written
   * @return if close tasks executed successful
   */
  boolean executeCloseTask(CompletionService<Void> completionService, List<IOException> thrown,
      List<Path> paths) throws InterruptedException, ExecutionException {
    for (final Map.Entry<String, WALSplitter.SinkWriter> writersEntry : writers.entrySet()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "Submitting close of " + ((WALSplitter.WriterAndPath) writersEntry.getValue()).path);
      }
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          WALSplitter.WriterAndPath wap = (WALSplitter.WriterAndPath) writersEntry.getValue();
          Path dst = closeWriter(writersEntry.getKey(), wap, thrown);
          paths.add(dst);
          return null;
        }
      });
    }
    boolean progress_failed = false;
    for (int i = 0, n = this.writers.size(); i < n; i++) {
      Future<Void> future = completionService.take();
      future.get();
      if (!progress_failed && reporter != null && !reporter.progress()) {
        progress_failed = true;
      }
    }
    return progress_failed;
  }

  Path closeWriter(String encodedRegionName, WALSplitter.WriterAndPath wap,
      List<IOException> thrown) throws IOException {
    LOG.trace("Closing {}", wap.path);
    try {
      wap.writer.close();
    } catch (IOException ioe) {
      LOG.error("Could not close log at {}", wap.path, ioe);
      thrown.add(ioe);
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed wap " + wap.path + " (wrote " + wap.editsWritten + " edits, skipped "
          + wap.editsSkipped + " edits in " + (wap.nanosSpent / 1000 / 1000) + "ms");
    }
    if (wap.editsWritten == 0) {
      // just remove the empty recovered.edits file
      if (walFS.exists(wap.path) && !walFS.delete(wap.path, false)) {
        LOG.warn("Failed deleting empty {}", wap.path);
        throw new IOException("Failed deleting empty  " + wap.path);
      }
      return null;
    }

    Path dst = getCompletedRecoveredEditsFilePath(wap.path,
      regionMaximumEditLogSeqNum.get(encodedRegionName));
    try {
      if (!dst.equals(wap.path) && walFS.exists(dst)) {
        deleteOneWithFewerEntries(wap, dst);
      }
      // Skip the unit tests which create a splitter that reads and
      // writes the data without touching disk.
      // TestHLogSplit#testThreading is an example.
      if (walFS.exists(wap.path)) {
        if (!walFS.rename(wap.path, dst)) {
          throw new IOException("Failed renaming " + wap.path + " to " + dst);
        }
        LOG.info("Rename {} to {}", wap.path, dst);
      }
    } catch (IOException ioe) {
      LOG.error("Could not rename {} to {}", wap.path, dst, ioe);
      thrown.add(ioe);
      return null;
    }
    return dst;
  }

  private List<IOException> closeLogWriters(List<IOException> thrown) throws IOException {
    if (writersClosed) {
      return thrown;
    }
    if (thrown == null) {
      thrown = Lists.newArrayList();
    }
    try {
      for (WriterThread writerThread : writerThreads) {
        while (writerThread.isAlive()) {
          writerThread.setShouldStop(true);
          writerThread.interrupt();
          try {
            writerThread.join(10);
          } catch (InterruptedException e) {
            IOException iie = new InterruptedIOException();
            iie.initCause(e);
            throw iie;
          }
        }
      }
    } finally {
      WALSplitter.WriterAndPath wap = null;
      for (WALSplitter.SinkWriter tmpWAP : writers.values()) {
        try {
          wap = (WALSplitter.WriterAndPath) tmpWAP;
          wap.writer.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close log at {}", wap.path, ioe);
          thrown.add(ioe);
          continue;
        }
        LOG.info("Closed log " + wap.path + " (wrote " + wap.editsWritten + " edits in "
            + (wap.nanosSpent / 1000 / 1000) + "ms)");
      }
      writersClosed = true;
    }

    return thrown;
  }

  /**
   * Get a writer and path for a log starting at the given entry. This function is threadsafe so
   * long as multiple threads are always acting on different regions.
   * @return null if this region shouldn't output any logs
   */
  WALSplitter.WriterAndPath getWriterAndPath(WAL.Entry entry, boolean reusable) throws IOException {
    byte[] region = entry.getKey().getEncodedRegionName();
    String regionName = Bytes.toString(region);
    WALSplitter.WriterAndPath ret = (WALSplitter.WriterAndPath) writers.get(regionName);
    if (ret != null) {
      return ret;
    }
    // If we already decided that this region doesn't get any output
    // we don't need to check again.
    if (blacklistedRegions.contains(region)) {
      return null;
    }
    ret = createWAP(region, entry);
    if (ret == null) {
      blacklistedRegions.add(region);
      return null;
    }
    if (reusable) {
      writers.put(regionName, ret);
    }
    return ret;
  }

  /**
   * @return a path with a write for that path. caller should close.
   */
  WALSplitter.WriterAndPath createWAP(byte[] region, WAL.Entry entry) throws IOException {
    String tmpDirName = walSplitter.conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
      HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    Path regionedits = getRegionSplitEditsPath(entry,
      walSplitter.getFileBeingSplit().getPath().getName(), tmpDirName, conf);
    if (regionedits == null) {
      return null;
    }
    FileSystem walFs = FSUtils.getWALFileSystem(conf);
    if (walFs.exists(regionedits)) {
      LOG.warn("Found old edits file. It could be the "
          + "result of a previous failed split attempt. Deleting " + regionedits + ", length="
          + walFs.getFileStatus(regionedits).getLen());
      if (!walFs.delete(regionedits, false)) {
        LOG.warn("Failed delete of old {}", regionedits);
      }
    }
    WALProvider.Writer w = walSplitter.createWriter(regionedits);
    LOG.debug("Creating writer path={}", regionedits);
    return new WALSplitter.WriterAndPath(regionedits, w, entry.getKey().getSequenceId());
  }



  void filterCellByStore(WAL.Entry logEntry) {
    Map<byte[], Long> maxSeqIdInStores = walSplitter.getRegionMaxSeqIdInStores()
        .get(Bytes.toString(logEntry.getKey().getEncodedRegionName()));
    if (MapUtils.isEmpty(maxSeqIdInStores)) {
      return;
    }
    // Create the array list for the cells that aren't filtered.
    // We make the assumption that most cells will be kept.
    ArrayList<Cell> keptCells = new ArrayList<>(logEntry.getEdit().getCells().size());
    for (Cell cell : logEntry.getEdit().getCells()) {
      if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
        keptCells.add(cell);
      } else {
        byte[] family = CellUtil.cloneFamily(cell);
        Long maxSeqId = maxSeqIdInStores.get(family);
        // Do not skip cell even if maxSeqId is null. Maybe we are in a rolling upgrade,
        // or the master was crashed before and we can not get the information.
        if (maxSeqId == null || maxSeqId.longValue() < logEntry.getKey().getSequenceId()) {
          keptCells.add(cell);
        }
      }
    }

    // Anything in the keptCells array list is still live.
    // So rather than removing the cells from the array list
    // which would be an O(n^2) operation, we just replace the list
    logEntry.getEdit().setCells(keptCells);
  }

  @Override
  public void append(WALSplitter.RegionEntryBuffer buffer) throws IOException {
    appendBuffer(buffer, true);
  }

  WALSplitter.WriterAndPath appendBuffer(WALSplitter.RegionEntryBuffer buffer, boolean reusable)
      throws IOException {
    List<WAL.Entry> entries = buffer.entryBuffer;
    if (entries.isEmpty()) {
      LOG.warn("got an empty buffer, skipping");
      return null;
    }

    WALSplitter.WriterAndPath wap = null;

    long startTime = System.nanoTime();
    try {
      int editsCount = 0;

      for (WAL.Entry logEntry : entries) {
        if (wap == null) {
          wap = getWriterAndPath(logEntry, reusable);
          if (wap == null) {
            // This log spews the full edit. Can be massive in the log. Enable only debugging
            // WAL lost edit issues.
            LOG.trace("getWriterAndPath decided we don't need to write edits for {}", logEntry);
            return null;
          }
        }
        filterCellByStore(logEntry);
        if (!logEntry.getEdit().isEmpty()) {
          wap.writer.append(logEntry);
          this.updateRegionMaximumEditLogSeqNum(logEntry);
          editsCount++;
        } else {
          wap.incrementSkippedEdits(1);
        }
      }
      // Pass along summary statistics
      wap.incrementEdits(editsCount);
      wap.incrementNanoTime(System.nanoTime() - startTime);
    } catch (IOException e) {
      e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
      LOG.error(HBaseMarkers.FATAL, "Got while writing log entry to log", e);
      throw e;
    }
    return wap;
  }

  @Override
  public boolean keepRegionEvent(WAL.Entry entry) {
    ArrayList<Cell> cells = entry.getEdit().getCells();
    for (Cell cell : cells) {
      if (WALEdit.isCompactionMarker(cell)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return a map from encoded region ID to the number of edits written out for that region.
   */
  @Override
  public Map<byte[], Long> getOutputCounts() {
    TreeMap<byte[], Long> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<String, WALSplitter.SinkWriter> entry : writers.entrySet()) {
      ret.put(Bytes.toBytes(entry.getKey()), entry.getValue().editsWritten);
    }
    return ret;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return writers.size();
  }
}
