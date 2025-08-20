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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.wal.WALSplitUtil.getCompletedRecoveredEditsFilePath;
import static org.apache.hadoop.hbase.wal.WALSplitUtil.getRegionSplitEditsPath;

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

@InterfaceAudience.Private
abstract class AbstractRecoveredEditsOutputSink extends OutputSink {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveredEditsOutputSink.class);
  private final WALSplitter walSplitter;
  private final ConcurrentMap<String, Long> regionMaximumEditLogSeqNum = new ConcurrentHashMap<>();
  private static final int MAX_RENAME_RETRY_COUNT = 5;

  public AbstractRecoveredEditsOutputSink(WALSplitter walSplitter,
    WALSplitter.PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
    super(controller, entryBuffers, numWriters);
    this.walSplitter = walSplitter;
  }

  /** Returns a writer that wraps a {@link WALProvider.Writer} and its Path. Caller should close. */
  protected RecoveredEditsWriter createRecoveredEditsWriter(TableName tableName, byte[] region,
    long seqId) throws IOException {
    // If multiple worker are splitting a WAL at a same time, both should use unique file name to
    // avoid conflict
    Path regionEditsPath = getRegionSplitEditsPath(tableName, region, seqId,
      walSplitter.getFileBeingSplit().getPath().getName(), walSplitter.getTmpDirName(),
      walSplitter.conf, getWorkerNameComponent());

    if (walSplitter.walFS.exists(regionEditsPath)) {
      LOG.warn("Found old edits file. It could be the "
        + "result of a previous failed split attempt. Deleting " + regionEditsPath + ", length="
        + walSplitter.walFS.getFileStatus(regionEditsPath).getLen());
      if (!walSplitter.walFS.delete(regionEditsPath, false)) {
        LOG.warn("Failed delete of old {}", regionEditsPath);
      }
    }
    WALProvider.Writer w = walSplitter.createWriter(regionEditsPath);
    final String msg = "Creating recovered edits writer path=" + regionEditsPath;
    LOG.info(msg);
    updateStatusWithMsg(msg);
    return new RecoveredEditsWriter(region, regionEditsPath, w, seqId);
  }

  private String getWorkerNameComponent() {
    if (walSplitter.rsServices == null) {
      return "";
    }
    try {
      return URLEncoder.encode(
        walSplitter.rsServices.getServerName().toShortString()
          .replace(Addressing.HOSTNAME_PORT_SEPARATOR, ServerName.SERVERNAME_SEPARATOR),
        StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("URLEncoder doesn't support UTF-8", e);
    }
  }

  /**
   * abortRecoveredEditsWriter closes the editsWriter, but does not rename and finalize the
   * recovered edits WAL files. Please see HBASE-28569.
   */
  protected void abortRecoveredEditsWriter(RecoveredEditsWriter editsWriter,
    List<IOException> thrown) {
    closeRecoveredEditsWriter(editsWriter, thrown);
    try {
      removeRecoveredEditsFile(editsWriter);
    } catch (IOException ioe) {
      final String errorMsg = "Failed removing recovered edits file at " + editsWriter.path;
      LOG.error(errorMsg);
      updateStatusWithMsg(errorMsg);
    }
  }

  protected Path closeRecoveredEditsWriterAndFinalizeEdits(RecoveredEditsWriter editsWriter,
    List<IOException> thrown) throws IOException {
    if (!closeRecoveredEditsWriter(editsWriter, thrown)) {
      return null;
    }
    if (editsWriter.editsWritten == 0) {
      // just remove the empty recovered.edits file
      removeRecoveredEditsFile(editsWriter);
      return null;
    }

    Path dst = getCompletedRecoveredEditsFilePath(editsWriter.path,
      regionMaximumEditLogSeqNum.get(Bytes.toString(editsWriter.encodedRegionName)));
    try {
      // Skip the unit tests which create a splitter that reads and
      // writes the data without touching disk.
      // TestHLogSplit#testThreading is an example.
      if (walSplitter.walFS.exists(editsWriter.path)) {
        boolean retry;
        int retryCount = 0;
        do {
          retry = false;
          retryCount++;
          // If rename is successful, it means recovered edits are successfully places at right
          // place but if rename fails, there can be below reasons
          // 1. dst already exist - in this case if dst have desired edits we will keep the dst,
          // delete the editsWriter.path and consider this success else if dst have fewer edits, we
          // will delete the dst and retry the rename
          // 2. parent directory does not exit - in one edge case this is possible when this worker
          // got stuck before rename and HMaster get another worker to split the wal, SCP will
          // proceed and once region get opened on one RS, we delete the recovered.edits directory,
          // in this case there is no harm in failing this procedure after retry exhausted.
          if (!walSplitter.walFS.rename(editsWriter.path, dst)) {
            retry = deleteOneWithFewerEntriesToRetry(editsWriter, dst);
          }
        } while (retry && retryCount < MAX_RENAME_RETRY_COUNT);

        // If we are out of loop with retry flag `true` it means we have exhausted the retries.
        if (retry) {
          final String errorMsg = "Failed renaming recovered edits " + editsWriter.path + " to "
            + dst + " in " + MAX_RENAME_RETRY_COUNT + " retries";
          updateStatusWithMsg(errorMsg);
          throw new IOException(errorMsg);
        } else {
          final String renameEditMsg = "Rename recovered edits " + editsWriter.path + " to " + dst;
          LOG.info(renameEditMsg);
          updateStatusWithMsg(renameEditMsg);
        }
      }
    } catch (IOException ioe) {
      final String errorMsg = "Could not rename recovered edits " + editsWriter.path + " to " + dst;
      LOG.error(errorMsg, ioe);
      updateStatusWithMsg(errorMsg);
      thrown.add(ioe);
      return null;
    }
    return dst;
  }

  private boolean closeRecoveredEditsWriter(RecoveredEditsWriter editsWriter,
    List<IOException> thrown) {
    try {
      editsWriter.writer.close();
    } catch (IOException ioe) {
      final String errorMsg = "Could not close recovered edits at " + editsWriter.path;
      LOG.error(errorMsg, ioe);
      updateStatusWithMsg(errorMsg);
      thrown.add(ioe);
      return false;
    }
    final String msg = "Closed recovered edits writer path=" + editsWriter.path + " (wrote "
      + editsWriter.editsWritten + " edits, skipped " + editsWriter.editsSkipped + " edits in "
      + (editsWriter.nanosSpent / 1000 / 1000) + " ms)";
    LOG.info(msg);
    updateStatusWithMsg(msg);
    return true;
  }

  private void removeRecoveredEditsFile(RecoveredEditsWriter editsWriter) throws IOException {
    if (
      walSplitter.walFS.exists(editsWriter.path)
        && !walSplitter.walFS.delete(editsWriter.path, false)
    ) {
      final String errorMsg = "Failed deleting empty " + editsWriter.path;
      LOG.warn(errorMsg);
      updateStatusWithMsg(errorMsg);
      throw new IOException("Failed deleting empty  " + editsWriter.path);
    }
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
   * Update region's maximum edit log SeqNum.
   */
  void updateRegionMaximumEditLogSeqNum(WAL.Entry entry) {
    synchronized (regionMaximumEditLogSeqNum) {
      String regionName = Bytes.toString(entry.getKey().getEncodedRegionName());
      Long currentMaxSeqNum = regionMaximumEditLogSeqNum.get(regionName);
      if (currentMaxSeqNum == null || entry.getKey().getSequenceId() > currentMaxSeqNum) {
        regionMaximumEditLogSeqNum.put(regionName, entry.getKey().getSequenceId());
      }
    }
  }

  // delete the one with fewer wal entries
  private boolean deleteOneWithFewerEntriesToRetry(RecoveredEditsWriter editsWriter, Path dst)
    throws IOException {
    if (!walSplitter.walFS.exists(dst)) {
      LOG.info("dst {} doesn't exist, need to retry ", dst);
      return true;
    }

    if (isDstHasFewerEntries(editsWriter, dst)) {
      LOG.warn("Found existing old edits file. It could be the result of a previous failed"
        + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
        + walSplitter.walFS.getFileStatus(dst).getLen() + " and retry is needed");
      if (!walSplitter.walFS.delete(dst, false)) {
        LOG.warn("Failed deleting of old {}", dst);
        throw new IOException("Failed deleting of old " + dst);
      }
      return true;
    } else {
      LOG
        .warn("Found existing old edits file and we have less entries. Deleting " + editsWriter.path
          + ", length=" + walSplitter.walFS.getFileStatus(editsWriter.path).getLen()
          + " and no retry needed as dst has all edits");
      if (!walSplitter.walFS.delete(editsWriter.path, false)) {
        LOG.warn("Failed deleting of {}", editsWriter.path);
        throw new IOException("Failed deleting of " + editsWriter.path);
      }
      return false;
    }
  }

  private boolean isDstHasFewerEntries(RecoveredEditsWriter editsWriter, Path dst)
    throws IOException {
    long dstMinLogSeqNum = -1L;
    try (WALStreamReader reader =
      walSplitter.getWalFactory().createStreamReader(walSplitter.walFS, dst)) {
      WAL.Entry entry = reader.next();
      if (entry != null) {
        dstMinLogSeqNum = entry.getKey().getSequenceId();
      }
    } catch (EOFException e) {
      LOG.debug("Got EOF when reading first WAL entry from {}, an empty or broken WAL file?", dst,
        e);
    }
    return editsWriter.minLogSeqNum < dstMinLogSeqNum;
  }

  /**
   * Private data structure that wraps a {@link WALProvider.Writer} and its Path, also collecting
   * statistics about the data written to this output.
   */
  final class RecoveredEditsWriter {
    /* Count of edits written to this path */
    long editsWritten = 0;
    /* Count of edits skipped to this path */
    long editsSkipped = 0;
    /* Number of nanos spent writing to this log */
    long nanosSpent = 0;

    final byte[] encodedRegionName;
    final Path path;
    final WALProvider.Writer writer;
    final long minLogSeqNum;

    RecoveredEditsWriter(byte[] encodedRegionName, Path path, WALProvider.Writer writer,
      long minLogSeqNum) {
      this.encodedRegionName = encodedRegionName;
      this.path = path;
      this.writer = writer;
      this.minLogSeqNum = minLogSeqNum;
    }

    private void incrementEdits(int edits) {
      editsWritten += edits;
    }

    private void incrementSkippedEdits(int skipped) {
      editsSkipped += skipped;
      totalSkippedEdits.addAndGet(skipped);
    }

    private void incrementNanoTime(long nanos) {
      nanosSpent += nanos;
    }

    void writeRegionEntries(List<WAL.Entry> entries) throws IOException {
      long startTime = System.nanoTime();
      int editsCount = 0;
      for (WAL.Entry logEntry : entries) {
        filterCellByStore(logEntry);
        if (!logEntry.getEdit().isEmpty()) {
          try {
            writer.append(logEntry);
          } catch (IOException e) {
            logAndThrowWriterAppendFailure(logEntry, e);
          }
          updateRegionMaximumEditLogSeqNum(logEntry);
          editsCount++;
        } else {
          incrementSkippedEdits(1);
        }
      }
      // Pass along summary statistics
      incrementEdits(editsCount);
      incrementNanoTime(System.nanoTime() - startTime);
    }

    private void logAndThrowWriterAppendFailure(WAL.Entry logEntry, IOException e)
      throws IOException {
      e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
      final String errorMsg = "Failed to write log entry " + logEntry.toString() + " to log";
      LOG.error(HBaseMarkers.FATAL, errorMsg, e);
      updateStatusWithMsg(errorMsg);
      throw e;
    }

    private void filterCellByStore(WAL.Entry logEntry) {
      Map<byte[], Long> maxSeqIdInStores = walSplitter.getRegionMaxSeqIdInStores()
        .get(Bytes.toString(logEntry.getKey().getEncodedRegionName()));
      if (MapUtils.isEmpty(maxSeqIdInStores)) {
        return;
      }
      // Create the array list for the cells that aren't filtered.
      // We make the assumption that most cells will be kept.
      ArrayList<Cell> keptCells = new ArrayList<>(logEntry.getEdit().getCells().size());
      for (Cell cell : logEntry.getEdit().getCells()) {
        if (WALEdit.isMetaEditFamily(cell)) {
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
  }
}
