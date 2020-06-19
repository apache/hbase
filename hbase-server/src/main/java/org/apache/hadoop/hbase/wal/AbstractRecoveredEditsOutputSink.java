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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.log.HBaseMarkers;
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

  public AbstractRecoveredEditsOutputSink(WALSplitter walSplitter,
      WALSplitter.PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
    super(controller, entryBuffers, numWriters);
    this.walSplitter = walSplitter;
  }

  /**
   * @return a writer that wraps a {@link WALProvider.Writer} and its Path. Caller should close.
   */
  protected RecoveredEditsWriter createRecoveredEditsWriter(TableName tableName, byte[] region,
      long seqId) throws IOException {
    Path regionEditsPath = getRegionSplitEditsPath(tableName, region, seqId,
      walSplitter.getFileBeingSplit().getPath().getName(), walSplitter.getTmpDirName(),
      walSplitter.conf);
    if (walSplitter.walFS.exists(regionEditsPath)) {
      LOG.warn("Found old edits file. It could be the " +
        "result of a previous failed split attempt. Deleting " + regionEditsPath + ", length=" +
        walSplitter.walFS.getFileStatus(regionEditsPath).getLen());
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

  protected Path closeRecoveredEditsWriter(RecoveredEditsWriter editsWriter,
      List<IOException> thrown) throws IOException {
    try {
      editsWriter.writer.close();
    } catch (IOException ioe) {
      final String errorMsg = "Could not close recovered edits at " + editsWriter.path;
      LOG.error(errorMsg, ioe);
      updateStatusWithMsg(errorMsg);
      thrown.add(ioe);
      return null;
    }
    final String msg = "Closed recovered edits writer path=" + editsWriter.path + " (wrote "
      + editsWriter.editsWritten + " edits, skipped " + editsWriter.editsSkipped + " edits in " + (
      editsWriter.nanosSpent / 1000 / 1000) + " ms)";
    LOG.info(msg);
    updateStatusWithMsg(msg);
    if (editsWriter.editsWritten == 0) {
      // just remove the empty recovered.edits file
      if (walSplitter.walFS.exists(editsWriter.path)
          && !walSplitter.walFS.delete(editsWriter.path, false)) {
        final String errorMsg = "Failed deleting empty " + editsWriter.path;
        LOG.warn(errorMsg);
        updateStatusWithMsg(errorMsg);
        throw new IOException("Failed deleting empty  " + editsWriter.path);
      }
      return null;
    }

    Path dst = getCompletedRecoveredEditsFilePath(editsWriter.path,
      regionMaximumEditLogSeqNum.get(Bytes.toString(editsWriter.encodedRegionName)));
    try {
      if (!dst.equals(editsWriter.path) && walSplitter.walFS.exists(dst)) {
        deleteOneWithFewerEntries(editsWriter, dst);
      }
      // Skip the unit tests which create a splitter that reads and
      // writes the data without touching disk.
      // TestHLogSplit#testThreading is an example.
      if (walSplitter.walFS.exists(editsWriter.path)) {
        if (!walSplitter.walFS.rename(editsWriter.path, dst)) {
          final String errorMsg =
            "Failed renaming recovered edits " + editsWriter.path + " to " + dst;
          updateStatusWithMsg(errorMsg);
          throw new IOException(errorMsg);
        }
        final String renameEditMsg = "Rename recovered edits " + editsWriter.path + " to " + dst;
        LOG.info(renameEditMsg);
        updateStatusWithMsg(renameEditMsg);
      }
    } catch (IOException ioe) {
      final String errorMsg = "Could not rename recovered edits " + editsWriter.path
        + " to " + dst;
      LOG.error(errorMsg, ioe);
      updateStatusWithMsg(errorMsg);
      thrown.add(ioe);
      return null;
    }
    return dst;
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
  private void deleteOneWithFewerEntries(RecoveredEditsWriter editsWriter, Path dst)
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
    if (editsWriter.minLogSeqNum < dstMinLogSeqNum) {
      LOG.warn("Found existing old edits file. It could be the result of a previous failed" +
        " split attempt or we have duplicated wal entries. Deleting " + dst + ", length=" +
        walSplitter.walFS.getFileStatus(dst).getLen());
      if (!walSplitter.walFS.delete(dst, false)) {
        LOG.warn("Failed deleting of old {}", dst);
        throw new IOException("Failed deleting of old " + dst);
      }
    } else {
      LOG.warn(
        "Found existing old edits file and we have less entries. Deleting " + editsWriter.path +
          ", length=" + walSplitter.walFS.getFileStatus(editsWriter.path).getLen());
      if (!walSplitter.walFS.delete(editsWriter.path, false)) {
        LOG.warn("Failed deleting of {}", editsWriter.path);
        throw new IOException("Failed deleting of " + editsWriter.path);
      }
    }
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
