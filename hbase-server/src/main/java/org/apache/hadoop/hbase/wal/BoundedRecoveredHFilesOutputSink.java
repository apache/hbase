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

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.CellSet;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.EntryBuffers.RegionEntryBuffer;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A WALSplitter sink that outputs {@link org.apache.hadoop.hbase.io.hfile.HFile}s.
 * Runs with a bounded number of HFile writers at any one time rather than let the count run up.
 * @see BoundedRecoveredEditsOutputSink for a sink implementation that writes intermediate
 *   recovered.edits files.
 */
@InterfaceAudience.Private
public class BoundedRecoveredHFilesOutputSink extends OutputSink {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedRecoveredHFilesOutputSink.class);

  private final WALSplitter walSplitter;

  // Since the splitting process may create multiple output files, we need a map
  // to track the output count of each region.
  private ConcurrentMap<String, Long> regionEditsWrittenMap = new ConcurrentHashMap<>();
  // Need a counter to track the opening writers.
  private final AtomicInteger openingWritersNum = new AtomicInteger(0);

  public BoundedRecoveredHFilesOutputSink(WALSplitter walSplitter,
    WALSplitter.PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
    super(controller, entryBuffers, numWriters);
    this.walSplitter = walSplitter;
  }

  @Override
  public void append(RegionEntryBuffer buffer) throws IOException {
    Map<String, CellSet> familyCells = new HashMap<>();
    Map<String, Long> familySeqIds = new HashMap<>();
    boolean isMetaTable = buffer.tableName.equals(META_TABLE_NAME);
    // First iterate all Cells to find which column families are present and to stamp Cell with
    // sequence id.
    for (WAL.Entry entry : buffer.entries) {
      long seqId = entry.getKey().getSequenceId();
      List<Cell> cells = entry.getEdit().getCells();
      for (Cell cell : cells) {
        if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
          continue;
        }
        PrivateCellUtil.setSequenceId(cell, seqId);
        String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
        // comparator need to be specified for meta
        familyCells
            .computeIfAbsent(familyName,
              key -> new CellSet(
                  isMetaTable ? MetaCellComparator.META_COMPARATOR : CellComparatorImpl.COMPARATOR))
            .add(cell);
        familySeqIds.compute(familyName, (k, v) -> v == null ? seqId : Math.max(v, seqId));
      }
    }

    // Create a new hfile writer for each column family, write edits then close writer.
    String regionName = Bytes.toString(buffer.encodedRegionName);
    for (Map.Entry<String, CellSet> cellsEntry : familyCells.entrySet()) {
      String familyName = cellsEntry.getKey();
      StoreFileWriter writer = createRecoveredHFileWriter(buffer.tableName, regionName,
        familySeqIds.get(familyName), familyName, isMetaTable);
      LOG.trace("Created {}", writer.getPath());
      openingWritersNum.incrementAndGet();
      try {
        for (Cell cell : cellsEntry.getValue()) {
          writer.append(cell);
        }
        // Append the max seqid to hfile, used when recovery.
        writer.appendMetadata(familySeqIds.get(familyName), false);
        regionEditsWrittenMap.compute(Bytes.toString(buffer.encodedRegionName),
          (k, v) -> v == null ? buffer.entries.size() : v + buffer.entries.size());
        splits.add(writer.getPath());
        openingWritersNum.decrementAndGet();
      } finally {
        writer.close();
        LOG.trace("Closed {}, edits={}", writer.getPath(), familyCells.size());
      }
    }
  }

  @Override
  public List<Path> close() throws IOException {
    boolean isSuccessful = true;
    try {
      isSuccessful = finishWriterThreads(false);
    } finally {
      isSuccessful &= writeRemainingEntryBuffers();
    }
    return isSuccessful ? splits : null;
  }

  /**
   * Write out the remaining RegionEntryBuffers and close the writers.
   *
   * @return true when there is no error.
   */
  private boolean writeRemainingEntryBuffers() throws IOException {
    for (EntryBuffers.RegionEntryBuffer buffer : entryBuffers.buffers.values()) {
      closeCompletionService.submit(() -> {
        append(buffer);
        return null;
      });
    }
    boolean progressFailed = false;
    try {
      for (int i = 0, n = entryBuffers.buffers.size(); i < n; i++) {
        Future<Void> future = closeCompletionService.take();
        future.get();
        if (!progressFailed && reporter != null && !reporter.progress()) {
          progressFailed = true;
        }
      }
    } catch (InterruptedException e) {
      IOException iie = new InterruptedIOException();
      iie.initCause(e);
      throw iie;
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      closeThreadPool.shutdownNow();
    }
    return !progressFailed;
  }

  @Override
  public Map<String, Long> getOutputCounts() {
    return regionEditsWrittenMap;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return regionEditsWrittenMap.size();
  }

  @Override
  public int getNumOpenWriters() {
    return openingWritersNum.get();
  }

  @Override
  public boolean keepRegionEvent(Entry entry) {
    return false;
  }

  /**
   * @return Returns a base HFile without compressions or encodings; good enough for recovery
   *   given hfile has metadata on how it was written.
   */
  private StoreFileWriter createRecoveredHFileWriter(TableName tableName, String regionName,
      long seqId, String familyName, boolean isMetaTable) throws IOException {
    Path outputDir = WALSplitUtil.tryCreateRecoveredHFilesDir(walSplitter.rootFS, walSplitter.conf,
      tableName, regionName, familyName);
    StoreFileWriter.Builder writerBuilder =
        new StoreFileWriter.Builder(walSplitter.conf, CacheConfig.DISABLED, walSplitter.rootFS)
            .withOutputDir(outputDir);
    HFileContext hFileContext = new HFileContextBuilder().
      withChecksumType(StoreUtils.getChecksumType(walSplitter.conf)).
      withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(walSplitter.conf)).
      withCellComparator(isMetaTable?
        MetaCellComparator.META_COMPARATOR: CellComparatorImpl.COMPARATOR).build();
    return writerBuilder.withFileContext(hFileContext).build();
  }
}
