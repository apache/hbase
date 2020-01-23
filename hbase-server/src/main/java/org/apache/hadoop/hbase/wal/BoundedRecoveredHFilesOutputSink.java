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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.CellSet;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.EntryBuffers.RegionEntryBuffer;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class BoundedRecoveredHFilesOutputSink extends OutputSink {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedRecoveredHFilesOutputSink.class);

  public static final String WAL_SPLIT_TO_HFILE = "hbase.wal.split.to.hfile";
  public static final boolean DEFAULT_WAL_SPLIT_TO_HFILE = false;

  private final WALSplitter walSplitter;
  private final Map<TableName, TableDescriptor> tableDescCache;
  private Connection connection;
  private Admin admin;
  private FileSystem rootFS;

  // Since the splitting process may create multiple output files, we need a map
  // to track the output count of each region.
  private ConcurrentMap<String, Long> regionEditsWrittenMap = new ConcurrentHashMap<>();
  // Need a counter to track the opening writers.
  private final AtomicInteger openingWritersNum = new AtomicInteger(0);

  public BoundedRecoveredHFilesOutputSink(WALSplitter walSplitter,
    WALSplitter.PipelineController controller, EntryBuffers entryBuffers, int numWriters) {
    super(controller, entryBuffers, numWriters);
    this.walSplitter = walSplitter;
    tableDescCache = new HashMap<>();
  }

  @Override
  public void startWriterThreads() throws IOException {
    connection = ConnectionFactory.createConnection(walSplitter.conf);
    admin = connection.getAdmin();
    rootFS = FSUtils.getRootDirFileSystem(walSplitter.conf);
    super.startWriterThreads();
  }

  @Override
  public void append(RegionEntryBuffer buffer) throws IOException {
    Map<String, CellSet> familyCells = new HashMap<>();
    Map<String, Long> familySeqIds = new HashMap<>();
    boolean isMetaTable = buffer.tableName.equals(META_TABLE_NAME);
    for (WAL.Entry entry : buffer.entries) {
      long seqId = entry.getKey().getSequenceId();
      List<Cell> cells = entry.getEdit().getCells();
      for (Cell cell : cells) {
        if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
          continue;
        }
        String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
        // comparator need to be specified for meta
        familyCells.computeIfAbsent(familyName, key -> new CellSet(
          isMetaTable ? CellComparatorImpl.META_COMPARATOR : CellComparator.getInstance()))
          .add(cell);
        familySeqIds.compute(familyName, (k, v) -> v == null ? seqId : Math.max(v, seqId));
      }
    }

    // The key point is create a new writer for each column family, write edits then close writer.
    String regionName = Bytes.toString(buffer.encodedRegionName);
    for (Map.Entry<String, CellSet> cellsEntry : familyCells.entrySet()) {
      String familyName = cellsEntry.getKey();
      StoreFileWriter writer = createRecoveredHFileWriter(buffer.tableName, regionName,
        familySeqIds.get(familyName), familyName, isMetaTable);
      openingWritersNum.incrementAndGet();
      try {
        for (Cell cell : cellsEntry.getValue()) {
          writer.append(cell);
        }
        regionEditsWrittenMap.compute(Bytes.toString(buffer.encodedRegionName),
          (k, v) -> v == null ? buffer.entries.size() : v + buffer.entries.size());
        splits.add(writer.getPath());
        openingWritersNum.decrementAndGet();
      } finally {
        writer.close();
      }
    }
  }

  @Override
  public List<Path> close() throws IOException {
    boolean isSuccessful = true;
    try {
      isSuccessful &= finishWriterThreads(false);
    } finally {
      isSuccessful &= writeRemainingEntryBuffers();
    }
    IOUtils.closeQuietly(admin);
    IOUtils.closeQuietly(connection);
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

  private StoreFileWriter createRecoveredHFileWriter(TableName tableName, String regionName,
      long seqId, String familyName, boolean isMetaTable) throws IOException {
    Path outputFile = WALSplitUtil
      .getRegionRecoveredHFilePath(tableName, regionName, familyName, seqId,
        walSplitter.getFileBeingSplit().getPath().getName(), walSplitter.conf, rootFS);
    checkPathValid(outputFile);
    StoreFileWriter.Builder writerBuilder =
        new StoreFileWriter.Builder(walSplitter.conf, CacheConfig.DISABLED, rootFS)
            .withFilePath(outputFile);
    HFileContextBuilder hFileContextBuilder = new HFileContextBuilder();
    if (isMetaTable) {
      hFileContextBuilder.withCellComparator(CellComparatorImpl.META_COMPARATOR);
    } else {
      configContextForNonMetaWriter(tableName, familyName, hFileContextBuilder, writerBuilder);
    }
    return writerBuilder.withFileContext(hFileContextBuilder.build()).build();
  }

  private void configContextForNonMetaWriter(TableName tableName, String familyName,
      HFileContextBuilder hFileContextBuilder, StoreFileWriter.Builder writerBuilder)
      throws IOException {
    if (!tableDescCache.containsKey(tableName)) {
      tableDescCache.put(tableName, admin.getDescriptor(tableName));
    }
    TableDescriptor tableDesc = tableDescCache.get(tableName);
    ColumnFamilyDescriptor cfd = tableDesc.getColumnFamily(Bytes.toBytesBinary(familyName));
    hFileContextBuilder.withCompression(cfd.getCompressionType()).withBlockSize(cfd.getBlocksize())
        .withCompressTags(cfd.isCompressTags()).withDataBlockEncoding(cfd.getDataBlockEncoding())
        .withCellComparator(CellComparatorImpl.COMPARATOR);
    writerBuilder.withBloomType(cfd.getBloomFilterType());
  }

  private void checkPathValid(Path outputFile) throws IOException {
    if (rootFS.exists(outputFile)) {
      LOG.warn("this file {} may be left after last failed split ", outputFile);
      if (!rootFS.delete(outputFile, false)) {
        LOG.warn("delete old generated HFile {} failed", outputFile);
      }
    }
  }
}
