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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateConstants;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControlUtil;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Store flusher interface. Turns a snapshot of memstore into a set of store files (usually one).
 * Custom implementation can be provided.
 */
@InterfaceAudience.Private
abstract class StoreFlusher {
  protected Configuration conf;
  protected HStore store;

  public StoreFlusher(Configuration conf, HStore store) {
    this.conf = conf;
    this.store = store;
  }

  /**
   * Turns a snapshot of memstore into a set of store files.
   * @param snapshot Memstore snapshot.
   * @param cacheFlushSeqNum Log cache flush sequence number.
   * @param status Task that represents the flush operation and may be updated with status.
   * @param throughputController A controller to avoid flush too fast
   * @return List of files written. Can be empty; must not be null.
   */
  public abstract List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
    MonitoredTask status, ThroughputController throughputController, FlushLifeCycleTracker tracker,
    Consumer<Path> writerCreationTracker) throws IOException;

  protected void finalizeWriter(StoreFileWriter writer, long cacheFlushSeqNum,
      MonitoredTask status) throws IOException {
    // Write out the log sequence number that corresponds to this output
    // hfile. Also write current time in metadata as minFlushTime.
    // The hfile is current up to and including cacheFlushSeqNum.
    status.setStatus("Flushing " + store + ": appending metadata");
    writer.appendMetadata(cacheFlushSeqNum, false);
    status.setStatus("Flushing " + store + ": closing flushed file");
    writer.close();
  }

  protected final StoreFileWriter createWriter(MemStoreSnapshot snapshot, boolean alwaysIncludesTag,
    Consumer<Path> writerCreationTracker) throws IOException {
    return store.getStoreEngine()
      .createWriter(
        CreateStoreFileWriterParams.create()
          .maxKeyCount(snapshot.getCellsCount())
          .compression(store.getColumnFamilyDescriptor().getCompressionType())
          .isCompaction(false)
          .includeMVCCReadpoint(true)
          .includesTag(alwaysIncludesTag || snapshot.isTagsPresent())
          .shouldDropBehind(false).writerCreationTracker(writerCreationTracker));
  }

  /**
   * Creates the scanner for flushing snapshot. Also calls coprocessors.
   * @return The scanner; null if coprocessor is canceling the flush.
   */
  protected final InternalScanner createScanner(List<KeyValueScanner> snapshotScanners,
    FlushLifeCycleTracker tracker) throws IOException {
    ScanInfo scanInfo;
    if (store.getCoprocessorHost() != null) {
      scanInfo = store.getCoprocessorHost().preFlushScannerOpen(store, tracker);
    } else {
      scanInfo = store.getScanInfo();
    }
    final long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = new StoreScanner(store, scanInfo, snapshotScanners,
        ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint, PrivateConstants.OLDEST_TIMESTAMP);

    if (store.getCoprocessorHost() != null) {
      try {
        return store.getCoprocessorHost().preFlush(store, scanner, tracker);
      } catch (IOException ioe) {
        scanner.close();
        throw ioe;
      }
    }
    return scanner;
  }

  /**
   * Performs memstore flush, writing data from scanner into sink.
   * @param scanner Scanner to get data from.
   * @param sink Sink to write data to. Could be StoreFile.Writer.
   * @param throughputController A controller to avoid flush too fast
   */
  protected void performFlush(InternalScanner scanner, CellSink sink,
      ThroughputController throughputController) throws IOException {
    int compactionKVMax =
        conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    List<Cell> kvs = new ArrayList<>();
    boolean hasMore;
    String flushName = ThroughputControlUtil.getNameForThrottling(store, "flush");
    // no control on system table (such as meta, namespace, etc) flush
    boolean control =
        throughputController != null && !store.getRegionInfo().getTable().isSystemTable();
    if (control) {
      throughputController.start(flushName);
    }
    try {
      do {
        hasMore = scanner.next(kvs, scannerContext);
        if (!kvs.isEmpty()) {
          for (Cell c : kvs) {
            sink.append(c);
            if (control) {
              throughputController.control(flushName, c.getSerializedSize());
            }
          }
          kvs.clear();
        }
      } while (hasMore);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(
          "Interrupted while control throughput of flushing " + flushName);
    } finally {
      if (control) {
        throughputController.finish(flushName);
      }
    }
  }
}
