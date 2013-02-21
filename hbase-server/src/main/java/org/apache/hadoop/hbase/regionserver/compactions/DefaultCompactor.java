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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Compact passed set of files. Create an instance and then call {@link #compact(CompactionRequest)}
 */
@InterfaceAudience.Private
public class DefaultCompactor extends Compactor {
  private static final Log LOG = LogFactory.getLog(DefaultCompactor.class);
  private final Store store;

  public DefaultCompactor(final Configuration conf, final Store store) {
    super(conf);
    this.store = store;
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   */
  @SuppressWarnings("deprecation")
  @Override
  public List<Path> compact(final CompactionRequest request) throws IOException {
    final Collection<StoreFile> filesToCompact = request.getFiles();
    boolean majorCompaction = request.isMajor();
    // Max-sequenceID is the last key in the files we're compacting
    long maxId = StoreFile.getMaxSequenceIdInList(filesToCompact, true);

    // Calculate maximum key count after compaction (for blooms)
    // Also calculate earliest put timestamp if major compaction
    int maxKeyCount = 0;
    long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    for (StoreFile file: filesToCompact) {
      StoreFile.Reader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: getFilterEntries could cause under-sized blooms if the user
      // switches bloom type (e.g. from ROW to ROWCOL)
      long keyCount = (r.getBloomFilterType() == store.getFamily().getBloomFilterType())?
        r.getFilterEntries() : r.getEntries();
      maxKeyCount += keyCount;
      // For major compactions calculate the earliest put timestamp of all
      // involved storefiles. This is used to remove family delete marker during
      // compaction.
      if (majorCompaction) {
        byte [] tmp = r.loadFileInfo().get(StoreFile.EARLIEST_PUT_TS);
        if (tmp == null) {
          // There's a file with no information, must be an old one
          // assume we have very old puts
          earliestPutTs = HConstants.OLDEST_TIMESTAMP;
        } else {
          earliestPutTs = Math.min(earliestPutTs, Bytes.toLong(tmp));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Compacting " + file +
          ", keycount=" + keyCount +
          ", bloomtype=" + r.getBloomFilterType().toString() +
          ", size=" + StringUtils.humanReadableInt(r.length()) +
          ", encoding=" + r.getHFileReader().getEncodingOnDisk() +
          (majorCompaction? ", earliestPutTs=" + earliestPutTs: ""));
      }
    }

    // keep track of compaction progress
    this.progress = new CompactionProgress(maxKeyCount);

    // For each file, obtain a scanner:
    List<StoreFileScanner> scanners = StoreFileScanner
      .getScannersForStoreFiles(filesToCompact, false, false, true);

    // Get some configs
    int compactionKVMax = this.conf.getInt(HConstants.COMPACTION_KV_MAX, 10);
    Compression.Algorithm compression = store.getFamily().getCompression();
    // Avoid overriding compression setting for major compactions if the user
    // has not specified it separately
    Compression.Algorithm compactionCompression =
      (store.getFamily().getCompactionCompression() != Compression.Algorithm.NONE) ?
      store.getFamily().getCompactionCompression(): compression;
    // Make the instantiation lazy in case compaction produces no product; i.e.
    // where all source cells are expired or deleted.
    StoreFile.Writer writer = null;
    List<Path> newFiles = new ArrayList<Path>();
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = store.getSmallestReadPoint();
    MultiVersionConsistencyControl.setThreadReadPoint(smallestReadPoint);
    try {
      InternalScanner scanner = null;
      try {
        if (store.getCoprocessorHost() != null) {
          scanner = store
              .getCoprocessorHost()
              .preCompactScannerOpen(store, scanners,
                majorCompaction ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT, earliestPutTs,
                request);
        }
        ScanType scanType = majorCompaction? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT;
        if (scanner == null) {
          Scan scan = new Scan();
          scan.setMaxVersions(store.getFamily().getMaxVersions());
          /* Include deletes, unless we are doing a major compaction */
          scanner = new StoreScanner(store, store.getScanInfo(), scan, scanners,
            scanType, smallestReadPoint, earliestPutTs);
        }
        if (store.getCoprocessorHost() != null) {
          InternalScanner cpScanner = store.getCoprocessorHost().preCompact(store, scanner,
            scanType, request);
          // NULL scanner returned from coprocessor hooks means skip normal processing
          if (cpScanner == null) {
            return newFiles; // an empty list
          }
          scanner = cpScanner;
        }

        int bytesWritten = 0;
        // Since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
        int closeCheckInterval = HStore.getCloseCheckInterval();
        boolean hasMore;
        do {
          hasMore = scanner.next(kvs, compactionKVMax);
          // Create the writer even if no kv(Empty store file is also ok),
          // because we need record the max seq id for the store file, see
          // HBASE-6059
          if (writer == null) {
            writer = store.createWriterInTmp(maxKeyCount, compactionCompression, true);
          }
          if (writer != null) {
            // output to writer:
            for (KeyValue kv : kvs) {
              if (kv.getMemstoreTS() <= smallestReadPoint) {
                kv.setMemstoreTS(0);
              }
              writer.append(kv);
              // update progress per key
              ++progress.currentCompactedKVs;

              // check periodically to see if a system stop is requested
              if (closeCheckInterval > 0) {
                bytesWritten += kv.getLength();
                if (bytesWritten > closeCheckInterval) {
                  bytesWritten = 0;
                  isInterrupted(store, writer);
                }
              }
            }
          }
          kvs.clear();
        } while (hasMore);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } finally {
      if (writer != null) {
        writer.appendMetadata(maxId, majorCompaction);
        writer.close();
        newFiles.add(writer.getPath());
      }
    }
    return newFiles;
  }

  void isInterrupted(final Store store, final StoreFile.Writer writer)
  throws IOException {
    if (store.areWritesEnabled()) return;
    // Else cleanup.
    writer.close();
    store.getFileSystem().delete(writer.getPath(), false);
    throw new InterruptedIOException( "Aborting compaction of store " + store +
      " in region " + store.getRegionInfo().getRegionNameAsString() +
      " because it was interrupted.");
  }
}
