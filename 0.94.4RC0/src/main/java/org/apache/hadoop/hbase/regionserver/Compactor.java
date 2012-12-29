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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Compact passed set of files.
 * Create an instance and then call {@ink #compact(Store, Collection, boolean, long)}.
 */
@InterfaceAudience.Private
class Compactor extends Configured {
  private static final Log LOG = LogFactory.getLog(Compactor.class);
  private CompactionProgress progress;

  Compactor(final Configuration c) {
    super(c);
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   *
   * @param store Store the files belong to
   * @param filesToCompact which files to compact
   * @param majorCompaction true to major compact (prune all deletes, max versions, etc)
   * @param maxId Readers maximum sequence id.
   * @return Product of compaction or null if all cells expired or deleted and
   * nothing made it through the compaction.
   * @throws IOException
   */
  StoreFile.Writer compact(final Store store,
      final Collection<StoreFile> filesToCompact,
      final boolean majorCompaction, final long maxId)
  throws IOException {
    // Calculate maximum key count after compaction (for blooms)
    // Also calculate earliest put timestamp if major compaction
    int maxKeyCount = 0;
    long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    for (StoreFile file : filesToCompact) {
      StoreFile.Reader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: getFilterEntries could cause under-sized blooms if the user
      //       switches bloom type (e.g. from ROW to ROWCOL)
      long keyCount = (r.getBloomFilterType() == store.getFamily().getBloomFilterType()) ?
          r.getFilterEntries() : r.getEntries();
      maxKeyCount += keyCount;
      // For major compactions calculate the earliest put timestamp
      // of all involved storefiles. This is used to remove 
      // family delete marker during the compaction.
      if (majorCompaction) {
        byte[] tmp = r.loadFileInfo().get(StoreFile.EARLIEST_PUT_TS);
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
    // Get some configs
    int compactionKVMax = getConf().getInt("hbase.hstore.compaction.kv.max", 10);
    Compression.Algorithm compression = store.getFamily().getCompression();
    // Avoid overriding compression setting for major compactions if the user
    // has not specified it separately
    Compression.Algorithm compactionCompression =
      (store.getFamily().getCompactionCompression() != Compression.Algorithm.NONE) ?
      store.getFamily().getCompactionCompression(): compression;

    // For each file, obtain a scanner:
    List<StoreFileScanner> scanners = StoreFileScanner
      .getScannersForStoreFiles(filesToCompact, false, false, true);

    // Make the instantiation lazy in case compaction produces no product; i.e.
    // where all source cells are expired or deleted.
    StoreFile.Writer writer = null;
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = store.getHRegion().getSmallestReadPoint();
    MultiVersionConsistencyControl.setThreadReadPoint(smallestReadPoint);
    try {
      InternalScanner scanner = null;
      try {
        if (store.getHRegion().getCoprocessorHost() != null) {
          scanner = store.getHRegion()
              .getCoprocessorHost()
              .preCompactScannerOpen(store, scanners,
                  majorCompaction ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT, earliestPutTs);
        }
        if (scanner == null) {
          Scan scan = new Scan();
          scan.setMaxVersions(store.getFamily().getMaxVersions());
          /* Include deletes, unless we are doing a major compaction */
          scanner = new StoreScanner(store, store.getScanInfo(), scan, scanners,
            majorCompaction? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT,
            smallestReadPoint, earliestPutTs);
        }
        if (store.getHRegion().getCoprocessorHost() != null) {
          InternalScanner cpScanner =
            store.getHRegion().getCoprocessorHost().preCompact(store, scanner);
          // NULL scanner returned from coprocessor hooks means skip normal processing
          if (cpScanner == null) {
            return null;
          }
          scanner = cpScanner;
        }

        int bytesWritten = 0;
        // since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
        boolean hasMore;
        do {
          hasMore = scanner.next(kvs, compactionKVMax);
          if (writer == null && !kvs.isEmpty()) {
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
              if (Store.closeCheckInterval > 0) {
                bytesWritten += kv.getLength();
                if (bytesWritten > Store.closeCheckInterval) {
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
      }
    }
    return writer;
  }

  void isInterrupted(final Store store, final StoreFile.Writer writer)
  throws IOException {
    if (store.getHRegion().areWritesEnabled()) return;
    // Else cleanup.
    writer.close();
    store.getFileSystem().delete(writer.getPath(), false);
    throw new InterruptedIOException( "Aborting compaction of store " + store +
      " in region " + store.getHRegion() + " because user requested stop.");
  }

  CompactionProgress getProgress() {
    return this.progress;
  }
}
