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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.CellOutputStream;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * A compactor is a compaction algorithm associated a given policy. Base class also contains
 * reusable parts for implementing compactors (what is common and what isn't is evolving).
 */
@InterfaceAudience.Private
public abstract class Compactor {
  private static final Log LOG = LogFactory.getLog(Compactor.class);
  protected CompactionProgress progress;
  protected Configuration conf;
  protected Store store;

  private int compactionKVMax;
  protected Compression.Algorithm compactionCompression;

  //TODO: depending on Store is not good but, realistically, all compactors currently do.
  Compactor(final Configuration conf, final Store store) {
    this.conf = conf;
    this.store = store;
    this.compactionKVMax =
      this.conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    this.compactionCompression = (this.store.getFamily() == null) ?
        Compression.Algorithm.NONE : this.store.getFamily().getCompactionCompression();
  }

  /**
   * TODO: Replace this with {@link CellOutputStream} when StoreFile.Writer uses cells.
   */
  public interface CellSink {
    void append(KeyValue kv) throws IOException;
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   * @param request the requested compaction
   * @return Product of compaction or an empty list if all cells expired or deleted and nothing made
   *         it through the compaction.
   * @throws IOException
   */
  public abstract List<Path> compact(final CompactionRequest request) throws IOException;

  /**
   * Compact a list of files for testing. Creates a fake {@link CompactionRequest} to pass to
   * {@link #compact(CompactionRequest)};
   * @param filesToCompact the files to compact. These are used as the compactionSelection for the
   *          generated {@link CompactionRequest}.
   * @param isMajor true to major compact (prune all deletes, max versions, etc)
   * @return Product of compaction or an empty list if all cells expired or deleted and nothing made
   *         it through the compaction.
   * @throws IOException
   */
  public List<Path> compactForTesting(final Collection<StoreFile> filesToCompact, boolean isMajor)
      throws IOException {
    CompactionRequest cr = new CompactionRequest(filesToCompact);
    cr.setIsMajor(isMajor);
    return this.compact(cr);
  }

  public CompactionProgress getProgress() {
    return this.progress;
  }

  /** The sole reason this class exists is that java has no ref/out/pointer parameters. */
  protected static class FileDetails {
    /** Maximum key count after compaction (for blooms) */
    public long maxKeyCount = 0;
    /** Earliest put timestamp if major compaction */
    public long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    /** The last key in the files we're compacting. */
    public long maxSeqId = 0;
    /** Latest memstore read point found in any of the involved files */
    public long maxMVCCReadpoint = 0;
  }

  protected FileDetails getFileDetails(
      Collection<StoreFile> filesToCompact, boolean calculatePutTs) throws IOException {
    FileDetails fd = new FileDetails();

    for (StoreFile file : filesToCompact) {
      long seqNum = file.getMaxSequenceId();
      fd.maxSeqId = Math.max(fd.maxSeqId, seqNum);
      StoreFile.Reader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: getFilterEntries could cause under-sized blooms if the user
      // switches bloom type (e.g. from ROW to ROWCOL)
      long keyCount = (r.getBloomFilterType() == store.getFamily().getBloomFilterType())
          ? r.getFilterEntries() : r.getEntries();
      fd.maxKeyCount += keyCount;
      // calculate the latest MVCC readpoint in any of the involved store files
      Map<byte[], byte[]> fileInfo = r.loadFileInfo();
      byte tmp[] = fileInfo.get(HFileWriterV2.MAX_MEMSTORE_TS_KEY);
      if (tmp != null) {
        fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, Bytes.toLong(tmp));
      }
      // If required, calculate the earliest put timestamp of all involved storefiles.
      // This is used to remove family delete marker during compaction.
      long earliestPutTs = 0;
      if (calculatePutTs) {
        tmp = fileInfo.get(StoreFile.EARLIEST_PUT_TS);
        if (tmp == null) {
          // There's a file with no information, must be an old one
          // assume we have very old puts
          fd.earliestPutTs = earliestPutTs = HConstants.OLDEST_TIMESTAMP;
        } else {
          earliestPutTs = Bytes.toLong(tmp);
          fd.earliestPutTs = Math.min(fd.earliestPutTs, earliestPutTs);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Compacting " + file +
          ", keycount=" + keyCount +
          ", bloomtype=" + r.getBloomFilterType().toString() +
          ", size=" + StringUtils.humanReadableInt(r.length()) +
          ", encoding=" + r.getHFileReader().getEncodingOnDisk() +
          ", seqNum=" + seqNum +
          (calculatePutTs ? ", earliestPutTs=" + earliestPutTs: ""));
      }
    }
    return fd;
  }

  protected List<StoreFileScanner> createFileScanners(
      final Collection<StoreFile> filesToCompact) throws IOException {
    return StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, false, true);
  }

  protected long setSmallestReadPoint() {
    long smallestReadPoint = store.getSmallestReadPoint();
    MultiVersionConsistencyControl.setThreadReadPoint(smallestReadPoint);
    return smallestReadPoint;
  }

  protected InternalScanner preCreateCoprocScanner(final CompactionRequest request,
      ScanType scanType, long earliestPutTs,  List<StoreFileScanner> scanners) throws IOException {
    if (store.getCoprocessorHost() == null) return null;
    return store.getCoprocessorHost()
        .preCompactScannerOpen(store, scanners, scanType, earliestPutTs, request);
  }

  protected InternalScanner postCreateCoprocScanner(final CompactionRequest request,
      ScanType scanType, InternalScanner scanner) throws IOException {
    if (store.getCoprocessorHost() == null) return scanner;
    return store.getCoprocessorHost().preCompact(store, scanner, scanType, request);
  }

  @SuppressWarnings("deprecation")
  protected boolean performCompaction(InternalScanner scanner,
      CellSink writer, long smallestReadPoint) throws IOException {
    int bytesWritten = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> kvs = new ArrayList<Cell>();
    // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
    int closeCheckInterval = HStore.getCloseCheckInterval();
    boolean hasMore;
    do {
      hasMore = scanner.next(kvs, compactionKVMax);
      // output to writer:
      for (Cell c : kvs) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(c);
        if (kv.getMvccVersion() <= smallestReadPoint) {
          kv.setMvccVersion(0);
        }
        writer.append(kv);
        ++progress.currentCompactedKVs;

        // check periodically to see if a system stop is requested
        if (closeCheckInterval > 0) {
          bytesWritten += kv.getLength();
          if (bytesWritten > closeCheckInterval) {
            bytesWritten = 0;
            if (!store.areWritesEnabled()) {
              progress.cancel();
              return false;
            }
          }
        }
      }
      kvs.clear();
    } while (hasMore);
    progress.complete();
    return true;
  }

  protected void abortWriter(final StoreFile.Writer writer) throws IOException {
    writer.close();
    store.getFileSystem().delete(writer.getPath(), false);
  }

  /**
   * @param scanners Store file scanners.
   * @param scanType Scan type.
   * @param smallestReadPoint Smallest MVCC read point.
   * @param earliestPutTs Earliest put across all files.
   * @return A compaction scanner.
   */
  protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
      ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    return new StoreScanner(store, store.getScanInfo(), scan, scanners,
        scanType, smallestReadPoint, earliestPutTs);
  }
}
