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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.EARLIEST_PUT_TS;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.TIMERANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.ScanType.COMPACT_DROP_DELETES;
import static org.apache.hadoop.hbase.regionserver.ScanType.COMPACT_RETAIN_DELETES;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.regionserver.CellSink;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControlUtil;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * A compactor is a compaction algorithm associated a given policy. Base class also contains
 * reusable parts for implementing compactors (what is common and what isn't is evolving).
 */
@InterfaceAudience.Private
public abstract class Compactor<T extends CellSink> {
  private static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
  protected static final long COMPACTION_PROGRESS_LOG_INTERVAL = 60 * 1000;
  protected volatile CompactionProgress progress;
  protected final Configuration conf;
  protected final HStore store;

  protected final int compactionKVMax;
  protected final Compression.Algorithm compactionCompression;

  /** specify how many days to keep MVCC values during major compaction **/
  protected int keepSeqIdPeriod;

  // Configs that drive whether we drop page cache behind compactions
  protected static final String  MAJOR_COMPACTION_DROP_CACHE =
      "hbase.regionserver.majorcompaction.pagecache.drop";
  protected static final String MINOR_COMPACTION_DROP_CACHE =
      "hbase.regionserver.minorcompaction.pagecache.drop";

  private final boolean dropCacheMajor;
  private final boolean dropCacheMinor;

  //TODO: depending on Store is not good but, realistically, all compactors currently do.
  Compactor(Configuration conf, HStore store) {
    this.conf = conf;
    this.store = store;
    this.compactionKVMax =
      this.conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    this.compactionCompression = (this.store.getColumnFamilyDescriptor() == null) ?
        Compression.Algorithm.NONE : this.store.getColumnFamilyDescriptor().getCompactionCompressionType();
    this.keepSeqIdPeriod = Math.max(this.conf.getInt(HConstants.KEEP_SEQID_PERIOD,
      HConstants.MIN_KEEP_SEQID_PERIOD), HConstants.MIN_KEEP_SEQID_PERIOD);
    this.dropCacheMajor = conf.getBoolean(MAJOR_COMPACTION_DROP_CACHE, true);
    this.dropCacheMinor = conf.getBoolean(MINOR_COMPACTION_DROP_CACHE, true);
  }



  protected interface CellSinkFactory<S> {
    S createWriter(InternalScanner scanner, FileDetails fd, boolean shouldDropBehind)
        throws IOException;
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
    /** Latest put timestamp */
    public long latestPutTs = HConstants.LATEST_TIMESTAMP;
    /** The last key in the files we're compacting. */
    public long maxSeqId = 0;
    /** Latest memstore read point found in any of the involved files */
    public long maxMVCCReadpoint = 0;
    /** Max tags length**/
    public int maxTagsLength = 0;
    /** Min SeqId to keep during a major compaction **/
    public long minSeqIdToKeep = 0;
    /** Total size of the compacted files **/
    private long totalCompactedFilesSize = 0;
  }

  /**
   * Extracts some details about the files to compact that are commonly needed by compactors.
   * @param filesToCompact Files.
   * @param allFiles Whether all files are included for compaction
   * @return The result.
   */
  private FileDetails getFileDetails(
      Collection<HStoreFile> filesToCompact, boolean allFiles) throws IOException {
    FileDetails fd = new FileDetails();
    long oldestHFileTimestampToKeepMVCC = System.currentTimeMillis() -
      (1000L * 60 * 60 * 24 * this.keepSeqIdPeriod);

    for (HStoreFile file : filesToCompact) {
      if(allFiles && (file.getModificationTimestamp() < oldestHFileTimestampToKeepMVCC)) {
        // when isAllFiles is true, all files are compacted so we can calculate the smallest
        // MVCC value to keep
        if(fd.minSeqIdToKeep < file.getMaxMemStoreTS()) {
          fd.minSeqIdToKeep = file.getMaxMemStoreTS();
        }
      }
      long seqNum = file.getMaxSequenceId();
      fd.maxSeqId = Math.max(fd.maxSeqId, seqNum);
      StoreFileReader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: use getEntries when compacting instead of getFilterEntries, otherwise under-sized
      // blooms can cause progress to be miscalculated or if the user switches bloom
      // type (e.g. from ROW to ROWCOL)
      long keyCount = r.getEntries();
      fd.maxKeyCount += keyCount;
      // calculate the latest MVCC readpoint in any of the involved store files
      Map<byte[], byte[]> fileInfo = r.loadFileInfo();

      // calculate the total size of the compacted files
      fd.totalCompactedFilesSize += r.length();

      byte[] tmp = null;
      // Get and set the real MVCCReadpoint for bulk loaded files, which is the
      // SeqId number.
      if (r.isBulkLoaded()) {
        fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, r.getSequenceID());
      }
      else {
        tmp = fileInfo.get(HFile.Writer.MAX_MEMSTORE_TS_KEY);
        if (tmp != null) {
          fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, Bytes.toLong(tmp));
        }
      }
      tmp = fileInfo.get(HFileInfo.MAX_TAGS_LEN);
      if (tmp != null) {
        fd.maxTagsLength = Math.max(fd.maxTagsLength, Bytes.toInt(tmp));
      }
      // If required, calculate the earliest put timestamp of all involved storefiles.
      // This is used to remove family delete marker during compaction.
      long earliestPutTs = 0;
      if (allFiles) {
        tmp = fileInfo.get(EARLIEST_PUT_TS);
        if (tmp == null) {
          // There's a file with no information, must be an old one
          // assume we have very old puts
          fd.earliestPutTs = earliestPutTs = HConstants.OLDEST_TIMESTAMP;
        } else {
          earliestPutTs = Bytes.toLong(tmp);
          fd.earliestPutTs = Math.min(fd.earliestPutTs, earliestPutTs);
        }
      }
      tmp = fileInfo.get(TIMERANGE_KEY);
      fd.latestPutTs = tmp == null ? HConstants.LATEST_TIMESTAMP: TimeRangeTracker.parseFrom(tmp).getMax();
      LOG.debug("Compacting {}, keycount={}, bloomtype={}, size={}, "
              + "encoding={}, compression={}, seqNum={}{}",
          (file.getPath() == null? null: file.getPath().getName()),
          keyCount,
          r.getBloomFilterType().toString(),
          TraditionalBinaryPrefix.long2String(r.length(), "", 1),
          r.getHFileReader().getDataBlockEncoding(),
          compactionCompression,
          seqNum,
          (allFiles? ", earliestPutTs=" + earliestPutTs: ""));
    }
    return fd;
  }

  /**
   * Creates file scanners for compaction.
   * @param filesToCompact Files.
   * @return Scanners.
   */
  private List<StoreFileScanner> createFileScanners(Collection<HStoreFile> filesToCompact,
      long smallestReadPoint, boolean useDropBehind) throws IOException {
    return StoreFileScanner.getScannersForCompaction(filesToCompact, useDropBehind,
      smallestReadPoint);
  }

  private long getSmallestReadPoint() {
    return store.getSmallestReadPoint();
  }

  protected interface InternalScannerFactory {

    ScanType getScanType(CompactionRequestImpl request);

    InternalScanner createScanner(ScanInfo scanInfo, List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException;
  }

  protected final InternalScannerFactory defaultScannerFactory = new InternalScannerFactory() {

    @Override
    public ScanType getScanType(CompactionRequestImpl request) {
      return request.isAllFiles() ? COMPACT_DROP_DELETES : COMPACT_RETAIN_DELETES;
    }

    @Override
    public InternalScanner createScanner(ScanInfo scanInfo, List<StoreFileScanner> scanners,
        ScanType scanType, FileDetails fd, long smallestReadPoint) throws IOException {
      return Compactor.this.createScanner(store, scanInfo, scanners, scanType, smallestReadPoint,
        fd.earliestPutTs);
    }
  };

  /**
   * Creates a writer for a new file in a temporary directory.
   * @param fd The file details.
   * @return Writer for a new StoreFile in the tmp dir.
   * @throws IOException if creation failed
   */
  protected final StoreFileWriter createTmpWriter(FileDetails fd, boolean shouldDropBehind)
      throws IOException {
    // When all MVCC readpoints are 0, don't write them.
    // See HBASE-8166, HBASE-12600, and HBASE-13389.
    return store
      .createWriterInTmp(fd.maxKeyCount, this.compactionCompression, true, fd.maxMVCCReadpoint > 0,
        fd.maxTagsLength > 0, shouldDropBehind, fd.totalCompactedFilesSize,
        HConstants.EMPTY_STRING);
  }

  protected final StoreFileWriter createTmpWriter(FileDetails fd, boolean shouldDropBehind,
      String fileStoragePolicy) throws IOException {
    return store
      .createWriterInTmp(fd.maxKeyCount, this.compactionCompression, true, fd.maxMVCCReadpoint > 0,
        fd.maxTagsLength > 0, shouldDropBehind, fd.totalCompactedFilesSize, fileStoragePolicy);
  }

  private ScanInfo preCompactScannerOpen(CompactionRequestImpl request, ScanType scanType,
      User user) throws IOException {
    if (store.getCoprocessorHost() == null) {
      return store.getScanInfo();
    }
    return store.getCoprocessorHost().preCompactScannerOpen(store, scanType, request.getTracker(),
      request, user);
  }

  /**
   * Calls coprocessor, if any, to create scanners - after normal scanner creation.
   * @param request Compaction request.
   * @param scanType Scan type.
   * @param scanner The default scanner created for compaction.
   * @return Scanner scanner to use (usually the default); null if compaction should not proceed.
   */
  private InternalScanner postCompactScannerOpen(CompactionRequestImpl request, ScanType scanType,
      InternalScanner scanner, User user) throws IOException {
    if (store.getCoprocessorHost() == null) {
      return scanner;
    }
    return store.getCoprocessorHost().preCompact(store, scanner, scanType, request.getTracker(),
      request, user);
  }

  protected final List<Path> compact(final CompactionRequestImpl request,
      InternalScannerFactory scannerFactory, CellSinkFactory<T> sinkFactory,
      ThroughputController throughputController, User user) throws IOException {
    FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
    this.progress = new CompactionProgress(fd.maxKeyCount);

    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = getSmallestReadPoint();

    T writer = null;
    boolean dropCache;
    if (request.isMajor() || request.isAllFiles()) {
      dropCache = this.dropCacheMajor;
    } else {
      dropCache = this.dropCacheMinor;
    }

    InternalScanner scanner = null;
    boolean finished = false;
    List<StoreFileScanner> scanners =
      createFileScanners(request.getFiles(), smallestReadPoint, dropCache);
    try {
      /* Include deletes, unless we are doing a major compaction */
      ScanType scanType = scannerFactory.getScanType(request);
      ScanInfo scanInfo = preCompactScannerOpen(request, scanType, user);
      scanner = postCompactScannerOpen(request, scanType,
        scannerFactory.createScanner(scanInfo, scanners, scanType, fd, smallestReadPoint), user);
      boolean cleanSeqId = false;
      if (fd.minSeqIdToKeep > 0 && !store.getColumnFamilyDescriptor().isNewVersionBehavior()) {
        // For mvcc-sensitive family, we never set mvcc to 0.
        smallestReadPoint = Math.min(fd.minSeqIdToKeep, smallestReadPoint);
        cleanSeqId = true;
      }
      writer = sinkFactory.createWriter(scanner, fd, dropCache);
      finished = performCompaction(fd, scanner, writer, smallestReadPoint, cleanSeqId,
        throughputController, request.isAllFiles(), request.getFiles().size());
      if (!finished) {
        throw new InterruptedIOException("Aborting compaction of store " + store + " in region "
            + store.getRegionInfo().getRegionNameAsString() + " because it was interrupted.");
      }
    } finally {
      // createScanner may fail when seeking hfiles encounter Exception, e.g. even only one hfile
      // reader encounters java.io.IOException: Invalid HFile block magic:
      // \x00\x00\x00\x00\x00\x00\x00\x00
      // and then scanner will be null, but scanners for all the hfiles should be closed,
      // or else we will find leak of ESTABLISHED sockets.
      if (scanner == null) {
        for (StoreFileScanner sfs : scanners) {
          sfs.close();
        }
      } else {
        Closeables.close(scanner, true);
      }
      if (!finished && writer != null) {
        abortWriter(writer);
      }
    }
    assert finished : "We should have exited the method on all error paths";
    assert writer != null : "Writer should be non-null if no error";
    return commitWriter(writer, fd, request);
  }

  protected abstract List<Path> commitWriter(T writer, FileDetails fd,
      CompactionRequestImpl request) throws IOException;

  protected abstract void abortWriter(T writer) throws IOException;

  /**
   * Performs the compaction.
   * @param fd FileDetails of cell sink writer
   * @param scanner Where to read from.
   * @param writer Where to write to.
   * @param smallestReadPoint Smallest read point.
   * @param cleanSeqId When true, remove seqId(used to be mvcc) value which is &lt;=
   *          smallestReadPoint
   * @param major Is a major compaction.
   * @param numofFilesToCompact the number of files to compact
   * @return Whether compaction ended; false if it was interrupted for some reason.
   */
  protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
      long smallestReadPoint, boolean cleanSeqId, ThroughputController throughputController,
      boolean major, int numofFilesToCompact) throws IOException {
    assert writer instanceof ShipperListener;
    long bytesWrittenProgressForCloseCheck = 0;
    long bytesWrittenProgressForLog = 0;
    long bytesWrittenProgressForShippedCall = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> cells = new ArrayList<>();
    long closeCheckSizeLimit = HStore.getCloseCheckInterval();
    long lastMillis = 0;
    if (LOG.isDebugEnabled()) {
      lastMillis = EnvironmentEdgeManager.currentTime();
    }
    String compactionName = ThroughputControlUtil.getNameForThrottling(store, "compaction");
    long now = 0;
    boolean hasMore;
    ScannerContext scannerContext =
          ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    throughputController.start(compactionName);
    KeyValueScanner kvs = (scanner instanceof KeyValueScanner) ? (KeyValueScanner) scanner : null;
    long shippedCallSizeLimit =
          (long) numofFilesToCompact * this.store.getColumnFamilyDescriptor().getBlocksize();
    try {
      do {
        hasMore = scanner.next(cells, scannerContext);
        if (LOG.isDebugEnabled()) {
          now = EnvironmentEdgeManager.currentTime();
        }
        // output to writer:
        Cell lastCleanCell = null;
        long lastCleanCellSeqId = 0;
        for (Cell c : cells) {
          if (cleanSeqId && c.getSequenceId() <= smallestReadPoint) {
            lastCleanCell = c;
            lastCleanCellSeqId = c.getSequenceId();
            PrivateCellUtil.setSequenceId(c, 0);
          } else {
            lastCleanCell = null;
            lastCleanCellSeqId = 0;
          }
          writer.append(c);
          int len = c.getSerializedSize();
          ++progress.currentCompactedKVs;
          progress.totalCompactedSize += len;
          bytesWrittenProgressForShippedCall += len;
          if (LOG.isDebugEnabled()) {
            bytesWrittenProgressForLog += len;
          }
          throughputController.control(compactionName, len);
          // check periodically to see if a system stop is requested
          if (closeCheckSizeLimit > 0) {
            bytesWrittenProgressForCloseCheck += len;
            if (bytesWrittenProgressForCloseCheck > closeCheckSizeLimit) {
              bytesWrittenProgressForCloseCheck = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
        }
        if (kvs != null && bytesWrittenProgressForShippedCall > shippedCallSizeLimit) {
          if (lastCleanCell != null) {
            // HBASE-16931, set back sequence id to avoid affecting scan order unexpectedly.
            // ShipperListener will do a clone of the last cells it refer, so need to set back
            // sequence id before ShipperListener.beforeShipped
            PrivateCellUtil.setSequenceId(lastCleanCell, lastCleanCellSeqId);
          }
          // Clone the cells that are in the writer so that they are freed of references,
          // if they are holding any.
          ((ShipperListener) writer).beforeShipped();
          // The SHARED block references, being read for compaction, will be kept in prevBlocks
          // list(See HFileScannerImpl#prevBlocks). In case of scan flow, after each set of cells
          // being returned to client, we will call shipped() which can clear this list. Here by
          // we are doing the similar thing. In between the compaction (after every N cells
          // written with collective size of 'shippedCallSizeLimit') we will call shipped which
          // may clear prevBlocks list.
          kvs.shipped();
          bytesWrittenProgressForShippedCall = 0;
        }
        if (lastCleanCell != null) {
          // HBASE-16931, set back sequence id to avoid affecting scan order unexpectedly
          PrivateCellUtil.setSequenceId(lastCleanCell, lastCleanCellSeqId);
        }
        // Log the progress of long running compactions every minute if
        // logging at DEBUG level
        if (LOG.isDebugEnabled()) {
          if ((now - lastMillis) >= COMPACTION_PROGRESS_LOG_INTERVAL) {
            String rate = String.format("%.2f",
              (bytesWrittenProgressForLog / 1024.0) / ((now - lastMillis) / 1000.0));
            LOG.debug("Compaction progress: {} {}, rate={} KB/sec, throughputController is {}",
              compactionName, progress, rate, throughputController);
            lastMillis = now;
            bytesWrittenProgressForLog = 0;
          }
        }
        cells.clear();
      } while (hasMore);
    } catch (InterruptedException e) {
      progress.cancel();
      throw new InterruptedIOException(
            "Interrupted while control throughput of compacting " + compactionName);
    } finally {
      // Clone last cell in the final because writer will append last cell when committing. If
      // don't clone here and once the scanner get closed, then the memory of last cell will be
      // released. (HBASE-22582)
      ((ShipperListener) writer).beforeShipped();
      throughputController.finish(compactionName);
    }
    progress.complete();
    return true;
  }

  /**
   * @param store store
   * @param scanners Store file scanners.
   * @param scanType Scan type.
   * @param smallestReadPoint Smallest MVCC read point.
   * @param earliestPutTs Earliest put across all files.
   * @return A compaction scanner.
   */
  protected InternalScanner createScanner(HStore store, ScanInfo scanInfo,
      List<StoreFileScanner> scanners, ScanType scanType, long smallestReadPoint,
      long earliestPutTs) throws IOException {
    return new StoreScanner(store, scanInfo, scanners, scanType, smallestReadPoint, earliestPutTs);
  }

  /**
   * @param store The store.
   * @param scanners Store file scanners.
   * @param smallestReadPoint Smallest MVCC read point.
   * @param earliestPutTs Earliest put across all files.
   * @param dropDeletesFromRow Drop deletes starting with this row, inclusive. Can be null.
   * @param dropDeletesToRow Drop deletes ending with this row, exclusive. Can be null.
   * @return A compaction scanner.
   */
  protected InternalScanner createScanner(HStore store, ScanInfo scanInfo,
      List<StoreFileScanner> scanners, long smallestReadPoint, long earliestPutTs,
      byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
    return new StoreScanner(store, scanInfo, scanners, smallestReadPoint, earliestPutTs,
        dropDeletesFromRow, dropDeletesToRow);
  }
}
