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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.io.Closeables;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor.CellSink;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A compactor is a compaction algorithm associated a given policy. Base class also contains
 * reusable parts for implementing compactors (what is common and what isn't is evolving).
 */
@InterfaceAudience.Private
public abstract class Compactor<T extends CellSink> {
  private static final Log LOG = LogFactory.getLog(Compactor.class);

  protected volatile CompactionProgress progress;

  protected final Configuration conf;
  protected final Store store;

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
   * TODO: Replace this with CellOutputStream when StoreFile.Writer uses cells.
   */
  public interface CellSink {
    void append(KeyValue kv) throws IOException;
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
    /** The last key in the files we're compacting. */
    public long maxSeqId = 0;
    /** Latest memstore read point found in any of the involved files */
    public long maxMVCCReadpoint = 0;
    /** Max tags length**/
    public int maxTagsLength = 0;
  }

  /**
   * Extracts some details about the files to compact that are commonly needed by compactors.
   * @param filesToCompact Files.
   * @param calculatePutTs Whether earliest put TS is needed.
   * @return The result.
   */
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
      // NOTE: use getEntries when compacting instead of getFilterEntries, otherwise under-sized
      // blooms can cause progress to be miscalculated or if the user switches bloom
      // type (e.g. from ROW to ROWCOL)
      long keyCount = r.getEntries();
      fd.maxKeyCount += keyCount;
      // calculate the latest MVCC readpoint in any of the involved store files
      Map<byte[], byte[]> fileInfo = r.loadFileInfo();
      byte[] tmp = fileInfo.get(HFileWriterV2.MAX_MEMSTORE_TS_KEY);
      if (tmp != null) {
        fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, Bytes.toLong(tmp));
      }
      tmp = fileInfo.get(FileInfo.MAX_TAGS_LEN);
      if (tmp != null) {
        fd.maxTagsLength = Math.max(fd.maxTagsLength, Bytes.toInt(tmp));
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
          ", encoding=" + r.getHFileReader().getDataBlockEncoding() +
          ", seqNum=" + seqNum +
          (calculatePutTs ? ", earliestPutTs=" + earliestPutTs: ""));
      }
    }
    return fd;
  }

  /**
   * Creates file scanners for compaction.
   * @param filesToCompact Files.
   * @return Scanners.
   */
  protected List<StoreFileScanner> createFileScanners(
      final Collection<StoreFile> filesToCompact,
      long smallestReadPoint,
      boolean useDropBehind) throws IOException {
    return StoreFileScanner.getScannersForStoreFiles(filesToCompact,
        /* cache blocks = */ false,
        /* use pread = */ false,
        /* is compaction */ true,
        /* use Drop Behind */ useDropBehind,
      smallestReadPoint);
  }

  protected long getSmallestReadPoint() {
    return store.getSmallestReadPoint();
  }

  protected interface InternalScannerFactory {

    ScanType getScanType(CompactionRequest request);

    InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException;
  }

  protected final InternalScannerFactory defaultScannerFactory = new InternalScannerFactory() {

    @Override
    public ScanType getScanType(CompactionRequest request) {
      return request.isMajor() ? ScanType.COMPACT_DROP_DELETES
          : ScanType.COMPACT_RETAIN_DELETES;
    }

    @Override
    public InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException {
      return Compactor.this.createScanner(store, scanners, scanType, smallestReadPoint,
        fd.earliestPutTs);
    }
  };

  /**
   * Creates a writer for a new file in a temporary directory.
   * @param fd The file details.
   * @return Writer for a new StoreFile in the tmp dir.
   * @throws IOException if creation failed
   */
  protected Writer createTmpWriter(FileDetails fd, boolean shouldDropBehind) throws IOException {
    // When all MVCC readpoints are 0, don't write them.
    // See HBASE-8166, HBASE-12600, and HBASE-13389.
    return store.createWriterInTmp(fd.maxKeyCount, this.compactionCompression,
    /* isCompaction = */true,
    /* includeMVCCReadpoint = */fd.maxMVCCReadpoint > 0,
    /* includesTags = */fd.maxTagsLength > 0, shouldDropBehind);
  }

  protected List<Path> compact(final CompactionRequest request,
      InternalScannerFactory scannerFactory, CellSinkFactory<T> sinkFactory,
      CompactionThroughputController throughputController, User user) throws IOException {
    FileDetails fd = getFileDetails(request.getFiles(), request.isMajor());
    this.progress = new CompactionProgress(fd.maxKeyCount);

    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = getSmallestReadPoint();

    List<StoreFileScanner> scanners;
    Collection<StoreFile> readersToClose;
    T writer = null;
    if (this.conf.getBoolean("hbase.regionserver.compaction.private.readers", true)) {
      // clone all StoreFiles, so we'll do the compaction on a independent copy of StoreFiles,
      // HFiles, and their readers
      readersToClose = new ArrayList<StoreFile>(request.getFiles().size());
      for (StoreFile f : request.getFiles()) {
        readersToClose.add(f.cloneForReader());
      }
      scanners = createFileScanners(readersToClose, smallestReadPoint,
        store.throttleCompaction(request.getSize()));
    } else {
      readersToClose = Collections.emptyList();
      scanners = createFileScanners(request.getFiles(), smallestReadPoint,
        store.throttleCompaction(request.getSize()));
    }
    InternalScanner scanner = null;
    boolean finished = false;
    try {
      /* Include deletes, unless we are doing a major compaction */
      ScanType scanType = scannerFactory.getScanType(request);
      scanner = preCreateCoprocScanner(request, scanType, fd.earliestPutTs, scanners);
      if (scanner == null) {
        scanner = scannerFactory.createScanner(scanners, scanType, fd, smallestReadPoint);
      }
      scanner = postCreateCoprocScanner(request, scanType, scanner, user);
      if (scanner == null) {
        // NULL scanner returned from coprocessor hooks means skip normal processing.
        return new ArrayList<Path>();
      }
      writer = sinkFactory.createWriter(scanner, fd, store.throttleCompaction(request.getSize()));
      finished =
          performCompaction(scanner, writer, smallestReadPoint, throughputController);
      if (!finished) {
        throw new InterruptedIOException("Aborting compaction of store " + store + " in region "
            + store.getRegionInfo().getRegionNameAsString() + " because it was interrupted.");
      }
    } finally {
      Closeables.close(scanner, true);
      for (StoreFile f : readersToClose) {
        try {
          f.closeReader(true);
        } catch (IOException e) {
          LOG.warn("Exception closing " + f, e);
        }
      }
      if (!finished && writer != null) {
        abortWriter(writer);
      }
    }
    assert finished : "We should have exited the method on all error paths";
    assert writer != null : "Writer should be non-null if no error";
    return commitWriter(writer, fd, request);
  }

  protected abstract List<Path> commitWriter(T writer, FileDetails fd, CompactionRequest request)
      throws IOException;

  protected abstract void abortWriter(T writer) throws IOException;

  /**
   * Calls coprocessor, if any, to create compaction scanner - before normal scanner creation.
   * @param request Compaction request.
   * @param scanType Scan type.
   * @param earliestPutTs Earliest put ts.
   * @param scanners File scanners for compaction files.
   * @return Scanner override by coprocessor; null if not overriding.
   */
  protected InternalScanner preCreateCoprocScanner(final CompactionRequest request,
      ScanType scanType, long earliestPutTs,  List<StoreFileScanner> scanners) throws IOException {
    return preCreateCoprocScanner(request, scanType, earliestPutTs, scanners, null);
  }

  protected InternalScanner preCreateCoprocScanner(final CompactionRequest request,
      final ScanType scanType, final long earliestPutTs, final List<StoreFileScanner> scanners,
      User user) throws IOException {
    if (store.getCoprocessorHost() == null) {
      return null;
    }
    if (user == null) {
      return store.getCoprocessorHost().preCompactScannerOpen(store, scanners, scanType,
        earliestPutTs, request);
    } else {
      try {
        return user.getUGI().doAs(new PrivilegedExceptionAction<InternalScanner>() {
          @Override
          public InternalScanner run() throws Exception {
            return store.getCoprocessorHost().preCompactScannerOpen(store, scanners,
              scanType, earliestPutTs, request);
          }
        });
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    }
  }

  /**
   * Calls coprocessor, if any, to create scanners - after normal scanner creation.
   * @param request Compaction request.
   * @param scanType Scan type.
   * @param scanner The default scanner created for compaction.
   * @return Scanner scanner to use (usually the default); null if compaction should not proceed.
   */
  protected InternalScanner postCreateCoprocScanner(final CompactionRequest request,
      final ScanType scanType, final InternalScanner scanner, User user) throws IOException {
    if (store.getCoprocessorHost() == null) {
      return scanner;
    }
    if (user == null) {
      return store.getCoprocessorHost().preCompact(store, scanner, scanType, request);
    } else {
      try {
        return user.getUGI().doAs(new PrivilegedExceptionAction<InternalScanner>() {
          @Override
          public InternalScanner run() throws Exception {
            return store.getCoprocessorHost().preCompact(store, scanner, scanType, request);
          }
        });
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    }
  }

  /**
   * Used to prevent compaction name conflict when multiple compactions running parallel on the
   * same store.
   */
  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(0);

  private String generateCompactionName() {
    int counter;
    for (;;) {
      counter = NAME_COUNTER.get();
      int next = counter == Integer.MAX_VALUE ? 0 : counter + 1;
      if (NAME_COUNTER.compareAndSet(counter, next)) {
        break;
      }
    }
    return store.getRegionInfo().getRegionNameAsString() + "#"
        + store.getFamily().getNameAsString() + "#" + counter;
  }
  /**
   * Performs the compaction.
   * @param scanner Where to read from.
   * @param writer Where to write to.
   * @param smallestReadPoint Smallest read point.
   * @return Whether compaction ended; false if it was interrupted for some reason.
   */
  protected boolean performCompaction(InternalScanner scanner, CellSink writer,
      long smallestReadPoint, CompactionThroughputController throughputController)
      throws IOException {
    long bytesWritten = 0;
    long bytesWrittenProgress = 0;
    // Since scanner.next() can return 'false' but still be delivering data,
    // we have to use a do/while loop.
    List<Cell> kvs = new ArrayList<Cell>();
    long closeCheckInterval = HStore.getCloseCheckInterval();
    long lastMillis = 0;
    if (LOG.isDebugEnabled()) {
      lastMillis = EnvironmentEdgeManager.currentTimeMillis();
    }
    String compactionName = generateCompactionName();
    long now = 0;
    boolean hasMore;
    throughputController.start(compactionName);
    try {
      do {
        hasMore = scanner.next(kvs, compactionKVMax);
        if (LOG.isDebugEnabled()) {
          now = EnvironmentEdgeManager.currentTimeMillis();
        }
        // output to writer:
        for (Cell c : kvs) {
          KeyValue kv = KeyValueUtil.ensureKeyValue(c);
          if (kv.getMvccVersion() <= smallestReadPoint) {
            kv.setMvccVersion(0);
          }
          writer.append(kv);
          int len = kv.getLength();
          ++progress.currentCompactedKVs;
          progress.totalCompactedSize += len;
          if (LOG.isDebugEnabled()) {
            bytesWrittenProgress += len;
          }
          throughputController.control(compactionName, len);

          // check periodically to see if a system stop is requested
          if (closeCheckInterval > 0) {
            bytesWritten += len;
            if (bytesWritten > closeCheckInterval) {
              bytesWritten = 0;
              if (!store.areWritesEnabled()) {
                progress.cancel();
                return false;
              }
            }
          }
        }
        // Log the progress of long running compactions every minute if
        // logging at DEBUG level
        if (LOG.isDebugEnabled()) {
          if ((now - lastMillis) >= 60 * 1000) {
            LOG.debug("Compaction progress: "
                + compactionName
                + " "
                + progress
                + String.format(", rate=%.2f kB/sec", (bytesWrittenProgress / 1024.0)
                    / ((now - lastMillis) / 1000.0)) + ", throughputController is "
                + throughputController);
            lastMillis = now;
            bytesWrittenProgress = 0;
          }
        }
        kvs.clear();
      } while (hasMore);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while control throughput of compacting "
          + compactionName);
    } finally {
      throughputController.finish(compactionName);
      progress.complete();
    }
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
  protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
      ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    return new StoreScanner(store, store.getScanInfo(), scan, scanners,
        scanType, smallestReadPoint, earliestPutTs);
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
  protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
     long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
     byte[] dropDeletesToRow) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    return new StoreScanner(store, store.getScanInfo(), scan, scanners, smallestReadPoint,
        earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
  }
}
