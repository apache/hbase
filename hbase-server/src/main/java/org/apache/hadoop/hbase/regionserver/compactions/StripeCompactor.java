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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.StripeMultiFileWriter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This is the placeholder for stripe compactor. The implementation,
 * as well as the proper javadoc, will be added in HBASE-7967.
 */
@InterfaceAudience.Private
public class StripeCompactor extends Compactor {
  private static final Log LOG = LogFactory.getLog(StripeCompactor.class);
  public StripeCompactor(Configuration conf, Store store) {
    super(conf, store);
  }

  public List<Path> compact(CompactionRequest request, List<byte[]> targetBoundaries,
      byte[] majorRangeFromRow, byte[] majorRangeToRow,
      CompactionThroughputController throughputController) throws IOException {
    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Executing compaction with " + targetBoundaries.size() + " boundaries:");
      for (byte[] tb : targetBoundaries) {
        sb.append(" [").append(Bytes.toString(tb)).append("]");
      }
      LOG.debug(sb.toString());
    }
    StripeMultiFileWriter writer = new StripeMultiFileWriter.BoundaryMultiWriter(
        targetBoundaries, majorRangeFromRow, majorRangeToRow);
    return compactInternal(writer, request, majorRangeFromRow, majorRangeToRow,
      throughputController);
  }

  public List<Path> compact(CompactionRequest request, int targetCount, long targetSize,
      byte[] left, byte[] right, byte[] majorRangeFromRow, byte[] majorRangeToRow,
      CompactionThroughputController throughputController) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing compaction with " + targetSize
          + " target file size, no more than " + targetCount + " files, in ["
          + Bytes.toString(left) + "] [" + Bytes.toString(right) + "] range");
    }
    StripeMultiFileWriter writer = new StripeMultiFileWriter.SizeMultiWriter(
        targetCount, targetSize, left, right);
    return compactInternal(writer, request, majorRangeFromRow, majorRangeToRow,
      throughputController);
  }

  private List<Path> compactInternal(StripeMultiFileWriter mw, final CompactionRequest request,
      byte[] majorRangeFromRow, byte[] majorRangeToRow,
      CompactionThroughputController throughputController) throws IOException {
    final Collection<StoreFile> filesToCompact = request.getFiles();
    final FileDetails fd = getFileDetails(filesToCompact, request.isMajor());
    this.progress = new CompactionProgress(fd.maxKeyCount);

    long smallestReadPoint = getSmallestReadPoint();
    List<StoreFileScanner> scanners = createFileScanners(filesToCompact,
        smallestReadPoint, store.throttleCompaction(request.getSize()));

    boolean finished = false;
    InternalScanner scanner = null;
    try {
      // Get scanner to use.
      ScanType coprocScanType = ScanType.COMPACT_RETAIN_DELETES;
      scanner = preCreateCoprocScanner(request, coprocScanType, fd.earliestPutTs, scanners);
      if (scanner == null) {
        scanner = (majorRangeFromRow == null)
            ? createScanner(store, scanners,
                ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint, fd.earliestPutTs)
            : createScanner(store, scanners,
                smallestReadPoint, fd.earliestPutTs, majorRangeFromRow, majorRangeToRow);
      }
      scanner = postCreateCoprocScanner(request, coprocScanType, scanner);
      if (scanner == null) {
        // NULL scanner returned from coprocessor hooks means skip normal processing.
        return new ArrayList<Path>();
      }

      // Create the writer factory for compactions.
      final boolean needMvcc = fd.maxMVCCReadpoint >= smallestReadPoint;
      final Compression.Algorithm compression = store.getFamily().getCompactionCompression();
      StripeMultiFileWriter.WriterFactory factory = new StripeMultiFileWriter.WriterFactory() {
        @Override
        public Writer createWriter() throws IOException {
          return store.createWriterInTmp(
              fd.maxKeyCount, compression, true, needMvcc, fd.maxTagsLength > 0,
              store.throttleCompaction(request.getSize()));
        }
      };

      // Prepare multi-writer, and perform the compaction using scanner and writer.
      // It is ok here if storeScanner is null.
      StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner)scanner : null;
      mw.init(storeScanner, factory, store.getComparator());
      finished = performCompaction(scanner, mw, smallestReadPoint, throughputController);
      if (!finished) {
        throw new InterruptedIOException( "Aborting compaction of store " + store +
            " in region " + store.getRegionInfo().getRegionNameAsString() +
            " because it was interrupted.");
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (Throwable t) {
          // Don't fail the compaction if this fails.
          LOG.error("Failed to close scanner after compaction.", t);
        }
      }
      if (!finished) {
        for (Path leftoverFile : mw.abortWriters()) {
          try {
            store.getFileSystem().delete(leftoverFile, false);
          } catch (Exception ex) {
            LOG.error("Failed to delete the leftover file after an unfinished compaction.", ex);
          }
        }
      }
    }

    assert finished : "We should have exited the method on all error paths";
    List<Path> newFiles = mw.commitWriters(fd.maxSeqId, request.isMajor());
    assert !newFiles.isEmpty() : "Should have produced an empty file to preserve metadata.";
    return newFiles;
  }
}
