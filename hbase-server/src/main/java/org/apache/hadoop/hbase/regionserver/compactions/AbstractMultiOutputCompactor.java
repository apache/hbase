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
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.AbstractMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.AbstractMultiFileWriter.WriterFactory;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.security.User;

import com.google.common.io.Closeables;

/**
 * Base class for implementing a Compactor which will generate multiple output files after
 * compaction.
 */
@InterfaceAudience.Private
public abstract class AbstractMultiOutputCompactor<T extends AbstractMultiFileWriter>
    extends Compactor {

  private static final Log LOG = LogFactory.getLog(AbstractMultiOutputCompactor.class);

  public AbstractMultiOutputCompactor(Configuration conf, Store store) {
    super(conf, store);
  }

  protected interface InternalScannerFactory {

    ScanType getScanType(CompactionRequest request);

    InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
        FileDetails fd, long smallestReadPoint) throws IOException;
  }

  protected List<Path> compact(T writer, final CompactionRequest request,
      InternalScannerFactory scannerFactory, CompactionThroughputController throughputController,
      User user) throws IOException {
    final FileDetails fd = getFileDetails(request.getFiles(), request.isMajor());
    this.progress = new CompactionProgress(fd.maxKeyCount);

    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = getSmallestReadPoint();

    List<StoreFileScanner> scanners;
    Collection<StoreFile> readersToClose;
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
      // Create the writer factory for compactions.
      final boolean needMvcc = fd.maxMVCCReadpoint >= smallestReadPoint;
      WriterFactory writerFactory = new WriterFactory() {
        @Override
        public Writer createWriter() throws IOException {
          return store.createWriterInTmp(fd.maxKeyCount, compactionCompression, true, needMvcc,
            fd.maxTagsLength > 0, store.throttleCompaction(request.getSize()));
        }
      };
      // Prepare multi-writer, and perform the compaction using scanner and writer.
      // It is ok here if storeScanner is null.
      StoreScanner storeScanner
        = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
      writer.init(storeScanner, writerFactory);
      finished = performCompaction(scanner, writer, smallestReadPoint, throughputController);
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
      if (!finished) {
        FileSystem fs = store.getFileSystem();
        for (Path leftoverFile : writer.abortWriters()) {
          try {
            fs.delete(leftoverFile, false);
          } catch (IOException e) {
            LOG.error("Failed to delete the leftover file " + leftoverFile
                + " after an unfinished compaction.",
              e);
          }
        }
      }
    }
    assert finished : "We should have exited the method on all error paths";
    return commitMultiWriter(writer, fd, request);
  }

  protected abstract List<Path> commitMultiWriter(T writer, FileDetails fd,
      CompactionRequest request) throws IOException;
}
