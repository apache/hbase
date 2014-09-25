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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

/**
 * Compact passed set of files. Create an instance and then call {@link #compact(CompactionRequest)}
 */
@InterfaceAudience.Private
public class DefaultCompactor extends Compactor {
  public DefaultCompactor(final Configuration conf, final Store store) {
    super(conf, store);
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   */
  public List<Path> compact(final CompactionRequest request) throws IOException {
    FileDetails fd = getFileDetails(request.getFiles(), request.isMajor());
    this.progress = new CompactionProgress(fd.maxKeyCount);

    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = getSmallestReadPoint();
    List<StoreFileScanner> scanners = createFileScanners(request.getFiles(), smallestReadPoint);

    StoreFile.Writer writer = null;
    List<Path> newFiles = new ArrayList<Path>();
    try {
      InternalScanner scanner = null;
      try {
        /* Include deletes, unless we are doing a major compaction */
        ScanType scanType =
            request.isMajor() ? ScanType.COMPACT_DROP_DELETES : ScanType.COMPACT_RETAIN_DELETES;
        scanner = preCreateCoprocScanner(request, scanType, fd.earliestPutTs, scanners);
        if (scanner == null) {
          scanner = createScanner(store, scanners, scanType, smallestReadPoint, fd.earliestPutTs);
        }
        scanner = postCreateCoprocScanner(request, scanType, scanner);
        if (scanner == null) {
          // NULL scanner returned from coprocessor hooks means skip normal processing.
          return newFiles;
        }
        // Create the writer even if no kv(Empty store file is also ok),
        // because we need record the max seq id for the store file, see HBASE-6059
        writer = store.createWriterInTmp(fd.maxKeyCount, this.compactionCompression, true,
            fd.maxMVCCReadpoint >= smallestReadPoint, fd.maxTagsLength > 0);
        boolean finished = performCompaction(scanner, writer, smallestReadPoint);
        if (!finished) {
          writer.close();
          store.getFileSystem().delete(writer.getPath(), false);
          writer = null;
          throw new InterruptedIOException( "Aborting compaction of store " + store +
              " in region " + store.getRegionInfo().getRegionNameAsString() +
              " because it was interrupted.");
         }
       } finally {
         if (scanner != null) {
           scanner.close();
         }
      }
    } finally {
      if (writer != null) {
        writer.appendMetadata(fd.maxSeqId, request.isMajor());
        writer.close();
        newFiles.add(writer.getPath());
      }
    }
    return newFiles;
  }

  /**
   * Compact a list of files for testing. Creates a fake {@link CompactionRequest} to pass to
   * {@link #compact(CompactionRequest)};
   * @param filesToCompact the files to compact. These are used as the compactionSelection for
   *          the generated {@link CompactionRequest}.
   * @param isMajor true to major compact (prune all deletes, max versions, etc)
   * @return Product of compaction or an empty list if all cells expired or deleted and nothing \
   *         made it through the compaction.
   * @throws IOException
   */
  public List<Path> compactForTesting(final Collection<StoreFile> filesToCompact, boolean isMajor)
      throws IOException {
    CompactionRequest cr = new CompactionRequest(filesToCompact);
    cr.setIsMajor(isMajor);
    return this.compact(cr);
  }
}
