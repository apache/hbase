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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.DateTieredMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;

/**
 * This compactor will generate StoreFile for different time ranges.
 */
@InterfaceAudience.Private
public class DateTieredCompactor extends AbstractMultiOutputCompactor<DateTieredMultiFileWriter> {

  private static final Log LOG = LogFactory.getLog(DateTieredCompactor.class);

  public DateTieredCompactor(Configuration conf, Store store) {
    super(conf, store);
  }

  private boolean needEmptyFile(CompactionRequest request) {
    // if we are going to compact the last N files, then we need to emit an empty file to retain the
    // maxSeqId if we haven't written out anything.
    return StoreFile.getMaxSequenceIdInList(request.getFiles()) == store.getMaxSequenceId();
  }

  public List<Path> compact(final CompactionRequest request, List<Long> lowerBoundaries,
      ThroughputController throughputController, User user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing compaction with " + lowerBoundaries.size()
          + "windows, lower boundaries: " + lowerBoundaries);
    }

    DateTieredMultiFileWriter writer =
        new DateTieredMultiFileWriter(lowerBoundaries, needEmptyFile(request));
    return compact(writer, request, new InternalScannerFactory() {

      @Override
      public ScanType getScanType(CompactionRequest request) {
        return request.isRetainDeleteMarkers() ? ScanType.COMPACT_RETAIN_DELETES
            : ScanType.COMPACT_DROP_DELETES;
      }

      @Override
      public InternalScanner createScanner(List<StoreFileScanner> scanners, ScanType scanType,
          FileDetails fd, long smallestReadPoint) throws IOException {
        return DateTieredCompactor.this.createScanner(store, scanners, scanType, smallestReadPoint,
          fd.earliestPutTs);
      }
    }, throughputController, user);
  }

  @Override
  protected List<Path> commitMultiWriter(DateTieredMultiFileWriter writer, FileDetails fd,
      CompactionRequest request) throws IOException {
    return writer.commitWriters(fd.maxSeqId, request.isAllFiles());
  }
}
