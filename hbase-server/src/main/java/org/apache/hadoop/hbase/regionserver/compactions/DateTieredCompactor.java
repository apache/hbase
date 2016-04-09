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
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
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
    return store.getMaxSequenceId() == StoreFile.getMaxSequenceIdInList(request.getFiles());
  }

  public List<Path> compact(final CompactionRequest request, final List<Long> lowerBoundaries,
      CompactionThroughputController throughputController, User user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing compaction with " + lowerBoundaries.size()
          + "windows, lower boundaries: " + lowerBoundaries);
    }
    return compact(request, defaultScannerFactory,
      new CellSinkFactory<DateTieredMultiFileWriter>() {

        @Override
        public DateTieredMultiFileWriter createWriter(InternalScanner scanner, FileDetails fd,
            boolean shouldDropBehind) throws IOException {
          DateTieredMultiFileWriter writer = new DateTieredMultiFileWriter(lowerBoundaries,
              needEmptyFile(request));
          initMultiWriter(writer, scanner, fd, shouldDropBehind);
          return writer;
        }
      }, throughputController, user);
  }

  @Override
  protected List<Path> commitWriter(DateTieredMultiFileWriter writer, FileDetails fd,
      CompactionRequest request) throws IOException {
    return writer.commitWriters(fd.maxSeqId, request.isMajor());
  }
}
