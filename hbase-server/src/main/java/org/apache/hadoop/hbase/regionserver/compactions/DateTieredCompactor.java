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
import java.util.OptionalLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.DateTieredMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This compactor will generate StoreFile for different time ranges.
 */
@InterfaceAudience.Private
public class DateTieredCompactor extends AbstractMultiOutputCompactor<DateTieredMultiFileWriter> {

  private static final Logger LOG = LoggerFactory.getLogger(DateTieredCompactor.class);

  public DateTieredCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  private boolean needEmptyFile(CompactionRequestImpl request) {
    // if we are going to compact the last N files, then we need to emit an empty file to retain the
    // maxSeqId if we haven't written out anything.
    OptionalLong maxSeqId = StoreUtils.getMaxSequenceIdInList(request.getFiles());
    OptionalLong storeMaxSeqId = store.getMaxSequenceId();
    return maxSeqId.isPresent() && storeMaxSeqId.isPresent() &&
        maxSeqId.getAsLong() == storeMaxSeqId.getAsLong();
  }

  public List<Path> compact(final CompactionRequestImpl request, final List<Long> lowerBoundaries,
      ThroughputController throughputController, User user) throws IOException {
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
      CompactionRequestImpl request) throws IOException {
    return writer.commitWriters(fd.maxSeqId, request.isAllFiles());
  }
}
