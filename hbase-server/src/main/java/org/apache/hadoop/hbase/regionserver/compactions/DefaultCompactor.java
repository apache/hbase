/*
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

import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_ENABLE_DUAL_FILE_WRITER_KEY;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.DualFileWriter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compact passed set of files. Create an instance and then call
 * {@link #compact(CompactionRequestImpl, ThroughputController, User)}
 */
@InterfaceAudience.Private
public class DefaultCompactor extends AbstractMultiOutputCompactor<DualFileWriter> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCompactor.class);

  public DefaultCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  private final CellSinkFactory<DualFileWriter> writerFactory =
    new CellSinkFactory<DualFileWriter>() {
      @Override
      public DualFileWriter createWriter(InternalScanner scanner, FileDetails fd,
        boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker)
        throws IOException {
        boolean enableDualFileWriter =
          conf.getBoolean(DEFAULT_COMPACTION_ENABLE_DUAL_FILE_WRITER_KEY, true);
        DualFileWriter writer = new DualFileWriter(store.getComparator(),
          store.getColumnFamilyDescriptor().getMaxVersions(),
          enableDualFileWriter);
        initMultiWriter(writer, scanner, fd, shouldDropBehind, major, writerCreationTracker);
        return writer;
      }
    };

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   */
  public List<Path> compact(final CompactionRequestImpl request,
    ThroughputController throughputController, User user) throws IOException {
    return compact(request, defaultScannerFactory, writerFactory, throughputController, user);
  }

  protected List<Path> commitWriter(DualFileWriter writer, FileDetails fd,
    CompactionRequestImpl request) throws IOException {
    List<Path> pathList =
      writer.commitWriters(fd.maxSeqId, request.isAllFiles(), request.getFiles());
    return pathList;
  }
}
