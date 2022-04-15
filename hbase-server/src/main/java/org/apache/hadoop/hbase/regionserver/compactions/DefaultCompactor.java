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
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Compact passed set of files. Create an instance and then call
 * {@link #compact(CompactionRequestImpl, ThroughputController, User)}
 */
@InterfaceAudience.Private
public class DefaultCompactor extends Compactor<StoreFileWriter> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCompactor.class);

  public DefaultCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  private final CellSinkFactory<StoreFileWriter> writerFactory =
    new CellSinkFactory<StoreFileWriter>() {
      @Override
      public StoreFileWriter createWriter(InternalScanner scanner, FileDetails fd,
        boolean shouldDropBehind, boolean major, Consumer<Path> writerCreationTracker)
        throws IOException {
        return DefaultCompactor.this
          .createWriter(fd, shouldDropBehind, major, writerCreationTracker);
      }
    };

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   */
  public List<Path> compact(final CompactionRequestImpl request,
      ThroughputController throughputController, User user) throws IOException {
    return compact(request, defaultScannerFactory, writerFactory, throughputController, user);
  }

  @Override
  protected List<Path> commitWriter(StoreFileWriter writer, FileDetails fd,
      CompactionRequestImpl request) throws IOException {
    List<Path> newFiles = Lists.newArrayList(writer.getPath());
    writer.appendMetadata(fd.maxSeqId, request.isAllFiles(), request.getFiles());
    writer.close();
    return newFiles;
  }

  @Override
  protected final void abortWriter(StoreFileWriter writer) throws IOException {
    Path leftoverFile = writer.getPath();
    try {
      writer.close();
    } catch (IOException e) {
      LOG.warn("Failed to close the writer after an unfinished compaction.", e);
    }
    try {
      store.getFileSystem().delete(leftoverFile, false);
    } catch (IOException e) {
      LOG.warn("Failed to delete the leftover file {} after an unfinished compaction.",
        leftoverFile, e);
    }
  }

}
