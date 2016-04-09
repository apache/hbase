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
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.security.User;

import com.google.common.collect.Lists;

/**
 * Compact passed set of files. Create an instance and then call
 * {@link #compact(CompactionRequest, CompactionThroughputController, User)}
 */
@InterfaceAudience.Private
public class DefaultCompactor extends Compactor<Writer> {
  private static final Log LOG = LogFactory.getLog(DefaultCompactor.class);

  public DefaultCompactor(final Configuration conf, final Store store) {
    super(conf, store);
  }

  private final CellSinkFactory<Writer> writerFactory = new CellSinkFactory<Writer>() {

    @Override
    public Writer createWriter(InternalScanner scanner,
        org.apache.hadoop.hbase.regionserver.compactions.Compactor.FileDetails fd,
        boolean shouldDropBehind) throws IOException {
      return createTmpWriter(fd, shouldDropBehind);
    }
  };

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   */
  public List<Path> compact(final CompactionRequest request,
      CompactionThroughputController throughputController, User user) throws IOException {
    return compact(request, defaultScannerFactory, writerFactory, throughputController, user);
  }

  /**
   * Compact a list of files for testing. Creates a fake {@link CompactionRequest} to pass to
   * {@link #compact(CompactionRequest, CompactionThroughputController, User)};
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
    return this.compact(cr, NoLimitCompactionThroughputController.INSTANCE, null);
  }

  @Override
  protected List<Path> commitWriter(Writer writer, FileDetails fd,
      CompactionRequest request) throws IOException {
    List<Path> newFiles = Lists.newArrayList(writer.getPath());
    writer.appendMetadata(fd.maxSeqId, request.isMajor());
    writer.close();
    return newFiles;
  }

  @Override
  protected void abortWriter(Writer writer) throws IOException {
    Path leftoverFile = writer.getPath();
    try {
      writer.close();
    } catch (IOException e) {
      LOG.warn("Failed to close the writer after an unfinished compaction.", e);
    }
    try {
      store.getFileSystem().delete(leftoverFile, false);
    } catch (IOException e) {
      LOG.warn(
        "Failed to delete the leftover file " + leftoverFile + " after an unfinished compaction.",
        e);
    }
  }
}
