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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.AbstractMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.AbstractMultiFileWriter.WriterFactory;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreScanner;

/**
 * Base class for implementing a Compactor which will generate multiple output files after
 * compaction.
 */
@InterfaceAudience.Private
public abstract class AbstractMultiOutputCompactor<T extends AbstractMultiFileWriter>
    extends Compactor<T> {

  private static final Log LOG = LogFactory.getLog(AbstractMultiOutputCompactor.class);

  public AbstractMultiOutputCompactor(Configuration conf, Store store) {
    super(conf, store);
  }

  protected void initMultiWriter(AbstractMultiFileWriter writer, InternalScanner scanner,
      final FileDetails fd, final boolean shouldDropBehind) {
    WriterFactory writerFactory = new WriterFactory() {
      @Override
      public Writer createWriter() throws IOException {
        return createTmpWriter(fd, shouldDropBehind);
      }
    };
    // Prepare multi-writer, and perform the compaction using scanner and writer.
    // It is ok here if storeScanner is null.
    StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
    writer.init(storeScanner, writerFactory);
  }

  @Override
  protected void abortWriter(T writer) throws IOException {
    FileSystem fs = store.getFileSystem();
    for (Path leftoverFile : writer.abortWriters()) {
      try {
        fs.delete(leftoverFile, false);
      } catch (IOException e) {
        LOG.warn(
          "Failed to delete the leftover file " + leftoverFile + " after an unfinished compaction.",
          e);
      }
    }
  }
}
