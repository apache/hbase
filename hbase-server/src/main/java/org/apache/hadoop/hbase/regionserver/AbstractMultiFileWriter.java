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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor.CellSink;

/**
 * Base class for cell sink that separates the provided cells into multiple files.
 */
@InterfaceAudience.Private
public abstract class AbstractMultiFileWriter implements CellSink {

  private static final Log LOG = LogFactory.getLog(AbstractMultiFileWriter.class);

  /** Factory that is used to produce single StoreFile.Writer-s */
  protected WriterFactory writerFactory;

  /** Source scanner that is tracking KV count; may be null if source is not StoreScanner */
  protected StoreScanner sourceScanner;

  public interface WriterFactory {
    public StoreFileWriter createWriter() throws IOException;
  }

  /**
   * Initializes multi-writer before usage.
   * @param sourceScanner Optional store scanner to obtain the information about read progress.
   * @param factory Factory used to produce individual file writers.
   */
  public void init(StoreScanner sourceScanner, WriterFactory factory) {
    this.writerFactory = factory;
    this.sourceScanner = sourceScanner;
  }

  /**
   * Commit all writers.
   * <p>
   * Notice that here we use the same <code>maxSeqId</code> for all output files since we haven't
   * find an easy to find enough sequence ids for different output files in some corner cases. See
   * comments in HBASE-15400 for more details.
   */
  public List<Path> commitWriters(long maxSeqId, boolean majorCompaction) throws IOException {
    preCommitWriters();
    Collection<StoreFileWriter> writers = this.writers();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Commit " + writers.size() + " writers, maxSeqId=" + maxSeqId
          + ", majorCompaction=" + majorCompaction);
    }
    List<Path> paths = new ArrayList<Path>();
    for (StoreFileWriter writer : writers) {
      if (writer == null) {
        continue;
      }
      writer.appendMetadata(maxSeqId, majorCompaction);
      preCloseWriter(writer);
      paths.add(writer.getPath());
      writer.close();
    }
    return paths;
  }

  /**
   * Close all writers without throwing any exceptions. This is used when compaction failed usually.
   */
  public List<Path> abortWriters() {
    List<Path> paths = new ArrayList<Path>();
    for (StoreFileWriter writer : writers()) {
      try {
        if (writer != null) {
          paths.add(writer.getPath());
          writer.close();
        }
      } catch (Exception ex) {
        LOG.error("Failed to close the writer after an unfinished compaction.", ex);
      }
    }
    return paths;
  }

  protected abstract Collection<StoreFileWriter> writers();

  /**
   * Subclasses override this method to be called at the end of a successful sequence of append; all
   * appends are processed before this method is called.
   */
  protected void preCommitWriters() throws IOException {
  }

  /**
   * Subclasses override this method to be called before we close the give writer. Usually you can
   * append extra metadata to the writer.
   */
  protected void preCloseWriter(StoreFileWriter writer) throws IOException {
  }
}
