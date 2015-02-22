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

package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;

import java.io.IOException;

/**
 * Wrapper class around WAL to help manage the implementation details
 * such as compression.
 */
@InterfaceAudience.Private
public class ReplicationWALReaderManager {

  private static final Log LOG = LogFactory.getLog(ReplicationWALReaderManager.class);
  private final FileSystem fs;
  private final Configuration conf;
  private long position = 0;
  private Reader reader;
  private Path lastPath;

  /**
   * Creates the helper but doesn't open any file
   * Use setInitialPosition after using the constructor if some content needs to be skipped
   * @param fs
   * @param conf
   */
  public ReplicationWALReaderManager(FileSystem fs, Configuration conf) {
    this.fs = fs;
    this.conf = conf;
  }

  /**
   * Opens the file at the current position
   * @param path
   * @return an WAL reader.
   * @throws IOException
   */
  public Reader openReader(Path path) throws IOException {
    // Detect if this is a new file, if so get a new reader else
    // reset the current reader so that we see the new data
    if (this.reader == null || !this.lastPath.equals(path)) {
      this.closeReader();
      this.reader = WALFactory.createReader(this.fs, path, this.conf);
      this.lastPath = path;
    } else {
      try {
        this.reader.reset();
      } catch (NullPointerException npe) {
        throw new IOException("NPE resetting reader, likely HDFS-4380", npe);
      }
    }
    return this.reader;
  }

  /**
   * Get the next entry, returned and also added in the array
   * @return a new entry or null
   * @throws IOException
   */
  public Entry readNextAndSetPosition() throws IOException {
    Entry entry = this.reader.next();
    // Store the position so that in the future the reader can start
    // reading from here. If the above call to next() throws an
    // exception, the position won't be changed and retry will happen
    // from the last known good position
    this.position = this.reader.getPosition();
    // We need to set the CC to null else it will be compressed when sent to the sink
    if (entry != null) {
      entry.setCompressionContext(null);
    }
    return entry;
  }

  /**
   * Advance the reader to the current position
   * @throws IOException
   */
  public void seek() throws IOException {
    if (this.position != 0) {
      this.reader.seek(this.position);
    }
  }

  /**
   * Get the position that we stopped reading at
   * @return current position, cannot be negative
   */
  public long getPosition() {
    return this.position;
  }

  public void setPosition(long pos) {
    this.position = pos;
  }

  /**
   * Close the current reader
   * @throws IOException
   */
  public void closeReader() throws IOException {
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }

  /**
   * Tell the helper to reset internal state
   */
  void finishCurrentFile() {
    this.position = 0;
    try {
      this.closeReader();
    } catch (IOException e) {
      LOG.warn("Unable to close reader", e);
    }
  }

}
