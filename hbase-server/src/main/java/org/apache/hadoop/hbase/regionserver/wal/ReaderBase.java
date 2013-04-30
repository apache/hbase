/**
 *
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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@InterfaceAudience.Private
public abstract class ReaderBase implements HLog.Reader {
  protected Configuration conf;
  protected FileSystem fs;
  protected Path path;
  protected long edit = 0;
  /**
   * Compression context to use reading.  Can be null if no compression.
   */
  protected CompressionContext compressionContext = null;
  protected boolean emptyCompressionContext = true;

  /**
   * Default constructor.
   */
  public ReaderBase() {
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, FSDataInputStream stream)
      throws IOException {
    this.conf = conf;
    this.path = path;
    this.fs = fs;

    initReader(stream);

    boolean compression = hasCompression();
    if (compression) {
      // If compression is enabled, new dictionaries are created here.
      try {
        if (compressionContext == null) {
          compressionContext = new CompressionContext(LRUDictionary.class);
        } else {
          compressionContext.clear();
        }
      } catch (Exception e) {
        throw new IOException("Failed to initialize CompressionContext", e);
      }
    }
    initAfterCompression();
  }

  @Override
  public HLog.Entry next() throws IOException {
    return next(null);
  }

  @Override
  public HLog.Entry next(HLog.Entry reuse) throws IOException {
    HLog.Entry e = reuse;
    if (e == null) {
      e = new HLog.Entry(new HLogKey(), new WALEdit());
    }
    if (compressionContext != null) {
      e.setCompressionContext(compressionContext);
    }

    boolean hasEntry = readNext(e);
    edit++;
    if (compressionContext != null && emptyCompressionContext) {
      emptyCompressionContext = false;
    }
    return hasEntry ? e : null;
  }


  @Override
  public void seek(long pos) throws IOException {
    if (compressionContext != null && emptyCompressionContext) {
      while (next() != null) {
        if (getPosition() == pos) {
          emptyCompressionContext = false;
          break;
        }
      }
    }
    seekOnFs(pos);
  }

  /**
   * Initializes the log reader with a particular stream (may be null).
   * Reader assumes ownership of the stream if not null and may use it. Called once.
   */
  protected abstract void initReader(FSDataInputStream stream) throws IOException;

  /**
   * Initializes the compression after the shared stuff has been initialized. Called once.
   */
  protected abstract void initAfterCompression() throws IOException;
  /**
   * @return Whether compression is enabled for this log.
   */
  protected abstract boolean hasCompression();

  /**
   * Read next entry.
   * @param e The entry to read into.
   * @return Whether there was anything to read.
   */
  protected abstract boolean readNext(HLog.Entry e) throws IOException;

  /**
   * Performs a filesystem-level seek to a certain position in an underlying file.
   */
  protected abstract void seekOnFs(long pos) throws IOException;

}
