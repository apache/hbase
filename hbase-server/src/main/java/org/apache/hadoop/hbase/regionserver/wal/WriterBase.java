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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Context used by our wal dictionary compressor. Null if we're not to do our
 * custom dictionary compression.
 */
@InterfaceAudience.Private
public abstract class WriterBase implements HLog.Writer {

  protected CompressionContext compressionContext;
  protected Configuration conf;

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable) throws IOException {
    this.conf = conf;
  }

  public boolean initializeCompressionContext(Configuration conf, Path path) throws IOException {
    boolean doCompress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (doCompress) {
      try {
        this.compressionContext = new CompressionContext(LRUDictionary.class,
            FSUtils.isRecoveredEdits(path), conf.getBoolean(
                CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true));
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }
    return doCompress;
  }

}
