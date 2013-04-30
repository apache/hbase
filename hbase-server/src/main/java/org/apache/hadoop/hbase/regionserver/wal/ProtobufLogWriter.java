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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;

/**
 * Writer for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class ProtobufLogWriter implements HLog.Writer {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private FSDataOutputStream output;
  private Codec.Encoder cellEncoder;
  private WALCellCodec.ByteStringCompressor compressor;


  /** Context used by our wal dictionary compressor.
   * Null if we're not to do our custom dictionary compression. */
  private CompressionContext compressionContext;

  public ProtobufLogWriter() {
    super();
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf) throws IOException {
    assert this.output == null;
    boolean doCompress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (doCompress) {
      try {
        this.compressionContext = new CompressionContext(LRUDictionary.class);
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }
    int bufferSize = fs.getConf().getInt("io.file.buffer.size", 4096);
    short replication = (short)conf.getInt(
        "hbase.regionserver.hlog.replication", fs.getDefaultReplication());
    long blockSize = conf.getLong("hbase.regionserver.hlog.blocksize", fs.getDefaultBlockSize());
    output = fs.create(path, true, bufferSize, replication, blockSize);
    output.write(ProtobufLogReader.PB_WAL_MAGIC);
    WALHeader.newBuilder().setHasCompression(doCompress).build().writeDelimitedTo(output);

    WALCellCodec codec = new WALCellCodec(this.compressionContext);
    this.cellEncoder = codec.getEncoder(this.output);
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    }
    LOG.debug("Writing protobuf WAL; path=" + path + ", compression=" + doCompress);
  }

  @Override
  public void append(HLog.Entry entry) throws IOException {
    entry.setCompressionContext(compressionContext);
    entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size())
      .build().writeDelimitedTo(output);
    for (KeyValue kv : entry.getEdit().getKeyValues()) {
      // cellEncoder must assume little about the stream, since we write PB and cells in turn.
      cellEncoder.write(kv);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.output != null) {
      try {
        this.output.close();
      } catch (NullPointerException npe) {
        // Can get a NPE coming up from down in DFSClient$DFSOutputStream#close
        LOG.warn(npe);
      }
      this.output = null;
    }
  }

  @Override
  public void sync() throws IOException {
    try {
      this.output.flush();
      this.output.sync();
    } catch (NullPointerException npe) {
      // Concurrent close...
      throw new IOException(npe);
    }
  }

  @Override
  public long getLength() throws IOException {
    try {
      return this.output.getPos();
    } catch (NullPointerException npe) {
      // Concurrent close...
      throw new IOException(npe);
    }
  }

  public FSDataOutputStream getStream() {
    return this.output;
  }
}
