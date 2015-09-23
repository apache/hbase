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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.DEFAULT_WAL_TRAILER_WARN_SIZE;

/**
 * Writer for protobuf-based WAL.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ProtobufLogWriter extends WriterBase {
  private final Log LOG = LogFactory.getLog(this.getClass());
  protected FSDataOutputStream output;
  protected Codec.Encoder cellEncoder;
  protected WALCellCodec.ByteStringCompressor compressor;
  private boolean trailerWritten;
  private WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  private int trailerWarnSize;

  public ProtobufLogWriter() {
    super();
  }

  protected WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }

  protected WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    if (!builder.hasWriterClsName()) {
      builder.setWriterClsName(ProtobufLogWriter.class.getSimpleName());
    }
    if (!builder.hasCellCodecClsName()) {
      builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf));
    }
    return builder.build();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable)
  throws IOException {
    super.init(fs, path, conf, overwritable);
    assert this.output == null;
    boolean doCompress = initializeCompressionContext(conf, path);
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    int bufferSize = FSUtils.getDefaultBufferSize(fs);
    short replication = (short)conf.getInt(
        "hbase.regionserver.hlog.replication", FSUtils.getDefaultReplication(fs, path));
    long blockSize = conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(fs, path));
    output = fs.createNonRecursive(path, overwritable, bufferSize, replication, blockSize, null);
    output.write(ProtobufLogReader.PB_WAL_MAGIC);
    boolean doTagCompress = doCompress
        && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    buildWALHeader(conf,
        WALHeader.newBuilder().setHasCompression(doCompress).setHasTagCompression(doTagCompress))
        .writeDelimitedTo(output);

    initAfterHeader(doCompress);

    // instantiate trailer to default value.
    trailer = WALTrailer.newBuilder().build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized protobuf WAL=" + path + ", compression=" + doCompress);
    }
  }

  protected void initAfterHeader(boolean doCompress) throws IOException {
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(this.output);
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    }
  }

  @Override
  public void append(Entry entry) throws IOException {
    entry.setCompressionContext(compressionContext);
    entry.getKey().getBuilder(compressor).
      setFollowingKvCount(entry.getEdit().size()).build().writeDelimitedTo(output);
    for (Cell cell : entry.getEdit().getCells()) {
      // cellEncoder must assume little about the stream, since we write PB and cells in turn.
      cellEncoder.write(cell);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.output != null) {
      try {
        if (!trailerWritten) writeWALTrailer();
        this.output.close();
      } catch (NullPointerException npe) {
        // Can get a NPE coming up from down in DFSClient$DFSOutputStream#close
        LOG.warn(npe);
      }
      this.output = null;
    }
  }

  WALTrailer buildWALTrailer(WALTrailer.Builder builder) {
    return builder.build();
  }

  private void writeWALTrailer() {
    try {
      int trailerSize = 0;
      if (this.trailer == null) {
        // use default trailer.
        LOG.warn("WALTrailer is null. Continuing with default.");
        this.trailer = buildWALTrailer(WALTrailer.newBuilder());
        trailerSize = this.trailer.getSerializedSize();
      } else if ((trailerSize = this.trailer.getSerializedSize()) > this.trailerWarnSize) {
        // continue writing after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " +
          trailerSize + " > " + this.trailerWarnSize);
      }
      this.trailer.writeTo(output);
      output.writeInt(trailerSize);
      output.write(ProtobufLogReader.PB_WAL_COMPLETE_MAGIC);
      this.trailerWritten = true;
    } catch (IOException ioe) {
      LOG.warn("Failed to write trailer, non-fatal, continuing...", ioe);
    }
  }

  @Override
  public void sync() throws IOException {
    try {
      // This looks to be a noop but its what we have always done.  Leaving for now.
      this.output.flush();
      // TODO: Add in option to call hsync. See HBASE-5954 Allow proper fsync support for HBase
      //
      this.output.hflush();
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

  void setWALTrailer(WALTrailer walTrailer) {
    this.trailer = walTrailer;
  }
}
