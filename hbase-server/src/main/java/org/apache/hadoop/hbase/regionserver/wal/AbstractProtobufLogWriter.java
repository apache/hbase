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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.DEFAULT_WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.WAL_TRAILER_WARN_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Base class for Protobuf log writer.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractProtobufLogWriter {

  private static final Log LOG = LogFactory.getLog(AbstractProtobufLogWriter.class);

  protected CompressionContext compressionContext;
  protected Configuration conf;
  protected Codec.Encoder cellEncoder;
  protected WALCellCodec.ByteStringCompressor compressor;
  protected boolean trailerWritten;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  protected AtomicLong length = new AtomicLong();

  private WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
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

  private boolean initializeCompressionContext(Configuration conf, Path path) throws IOException {
    boolean doCompress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (doCompress) {
      try {
        this.compressionContext = new CompressionContext(LRUDictionary.class,
            FSUtils.isRecoveredEdits(path),
            conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true));
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }
    return doCompress;
  }

  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable)
      throws IOException {
    this.conf = conf;
    boolean doCompress = initializeCompressionContext(conf, path);
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    int bufferSize = FSUtils.getDefaultBufferSize(fs);
    short replication = (short) conf.getInt("hbase.regionserver.hlog.replication",
      FSUtils.getDefaultReplication(fs, path));
    long blockSize = conf.getLong("hbase.regionserver.hlog.blocksize",
      FSUtils.getDefaultBlockSize(fs, path));

    initOutput(fs, path, overwritable, bufferSize, replication, blockSize);

    boolean doTagCompress = doCompress
        && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    length.set(writeMagicAndWALHeader(ProtobufLogReader.PB_WAL_MAGIC, buildWALHeader(conf,
      WALHeader.newBuilder().setHasCompression(doCompress).setHasTagCompression(doTagCompress))));

    initAfterHeader(doCompress);

    // instantiate trailer to default value.
    trailer = WALTrailer.newBuilder().build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized protobuf WAL=" + path + ", compression=" + doCompress);
    }
  }

  protected void initAfterHeader(boolean doCompress) throws IOException {
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    }
  }

  void setWALTrailer(WALTrailer walTrailer) {
    this.trailer = walTrailer;
  }

  public long getLength() {
    return length.get();
  }

  private WALTrailer buildWALTrailer(WALTrailer.Builder builder) {
    return builder.build();
  }

  protected void writeWALTrailer() {
    try {
      int trailerSize = 0;
      if (this.trailer == null) {
        // use default trailer.
        LOG.warn("WALTrailer is null. Continuing with default.");
        this.trailer = buildWALTrailer(WALTrailer.newBuilder());
        trailerSize = this.trailer.getSerializedSize();
      } else if ((trailerSize = this.trailer.getSerializedSize()) > this.trailerWarnSize) {
        // continue writing after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " + trailerSize
            + " > " + this.trailerWarnSize);
      }
      length.set(writeWALTrailerAndMagic(trailer, ProtobufLogReader.PB_WAL_COMPLETE_MAGIC));
      this.trailerWritten = true;
    } catch (IOException ioe) {
      LOG.warn("Failed to write trailer, non-fatal, continuing...", ioe);
    }
  }

  protected abstract void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize) throws IOException;

  /**
   * return the file length after written.
   */
  protected abstract long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException;

  protected abstract long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic)
      throws IOException;

  protected abstract OutputStream getOutputStreamForCellEncoder();
}
