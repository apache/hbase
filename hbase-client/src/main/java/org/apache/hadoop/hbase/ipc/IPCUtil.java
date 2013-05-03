/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * Utility to help ipc'ing.
 */
class IPCUtil {
  public static final Log LOG = LogFactory.getLog(IPCUtil.class);
  private final int cellBlockBuildingInitialBufferSize;
  /**
   * How much we think the decompressor will expand the original compressed content.
   */
  private final int cellBlockDecompressionMultiplier;
  private final Configuration conf;

  IPCUtil(final Configuration conf) {
    super();
    this.conf = conf;
    this.cellBlockBuildingInitialBufferSize =
      conf.getInt("hbase.ipc.cellblock.building.initial.buffersize", 16 * 1024);
    this.cellBlockDecompressionMultiplier =
        conf.getInt("hbase.ipc.cellblock.decompression.buffersize.multiplier", 3);
  }

  /**
   * Build a cell block using passed in <code>codec</code>
   * @param codec
   * @param compressor
   * @Param cells
   * @return Null or byte buffer filled with passed-in Cells encoded using passed in
   * <code>codec</code>; the returned buffer has been flipped and is ready for
   * reading.  Use limit to find total size.
   * @throws IOException
   */
  @SuppressWarnings("resource")
  ByteBuffer buildCellBlock(final Codec codec, final CompressionCodec compressor,
      final CellScanner cells)
  throws IOException {
    if (cells == null) return null;
    // TOOD: Reuse buffers?
    // Presizing doesn't work because can't tell what size will be when serialized.
    // BBOS will resize itself.
    ByteBufferOutputStream baos =
      new ByteBufferOutputStream(this.cellBlockBuildingInitialBufferSize);
    OutputStream os = baos;
    Compressor poolCompressor = null;
    try {
      if (compressor != null) {
        if (compressor instanceof Configurable) ((Configurable)compressor).setConf(this.conf);
        poolCompressor = CodecPool.getCompressor(compressor);
        os = compressor.createOutputStream(os, poolCompressor);
      }
      Codec.Encoder encoder = codec.getEncoder(os);
      while (cells.advance()) {
        encoder.write(cells.current());
      }
      encoder.flush();
    } finally {
      os.close();
      if (poolCompressor != null) CodecPool.returnCompressor(poolCompressor);
    }
    if (LOG.isTraceEnabled()) {
      if (this.cellBlockBuildingInitialBufferSize < baos.size()) {
        LOG.trace("Buffer grew from " + this.cellBlockBuildingInitialBufferSize +
        " to " + baos.size());
      }
    }
    return baos.getByteBuffer();
  }

  /**
   * @param codec
   * @param cellBlock
   * @return CellScanner to work against the content of <code>cellBlock</code>
   * @throws IOException
   */
  CellScanner createCellScanner(final Codec codec, final CompressionCodec compressor,
      final byte [] cellBlock)
  throws IOException {
    return createCellScanner(codec, compressor, cellBlock, 0, cellBlock.length);
  }

  /**
   * @param codec
   * @param cellBlock
   * @param offset
   * @param length
   * @return CellScanner to work against the content of <code>cellBlock</code>
   * @throws IOException
   */
  CellScanner createCellScanner(final Codec codec, final CompressionCodec compressor,
      final byte [] cellBlock, final int offset, final int length)
  throws IOException {
    // If compressed, decompress it first before passing it on else we will leak compression
    // resources if the stream is not closed properly after we let it out.
    InputStream is = null;
    if (compressor != null) {
      // GZIPCodec fails w/ NPE if no configuration.
      if (compressor instanceof Configurable) ((Configurable)compressor).setConf(this.conf);
      Decompressor poolDecompressor = CodecPool.getDecompressor(compressor);
      CompressionInputStream cis =
        compressor.createInputStream(new ByteArrayInputStream(cellBlock, offset, length),
        poolDecompressor);
      try {
        // TODO: This is ugly.  The buffer will be resized on us if we guess wrong.
        // TODO: Reuse buffers.
        ByteBufferOutputStream bbos = new ByteBufferOutputStream((length - offset) *
          this.cellBlockDecompressionMultiplier);
        IOUtils.copy(cis, bbos);
        bbos.close();
        ByteBuffer bb = bbos.getByteBuffer();
        is = new ByteArrayInputStream(bb.array(), 0, bb.limit());
      } finally {
        if (is != null) is.close();
        CodecPool.returnDecompressor(poolDecompressor);
      }
    } else {
      is = new ByteArrayInputStream(cellBlock, offset, length);
    }
    return codec.getDecoder(is);
  }

  /**
   * Write out header, param, and cell block if there to a {@link ByteBufferOutputStream} sized
   * to hold these elements.
   * @param header
   * @param param
   * @param cellBlock
   * @return A {@link ByteBufferOutputStream} filled with the content of the passed in
   * <code>header</code>, <code>param</code>, and <code>cellBlock</code>.
   * @throws IOException
   */
  static ByteBufferOutputStream write(final Message header, final Message param,
      final ByteBuffer cellBlock)
  throws IOException {
    int totalSize = getTotalSizeWhenWrittenDelimited(header, param);
    if (cellBlock != null) totalSize += cellBlock.limit();
    ByteBufferOutputStream bbos = new ByteBufferOutputStream(totalSize);
    write(bbos, header, param, cellBlock, totalSize);
    bbos.close();
    return bbos;
  }

  /**
   * Write out header, param, and cell block if there is one.
   * @param dos
   * @param header
   * @param param
   * @param cellBlock
   * @return Total number of bytes written.
   * @throws IOException
   */
  static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuffer cellBlock)
  throws IOException {
    // Must calculate total size and write that first so other side can read it all in in one
    // swoop.  This is dictated by how the server is currently written.  Server needs to change
    // if we are to be able to write without the length prefixing.
    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
    if (cellBlock != null) totalSize += cellBlock.remaining();
    return write(dos, header, param, cellBlock, totalSize);
  }

  private static int write(final OutputStream dos, final Message header, final Message param,
    final ByteBuffer cellBlock, final int totalSize)
  throws IOException {
    // I confirmed toBytes does same as say DataOutputStream#writeInt.
    dos.write(Bytes.toBytes(totalSize));
    header.writeDelimitedTo(dos);
    if (param != null) param.writeDelimitedTo(dos);
    if (cellBlock != null) dos.write(cellBlock.array(), 0, cellBlock.remaining());
    dos.flush();
    return totalSize;
  }

  /**
   * @param in Stream cue'd up just before a delimited message
   * @return Bytes that hold the bytes that make up the message read from <code>in</code>
   * @throws IOException
   */
  static byte [] getDelimitedMessageBytes(final DataInputStream in) throws IOException {
    byte b = in.readByte();
    int size = CodedInputStream.readRawVarint32(b, in);
    // Allocate right-sized buffer rather than let pb allocate its default minimum 4k.
    byte [] bytes = new byte[size];
    IOUtils.readFully(in, bytes);
    return bytes;
  }

  /**
   * Read in chunks of 8K (HBASE-7239)
   * @param in
   * @param dest
   * @param offset
   * @param len
   * @throws IOException
   */
  static void readChunked(final DataInput in, byte[] dest, int offset, int len)
      throws IOException {
    int maxRead = 8192;

    for (; offset < len; offset += maxRead) {
      in.readFully(dest, offset, Math.min(len - offset, maxRead));
    }
  }

  /**
   * @param header
   * @param body
   * @return Size on the wire when the two messages are written with writeDelimitedTo
   */
  static int getTotalSizeWhenWrittenDelimited(Message ... messages) {
    int totalSize = 0;
    for (Message m: messages) {
      if (m == null) continue;
      totalSize += m.getSerializedSize();
      totalSize += CodedOutputStream.computeRawVarint32Size(m.getSerializedSize());
    }
    Preconditions.checkArgument(totalSize < Integer.MAX_VALUE);
    return totalSize;
  }

  /**
   * Return short version of Param Message toString'd, shorter than TextFormat#regionServerStartup
   * @param methodName
   * @param request
   * @return toString of passed <code>param</code>
   */
  static String getRequestShortTextFormat(Message request) {
    if (request instanceof ScanRequest) {
      return TextFormat.shortDebugString(request);
    } else if (request instanceof RegionServerReportRequest) {
      // Print a short message only, just the servername and the requests, not the full load.
      RegionServerReportRequest r = (RegionServerReportRequest)request;
      return "server " + TextFormat.shortDebugString(r.getServer()) +
        " load { numberOfRequests: " + r.getLoad().getNumberOfRequests() + " }";
    } else if (request instanceof RegionServerStartupRequest) {
      return TextFormat.shortDebugString(request);
    }
    return "TODO " + TextFormat.shortDebugString(request);
  }
}