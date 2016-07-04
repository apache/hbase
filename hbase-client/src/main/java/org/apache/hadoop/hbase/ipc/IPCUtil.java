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

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.io.ByteBufferListOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Utility to help ipc'ing.
 */
@InterfaceAudience.Private
public class IPCUtil {
  // LOG is being used in TestIPCUtil
  public static final Log LOG = LogFactory.getLog(IPCUtil.class);
  /**
   * How much we think the decompressor will expand the original compressed content.
   */
  private final int cellBlockDecompressionMultiplier;
  private final int cellBlockBuildingInitialBufferSize;
  private final Configuration conf;

  public IPCUtil(final Configuration conf) {
    super();
    this.conf = conf;
    this.cellBlockDecompressionMultiplier =
        conf.getInt("hbase.ipc.cellblock.decompression.buffersize.multiplier", 3);

    // Guess that 16k is a good size for rpc buffer.  Could go bigger.  See the TODO below in
    // #buildCellBlock.
    this.cellBlockBuildingInitialBufferSize =
      ClassSize.align(conf.getInt("hbase.ipc.cellblock.building.initial.buffersize", 16 * 1024));
  }

  /**
   * Thrown if a cellscanner but no codec to encode it with.
   */
  public static class CellScannerButNoCodecException extends HBaseIOException {};

  /**
   * Puts CellScanner Cells into a cell block using passed in <code>codec</code> and/or
   * <code>compressor</code>.
   * @param codec to use for encoding
   * @param compressor to use for encoding
   * @param cellScanner to encode
   * @return Null or byte buffer filled with a cellblock filled with passed-in Cells encoded using
   *   passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has been
   *   flipped and is ready for reading.  Use limit to find total size.
   * @throws IOException if encoding the cells fail
   */
  @SuppressWarnings("resource")
  public ByteBuffer buildCellBlock(final Codec codec, final CompressionCodec compressor,
      final CellScanner cellScanner) throws IOException {
    if (cellScanner == null) {
      return null;
    }
    if (codec == null) {
      throw new CellScannerButNoCodecException();
    }
    int bufferSize = this.cellBlockBuildingInitialBufferSize;
    ByteBufferOutputStream baos = new ByteBufferOutputStream(bufferSize);
    encodeCellsTo(baos, cellScanner, codec, compressor);
    if (LOG.isTraceEnabled()) {
      if (bufferSize < baos.size()) {
        LOG.trace("Buffer grew from initial bufferSize=" + bufferSize + " to " + baos.size()
            + "; up hbase.ipc.cellblock.building.initial.buffersize?");
      }
    }
    ByteBuffer bb = baos.getByteBuffer();
    // If no cells, don't mess around. Just return null (could be a bunch of existence checking
    // gets or something -- stuff that does not return a cell).
    if (!bb.hasRemaining()) return null;
    return bb;
  }

  private void encodeCellsTo(ByteBufferOutputStream bbos, CellScanner cellScanner, Codec codec,
      CompressionCodec compressor) throws IOException {
    OutputStream os = bbos;
    Compressor poolCompressor = null;
    try  {
      if (compressor != null) {
        if (compressor instanceof Configurable) {
          ((Configurable) compressor).setConf(this.conf);
        }
        poolCompressor = CodecPool.getCompressor(compressor);
        os = compressor.createOutputStream(os, poolCompressor);
      }
      Codec.Encoder encoder = codec.getEncoder(os);
      while (cellScanner.advance()) {
        encoder.write(cellScanner.current());
      }
      encoder.flush();
    } catch (BufferOverflowException e) {
      throw new DoNotRetryIOException(e);
    } finally {
      os.close();
      if (poolCompressor != null) {
        CodecPool.returnCompressor(poolCompressor);
      }
    }
  }

  /**
   * Puts CellScanner Cells into a cell block using passed in <code>codec</code> and/or
   * <code>compressor</code>.
   * @param codec to use for encoding
   * @param compressor to use for encoding
   * @param cellScanner to encode
   * @param pool Pool of ByteBuffers to make use of.
   * @return Null or byte buffer filled with a cellblock filled with passed-in Cells encoded using
   *   passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has been
   *   flipped and is ready for reading.  Use limit to find total size. If <code>pool</code> was not
   *   null, then this returned ByteBuffer came from there and should be returned to the pool when
   *   done.
   * @throws IOException if encoding the cells fail
   */
  @SuppressWarnings("resource")
  public ByteBufferListOutputStream buildCellBlockStream(Codec codec, CompressionCodec compressor,
      CellScanner cellScanner, ByteBufferPool pool) throws IOException {
    if (cellScanner == null) {
      return null;
    }
    if (codec == null) {
      throw new CellScannerButNoCodecException();
    }
    assert pool != null;
    ByteBufferListOutputStream bbos = new ByteBufferListOutputStream(pool);
    encodeCellsTo(bbos, cellScanner, codec, compressor);
    if (bbos.size() == 0) {
      bbos.releaseResources();
      return null;
    }
    return bbos;
  }

  /**
   * @param codec to use for cellblock
   * @param cellBlock to encode
   * @return CellScanner to work against the content of <code>cellBlock</code>
   * @throws IOException if encoding fails
   */
  public CellScanner createCellScanner(final Codec codec, final CompressionCodec compressor,
      final byte[] cellBlock) throws IOException {
    // Use this method from Client side to create the CellScanner
    ByteBuffer cellBlockBuf = ByteBuffer.wrap(cellBlock);
    if (compressor != null) {
      cellBlockBuf = decompress(compressor, cellBlockBuf);
    }
    // Not making the Decoder over the ByteBuffer purposefully. The Decoder over the BB will
    // make Cells directly over the passed BB. This method is called at client side and we don't
    // want the Cells to share the same byte[] where the RPC response is being read. Caching of any
    // of the Cells at user's app level will make it not possible to GC the response byte[]
    return codec.getDecoder(new ByteBufferInputStream(cellBlockBuf));
  }

  /**
   * @param codec to use for cellblock
   * @param cellBlock ByteBuffer containing the cells written by the Codec. The buffer should be
   *   position()'ed at the start of the cell block and limit()'ed at the end.
   * @return CellScanner to work against the content of <code>cellBlock</code>.
   *   All cells created out of the CellScanner will share the same ByteBuffer being passed.
   * @throws IOException if cell encoding fails
   */
  public CellScanner createCellScannerReusingBuffers(final Codec codec,
      final CompressionCodec compressor, ByteBuffer cellBlock) throws IOException {
    // Use this method from HRS to create the CellScanner
    // If compressed, decompress it first before passing it on else we will leak compression
    // resources if the stream is not closed properly after we let it out.
    if (compressor != null) {
      cellBlock = decompress(compressor, cellBlock);
    }
    return codec.getDecoder(cellBlock);
  }

  private ByteBuffer decompress(CompressionCodec compressor, ByteBuffer cellBlock)
      throws IOException {
    // GZIPCodec fails w/ NPE if no configuration.
    if (compressor instanceof Configurable) {
      ((Configurable) compressor).setConf(this.conf);
    }
    Decompressor poolDecompressor = CodecPool.getDecompressor(compressor);
    CompressionInputStream cis = compressor.createInputStream(new ByteBufferInputStream(cellBlock),
        poolDecompressor);
    ByteBufferOutputStream bbos;
    try {
      // TODO: This is ugly. The buffer will be resized on us if we guess wrong.
      // TODO: Reuse buffers.
      bbos = new ByteBufferOutputStream(
          cellBlock.remaining() * this.cellBlockDecompressionMultiplier);
      IOUtils.copy(cis, bbos);
      bbos.close();
      cellBlock = bbos.getByteBuffer();
    } finally {
      CodecPool.returnDecompressor(poolDecompressor);
    }
    return cellBlock;
  }

  /**
   * Write out header, param, and cell block if there is one.
   * @param dos Stream to write into
   * @param header to write
   * @param param to write
   * @param cellBlock to write
   * @return Total number of bytes written.
   * @throws IOException if write action fails
   */
  public static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuffer cellBlock)
  throws IOException {
    // Must calculate total size and write that first so other side can read it all in in one
    // swoop.  This is dictated by how the server is currently written.  Server needs to change
    // if we are to be able to write without the length prefixing.
    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
    if (cellBlock != null) {
      totalSize += cellBlock.remaining();
    }
    return write(dos, header, param, cellBlock, totalSize);
  }

  private static int write(final OutputStream dos, final Message header, final Message param,
    final ByteBuffer cellBlock, final int totalSize)
  throws IOException {
    // I confirmed toBytes does same as DataOutputStream#writeInt.
    dos.write(Bytes.toBytes(totalSize));
    // This allocates a buffer that is the size of the message internally.
    header.writeDelimitedTo(dos);
    if (param != null) {
      param.writeDelimitedTo(dos);
    }
    if (cellBlock != null) {
      dos.write(cellBlock.array(), 0, cellBlock.remaining());
    }
    dos.flush();
    return totalSize;
  }

  /**
   * @return Size on the wire when the two messages are written with writeDelimitedTo
   */
  public static int getTotalSizeWhenWrittenDelimited(Message ... messages) {
    int totalSize = 0;
    for (Message m: messages) {
      if (m == null) {
        continue;
      }
      totalSize += m.getSerializedSize();
      totalSize += CodedOutputStream.computeRawVarint32Size(m.getSerializedSize());
    }
    Preconditions.checkArgument(totalSize < Integer.MAX_VALUE);
    return totalSize;
  }
}
