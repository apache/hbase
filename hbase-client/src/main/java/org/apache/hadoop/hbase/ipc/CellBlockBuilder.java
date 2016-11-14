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
package org.apache.hadoop.hbase.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.ByteBufferListOutputStream;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Helper class for building cell block.
 */
@InterfaceAudience.Private
class CellBlockBuilder {

  // LOG is being used in TestCellBlockBuilder
  static final Log LOG = LogFactory.getLog(CellBlockBuilder.class);

  private final Configuration conf;

  /**
   * How much we think the decompressor will expand the original compressed content.
   */
  private final int cellBlockDecompressionMultiplier;

  private final int cellBlockBuildingInitialBufferSize;

  public CellBlockBuilder(Configuration conf) {
    this.conf = conf;
    this.cellBlockDecompressionMultiplier = conf
        .getInt("hbase.ipc.cellblock.decompression.buffersize.multiplier", 3);

    // Guess that 16k is a good size for rpc buffer. Could go bigger. See the TODO below in
    // #buildCellBlock.
    this.cellBlockBuildingInitialBufferSize = ClassSize
        .align(conf.getInt("hbase.ipc.cellblock.building.initial.buffersize", 16 * 1024));
  }

  private interface OutputStreamSupplier {

    OutputStream get(int expectedSize);

    int size();
  }

  private static final class ByteBufferOutputStreamSupplier implements OutputStreamSupplier {

    private ByteBufferOutputStream baos;

    @Override
    public OutputStream get(int expectedSize) {
      baos = new ByteBufferOutputStream(expectedSize);
      return baos;
    }

    @Override
    public int size() {
      return baos.size();
    }
  }

  /**
   * Puts CellScanner Cells into a cell block using passed in <code>codec</code> and/or
   * <code>compressor</code>.
   * @param codec
   * @param compressor
   * @param cellScanner
   * @return Null or byte buffer filled with a cellblock filled with passed-in Cells encoded using
   *         passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has
   *         been flipped and is ready for reading. Use limit to find total size.
   * @throws IOException
   */
  public ByteBuffer buildCellBlock(final Codec codec, final CompressionCodec compressor,
      final CellScanner cellScanner) throws IOException {
    ByteBufferOutputStreamSupplier supplier = new ByteBufferOutputStreamSupplier();
    if (buildCellBlock(codec, compressor, cellScanner, supplier)) {
      ByteBuffer bb = supplier.baos.getByteBuffer();
      // If no cells, don't mess around. Just return null (could be a bunch of existence checking
      // gets or something -- stuff that does not return a cell).
      return bb.hasRemaining() ? bb : null;
    } else {
      return null;
    }
  }

  private static final class ByteBufOutputStreamSupplier implements OutputStreamSupplier {

    private final ByteBufAllocator alloc;

    private ByteBuf buf;

    public ByteBufOutputStreamSupplier(ByteBufAllocator alloc) {
      this.alloc = alloc;
    }

    @Override
    public OutputStream get(int expectedSize) {
      buf = alloc.buffer(expectedSize);
      return new ByteBufOutputStream(buf);
    }

    @Override
    public int size() {
      return buf.writerIndex();
    }
  }

  public ByteBuf buildCellBlock(Codec codec, CompressionCodec compressor, CellScanner cellScanner,
      ByteBufAllocator alloc) throws IOException {
    ByteBufOutputStreamSupplier supplier = new ByteBufOutputStreamSupplier(alloc);
    if (buildCellBlock(codec, compressor, cellScanner, supplier)) {
      return supplier.buf;
    } else {
      return null;
    }
  }

  private boolean buildCellBlock(final Codec codec, final CompressionCodec compressor,
      final CellScanner cellScanner, OutputStreamSupplier supplier) throws IOException {
    if (cellScanner == null) {
      return false;
    }
    if (codec == null) {
      throw new CellScannerButNoCodecException();
    }
    int bufferSize = cellBlockBuildingInitialBufferSize;
    encodeCellsTo(supplier.get(bufferSize), cellScanner, codec, compressor);
    if (LOG.isTraceEnabled() && bufferSize < supplier.size()) {
      LOG.trace("Buffer grew from initial bufferSize=" + bufferSize + " to " + supplier.size()
          + "; up hbase.ipc.cellblock.building.initial.buffersize?");
    }
    return true;
  }

  private void encodeCellsTo(OutputStream os, CellScanner cellScanner, Codec codec,
      CompressionCodec compressor) throws IOException {
    Compressor poolCompressor = null;
    try {
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
    } catch (BufferOverflowException | IndexOutOfBoundsException e) {
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
   *         passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has
   *         been flipped and is ready for reading. Use limit to find total size. If
   *         <code>pool</code> was not null, then this returned ByteBuffer came from there and
   *         should be returned to the pool when done.
   * @throws IOException if encoding the cells fail
   */
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
    if (compressor != null) {
      ByteBuffer cellBlockBuf = decompress(compressor, cellBlock);
      return codec.getDecoder(new ByteBufferInputStream(cellBlockBuf));
    }
    // Not making the Decoder over the ByteBuffer purposefully. The Decoder over the BB will
    // make Cells directly over the passed BB. This method is called at client side and we don't
    // want the Cells to share the same byte[] where the RPC response is being read. Caching of any
    // of the Cells at user's app level will make it not possible to GC the response byte[]
    return codec.getDecoder(new ByteArrayInputStream(cellBlock));
  }

  /**
   * @param codec to use for cellblock
   * @param cellBlock ByteBuffer containing the cells written by the Codec. The buffer should be
   *          position()'ed at the start of the cell block and limit()'ed at the end.
   * @return CellScanner to work against the content of <code>cellBlock</code>. All cells created
   *         out of the CellScanner will share the same ByteBuffer being passed.
   * @throws IOException if cell encoding fails
   */
  public CellScanner createCellScannerReusingBuffers(final Codec codec,
      final CompressionCodec compressor, ByteBuff cellBlock) throws IOException {
    // Use this method from HRS to create the CellScanner
    // If compressed, decompress it first before passing it on else we will leak compression
    // resources if the stream is not closed properly after we let it out.
    if (compressor != null) {
      cellBlock = decompress(compressor, cellBlock);
    }
    return codec.getDecoder(cellBlock);
  }

  private ByteBuffer decompress(CompressionCodec compressor, byte[] compressedCellBlock)
      throws IOException {
    ByteBuffer cellBlock = decompress(compressor, new ByteArrayInputStream(compressedCellBlock),
        compressedCellBlock.length * this.cellBlockDecompressionMultiplier);
    return cellBlock;
  }

  private ByteBuff decompress(CompressionCodec compressor, ByteBuff compressedCellBlock)
      throws IOException {
    ByteBuffer cellBlock = decompress(compressor, new ByteBuffInputStream(compressedCellBlock),
        compressedCellBlock.remaining() * this.cellBlockDecompressionMultiplier);
    return new SingleByteBuff(cellBlock);
  }

  private ByteBuffer decompress(CompressionCodec compressor, InputStream cellBlockStream,
      int osInitialSize) throws IOException {
    // GZIPCodec fails w/ NPE if no configuration.
    if (compressor instanceof Configurable) {
      ((Configurable) compressor).setConf(this.conf);
    }
    Decompressor poolDecompressor = CodecPool.getDecompressor(compressor);
    CompressionInputStream cis = compressor.createInputStream(cellBlockStream, poolDecompressor);
    ByteBufferOutputStream bbos;
    try {
      // TODO: This is ugly. The buffer will be resized on us if we guess wrong.
      // TODO: Reuse buffers.
      bbos = new ByteBufferOutputStream(osInitialSize);
      IOUtils.copy(cis, bbos);
      bbos.close();
      return bbos.getByteBuffer();
    } finally {
      CodecPool.returnDecompressor(poolDecompressor);
    }
  }
}
