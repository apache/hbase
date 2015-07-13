/*
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

package org.apache.hadoop.hbase.codec.prefixtree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.CellComparator.MetaCellComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.codec.prefixtree.decode.DecoderFactory;
import org.apache.hadoop.hbase.codec.prefixtree.decode.PrefixTreeArraySearcher;
import org.apache.hadoop.hbase.codec.prefixtree.encode.EncoderFactory;
import org.apache.hadoop.hbase.codec.prefixtree.encode.PrefixTreeEncoder;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.EncodingState;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.io.WritableUtils;

/**
 * <p>
 * This class is created via reflection in DataBlockEncoding enum. Update the enum if class name or
 * package changes.
 * </p>
 * PrefixTreeDataBlockEncoder implementation of DataBlockEncoder. This is the primary entry point
 * for PrefixTree encoding and decoding. Encoding is delegated to instances of
 * {@link PrefixTreeEncoder}, and decoding is delegated to instances of
 * {@link org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher}. Encoder and decoder instances are
 * created and recycled by static PtEncoderFactory and PtDecoderFactory.
 */
@InterfaceAudience.Private
public class PrefixTreeCodec implements DataBlockEncoder {

  /**
   * no-arg constructor for reflection
   */
  public PrefixTreeCodec() {
  }

  @Override
  public ByteBuffer decodeKeyValues(DataInputStream source, HFileBlockDecodingContext decodingCtx)
      throws IOException {
    return decodeKeyValues(source, 0, 0, decodingCtx);
  }


  /**
   * I don't think this method is called during normal HBase operation, so efficiency is not
   * important.
   */
  public ByteBuffer decodeKeyValues(DataInputStream source, int allocateHeaderLength,
      int skipLastBytes, HFileBlockDecodingContext decodingCtx) throws IOException {
    ByteBuffer sourceAsBuffer = ByteBufferUtils.drainInputStreamToBuffer(source);// waste
    sourceAsBuffer.mark();
    PrefixTreeBlockMeta blockMeta = new PrefixTreeBlockMeta(sourceAsBuffer);
    sourceAsBuffer.rewind();
    int numV1BytesWithHeader = allocateHeaderLength + blockMeta.getNumKeyValueBytes();
    byte[] keyValueBytesWithHeader = new byte[numV1BytesWithHeader];
    ByteBuffer result = ByteBuffer.wrap(keyValueBytesWithHeader);
    result.rewind();
    CellSearcher searcher = null;
    try {
      boolean includesMvcc = decodingCtx.getHFileContext().isIncludesMvcc();
      searcher = DecoderFactory.checkOut(sourceAsBuffer, includesMvcc);
      while (searcher.advance()) {
        KeyValue currentCell = KeyValueUtil.copyToNewKeyValue(searcher.current());
        // needs to be modified for DirectByteBuffers. no existing methods to
        // write VLongs to byte[]
        int offset = result.arrayOffset() + result.position();
        System.arraycopy(currentCell.getBuffer(), currentCell.getOffset(), result.array(), offset,
            currentCell.getLength());
        int keyValueLength = KeyValueUtil.length(currentCell);
        ByteBufferUtils.skip(result, keyValueLength);
        offset += keyValueLength;
        if (includesMvcc) {
          ByteBufferUtils.writeVLong(result, currentCell.getSequenceId());
        }
      }
      result.position(result.limit());//make it appear as if we were appending
      return result;
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }


  @Override
  public Cell getFirstKeyCellInBlock(ByteBuffer block) {
    block.rewind();
    PrefixTreeArraySearcher searcher = null;
    try {
      // should i includeMemstoreTS (second argument)?  i think PrefixKeyDeltaEncoder is, so i will
      searcher = DecoderFactory.checkOut(block, true);
      if (!searcher.positionAtFirstCell()) {
        return null;
      }
      return searcher.current();
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(
      DataBlockEncoding encoding, byte[] header, HFileContext meta) {
    if(DataBlockEncoding.PREFIX_TREE != encoding){
      //i'm not sure why encoding is in the interface.  Each encoder implementation should probably
      //know it's encoding type
      throw new IllegalArgumentException("only DataBlockEncoding.PREFIX_TREE supported");
    }
    return new HFileBlockDefaultEncodingContext(encoding, header, meta);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext meta) {
    return new HFileBlockDefaultDecodingContext(meta);
  }

  /**
   * Is this the correct handling of an illegal comparator?  How to prevent that from getting all
   * the way to this point.
   */
  @Override
  public EncodedSeeker createSeeker(CellComparator comparator,
      HFileBlockDecodingContext decodingCtx) {
    if (comparator instanceof MetaCellComparator) {
      throw new IllegalArgumentException(
          "DataBlockEncoding.PREFIX_TREE not compatible with hbase:meta " + "table");
    }

    return new PrefixTreeSeeker(decodingCtx.getHFileContext().isIncludesMvcc());
  }

  @Override
  public int encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
      throws IOException {
    PrefixTreeEncodingState state = (PrefixTreeEncodingState) encodingCtx.getEncodingState();
    PrefixTreeEncoder builder = state.builder;
    builder.write(cell);
    int size = KeyValueUtil.length(cell);
    if (encodingCtx.getHFileContext().isIncludesMvcc()) {
      size += WritableUtils.getVIntSize(cell.getSequenceId());
    }
    return size;
  }

  private static class PrefixTreeEncodingState extends EncodingState {
    PrefixTreeEncoder builder = null;
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext blkEncodingCtx, DataOutputStream out)
      throws IOException {
    if (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class) {
      throw new IOException(this.getClass().getName() + " only accepts "
          + HFileBlockDefaultEncodingContext.class.getName() + " as the " + "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx = 
        (HFileBlockDefaultEncodingContext) blkEncodingCtx;
    encodingCtx.prepareEncoding(out);

    PrefixTreeEncoder builder = EncoderFactory.checkOut(out, encodingCtx.getHFileContext()
        .isIncludesMvcc());
    PrefixTreeEncodingState state = new PrefixTreeEncodingState();
    state.builder = builder;
    blkEncodingCtx.setEncodingState(state);
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
      byte[] uncompressedBytesWithHeader) throws IOException {
    PrefixTreeEncodingState state = (PrefixTreeEncodingState) encodingCtx.getEncodingState();
    PrefixTreeEncoder builder = state.builder;
    builder.flush();
    EncoderFactory.checkIn(builder);
    // do i need to check this, or will it always be DataBlockEncoding.PREFIX_TREE?
    if (encodingCtx.getDataBlockEncoding() != DataBlockEncoding.NONE) {
      encodingCtx.postEncoding(BlockType.ENCODED_DATA);
    } else {
      encodingCtx.postEncoding(BlockType.DATA);
    }
  }
}
