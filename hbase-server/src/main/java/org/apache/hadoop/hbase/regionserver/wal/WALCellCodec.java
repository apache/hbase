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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.BaseDecoder;
import org.apache.hadoop.hbase.codec.BaseEncoder;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.util.Dictionary;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;

import com.google.protobuf.ByteString;


/**
 * Compression in this class is lifted off Compressor/KeyValueCompression.
 * This is a pure coincidence... they are independent and don't have to be compatible.
 *
 * This codec is used at server side for writing cells to WAL as well as for sending edits
 * as part of the distributed splitting process.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC,
  HBaseInterfaceAudience.PHOENIX, HBaseInterfaceAudience.CONFIG})
public class WALCellCodec implements Codec {
  /** Configuration key for the class to use when encoding cells in the WAL */
  public static final String WAL_CELL_CODEC_CLASS_KEY = "hbase.regionserver.wal.codec";

  protected final CompressionContext compression;
  protected final ByteStringUncompressor statelessUncompressor = new ByteStringUncompressor() {
    @Override
    public byte[] uncompress(ByteString data, Dictionary dict) throws IOException {
      return WALCellCodec.uncompressByteString(data, dict);
    }
  };

  /**
   * <b>All subclasses must implement a no argument constructor</b>
   */
  public WALCellCodec() {
    this.compression = null;
  }

  /**
   * Default constructor - <b>all subclasses must implement a constructor with this signature </b>
   * if they are to be dynamically loaded from the {@link Configuration}.
   * @param conf configuration to configure <tt>this</tt>
   * @param compression compression the codec should support, can be <tt>null</tt> to indicate no
   *          compression
   */
  public WALCellCodec(Configuration conf, CompressionContext compression) {
    this.compression = compression;
  }

  public static String getWALCellCodecClass(Configuration conf) {
    return conf.get(WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
  }

  /**
   * Create and setup a {@link WALCellCodec} from the {@code cellCodecClsName} and
   * CompressionContext, if {@code cellCodecClsName} is specified.
   * Otherwise Cell Codec classname is read from {@link Configuration}.
   * Fully prepares the codec for use.
   * @param conf {@link Configuration} to read for the user-specified codec. If none is specified,
   *          uses a {@link WALCellCodec}.
   * @param cellCodecClsName name of codec
   * @param compression compression the codec should use
   * @return a {@link WALCellCodec} ready for use.
   * @throws UnsupportedOperationException if the codec cannot be instantiated
   */

  public static WALCellCodec create(Configuration conf, String cellCodecClsName,
      CompressionContext compression) throws UnsupportedOperationException {
    if (cellCodecClsName == null) {
      cellCodecClsName = getWALCellCodecClass(conf);
    }
    return ReflectionUtils.instantiateWithCustomCtor(cellCodecClsName, new Class[]
        { Configuration.class, CompressionContext.class }, new Object[] { conf, compression });
  }

  /**
   * Create and setup a {@link WALCellCodec} from the
   * CompressionContext.
   * Cell Codec classname is read from {@link Configuration}.
   * Fully prepares the codec for use.
   * @param conf {@link Configuration} to read for the user-specified codec. If none is specified,
   *          uses a {@link WALCellCodec}.
   * @param compression compression the codec should use
   * @return a {@link WALCellCodec} ready for use.
   * @throws UnsupportedOperationException if the codec cannot be instantiated
   */
  public static WALCellCodec create(Configuration conf,
      CompressionContext compression) throws UnsupportedOperationException {
    String cellCodecClsName = getWALCellCodecClass(conf);
    return ReflectionUtils.instantiateWithCustomCtor(cellCodecClsName, new Class[]
        { Configuration.class, CompressionContext.class }, new Object[] { conf, compression });
  }

  public interface ByteStringCompressor {
    ByteString compress(byte[] data, Dictionary dict) throws IOException;
  }

  public interface ByteStringUncompressor {
    byte[] uncompress(ByteString data, Dictionary dict) throws IOException;
  }

  // TODO: it sucks that compression context is in WAL.Entry. It'd be nice if it was here.
  //       Dictionary could be gotten by enum; initially, based on enum, context would create
  //       an array of dictionaries.
  static class BaosAndCompressor extends ByteArrayOutputStream implements ByteStringCompressor {
    public ByteString toByteString() {
      return ByteString.copyFrom(this.buf, 0, this.count);
    }

    @Override
    public ByteString compress(byte[] data, Dictionary dict) throws IOException {
      writeCompressed(data, dict);
      ByteString result = ByteString.copyFrom(this.buf, 0, this.count);
      reset(); // Only resets the count - we reuse the byte array.
      return result;
    }

    private void writeCompressed(byte[] data, Dictionary dict) throws IOException {
      assert dict != null;
      short dictIdx = dict.findEntry(data, 0, data.length);
      if (dictIdx == Dictionary.NOT_IN_DICTIONARY) {
        write(Dictionary.NOT_IN_DICTIONARY);
        StreamUtils.writeRawVInt32(this, data.length);
        write(data, 0, data.length);
      } else {
        StreamUtils.writeShort(this, dictIdx);
      }
    }
  }

  private static byte[] uncompressByteString(ByteString bs, Dictionary dict) throws IOException {
    InputStream in = bs.newInput();
    byte status = (byte)in.read();
    if (status == Dictionary.NOT_IN_DICTIONARY) {
      byte[] arr = new byte[StreamUtils.readRawVarint32(in)];
      int bytesRead = in.read(arr);
      if (bytesRead != arr.length) {
        throw new IOException("Cannot read; wanted " + arr.length + ", but got " + bytesRead);
      }
      if (dict != null) dict.addEntry(arr, 0, arr.length);
      return arr;
    } else {
      // Status here is the higher-order byte of index of the dictionary entry.
      short dictIdx = StreamUtils.toShort(status, (byte)in.read());
      byte[] entry = dict.getEntry(dictIdx);
      if (entry == null) {
        throw new IOException("Missing dictionary entry for index " + dictIdx);
      }
      return entry;
    }
  }

  static class CompressedKvEncoder extends BaseEncoder {
    private final CompressionContext compression;
    public CompressedKvEncoder(OutputStream out, CompressionContext compression) {
      super(out);
      this.compression = compression;
    }

    @Override
    public void write(Cell cell) throws IOException {
      // We first write the KeyValue infrastructure as VInts.
      StreamUtils.writeRawVInt32(out, KeyValueUtil.keyLength(cell));
      StreamUtils.writeRawVInt32(out, cell.getValueLength());
      // To support tags
      int tagsLength = cell.getTagsLength();
      StreamUtils.writeRawVInt32(out, tagsLength);

      // Write row, qualifier, and family; use dictionary
      // compression as they're likely to have duplicates.
      write(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), compression.rowDict);
      write(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          compression.familyDict);
      write(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
          compression.qualifierDict);

      // Write timestamp, type and value as uncompressed.
      StreamUtils.writeLong(out, cell.getTimestamp());
      out.write(cell.getTypeByte());
      out.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      if (tagsLength > 0) {
        if (compression.tagCompressionContext != null) {
          // Write tags using Dictionary compression
          compression.tagCompressionContext.compressTags(out, cell.getTagsArray(),
              cell.getTagsOffset(), tagsLength);
        } else {
          // Tag compression is disabled within the WAL compression. Just write the tags bytes as
          // it is.
          out.write(cell.getTagsArray(), cell.getTagsOffset(), tagsLength);
        }
      }
    }

    private void write(byte[] data, int offset, int length, Dictionary dict) throws IOException {
      short dictIdx = Dictionary.NOT_IN_DICTIONARY;
      if (dict != null) {
        dictIdx = dict.findEntry(data, offset, length);
      }
      if (dictIdx == Dictionary.NOT_IN_DICTIONARY) {
        out.write(Dictionary.NOT_IN_DICTIONARY);
        StreamUtils.writeRawVInt32(out, length);
        out.write(data, offset, length);
      } else {
        StreamUtils.writeShort(out, dictIdx);
      }
    }
  }

  static class CompressedKvDecoder extends BaseDecoder {
    private final CompressionContext compression;
    public CompressedKvDecoder(InputStream in, CompressionContext compression) {
      super(in);
      this.compression = compression;
    }

    @Override
    protected Cell parseCell() throws IOException {
      int keylength = StreamUtils.readRawVarint32(in);
      int vlength = StreamUtils.readRawVarint32(in);

      int tagsLength = StreamUtils.readRawVarint32(in);
      int length = 0;
      if(tagsLength == 0) {
        length = KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength;
      } else {
        length = KeyValue.KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE + keylength + vlength + tagsLength;
      }

      byte[] backingArray = new byte[length];
      int pos = 0;
      pos = Bytes.putInt(backingArray, pos, keylength);
      pos = Bytes.putInt(backingArray, pos, vlength);

      // the row
      int elemLen = readIntoArray(backingArray, pos + Bytes.SIZEOF_SHORT, compression.rowDict);
      checkLength(elemLen, Short.MAX_VALUE);
      pos = Bytes.putShort(backingArray, pos, (short)elemLen);
      pos += elemLen;

      // family
      elemLen = readIntoArray(backingArray, pos + Bytes.SIZEOF_BYTE, compression.familyDict);
      checkLength(elemLen, Byte.MAX_VALUE);
      pos = Bytes.putByte(backingArray, pos, (byte)elemLen);
      pos += elemLen;

      // qualifier
      elemLen = readIntoArray(backingArray, pos, compression.qualifierDict);
      pos += elemLen;

      // timestamp, type and value
      int tsTypeValLen = length - pos;
      if (tagsLength > 0) {
        tsTypeValLen = tsTypeValLen - tagsLength - KeyValue.TAGS_LENGTH_SIZE;
      }
      IOUtils.readFully(in, backingArray, pos, tsTypeValLen);
      pos += tsTypeValLen;

      // tags
      if (tagsLength > 0) {
        pos = Bytes.putAsShort(backingArray, pos, tagsLength);
        if (compression.tagCompressionContext != null) {
          compression.tagCompressionContext.uncompressTags(in, backingArray, pos, tagsLength);
        } else {
          IOUtils.readFully(in, backingArray, pos, tagsLength);
        }
      }
      return new KeyValue(backingArray, 0, length);
    }

    private int readIntoArray(byte[] to, int offset, Dictionary dict) throws IOException {
      byte status = (byte)in.read();
      if (status == Dictionary.NOT_IN_DICTIONARY) {
        // status byte indicating that data to be read is not in dictionary.
        // if this isn't in the dictionary, we need to add to the dictionary.
        int length = StreamUtils.readRawVarint32(in);
        IOUtils.readFully(in, to, offset, length);
        dict.addEntry(to, offset, length);
        return length;
      } else {
        // the status byte also acts as the higher order byte of the dictionary entry.
        short dictIdx = StreamUtils.toShort(status, (byte)in.read());
        byte[] entry = dict.getEntry(dictIdx);
        if (entry == null) {
          throw new IOException("Missing dictionary entry for index " + dictIdx);
        }
        // now we write the uncompressed value.
        Bytes.putBytes(to, offset, entry, 0, entry.length);
        return entry.length;
      }
    }

    private static void checkLength(int len, int max) throws IOException {
      if (len < 0 || len > max) {
        throw new IOException("Invalid length for compresesed portion of keyvalue: " + len);
      }
    }
  }

  public static class EnsureKvEncoder extends BaseEncoder {
    public EnsureKvEncoder(OutputStream out) {
      super(out);
    }
    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      // Make sure to write tags into WAL
      KeyValueUtil.oswrite(cell, this.out, true);
    }
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return (compression == null)
        ? new KeyValueCodecWithTags.KeyValueDecoder(is) : new CompressedKvDecoder(is, compression);
  }

  @Override
  public Decoder getDecoder(ByteBuffer buf) {
    return getDecoder(new ByteBufferInputStream(buf));
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return (compression == null)
        ? new EnsureKvEncoder(os) : new CompressedKvEncoder(os, compression);
  }

  public ByteStringCompressor getByteStringCompressor() {
    // TODO: ideally this should also encapsulate compressionContext
    return new BaosAndCompressor();
  }

  public ByteStringUncompressor getByteStringUncompressor() {
    // TODO: ideally this should also encapsulate compressionContext
    return this.statelessUncompressor;
  }
}
