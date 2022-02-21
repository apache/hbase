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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.io.ByteBufferWriterOutputStream;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A WALCellCodec that encrypts the WALedits.
 */
@InterfaceAudience.Private
public class SecureWALCellCodec extends WALCellCodec {

  private Encryptor encryptor;
  private Decryptor decryptor;

  public SecureWALCellCodec(Configuration conf, CompressionContext compression) {
    super(conf, compression);
  }

  public SecureWALCellCodec(Configuration conf, Encryptor encryptor) {
    super(conf, null);
    this.encryptor = encryptor;
  }

  public SecureWALCellCodec(Configuration conf, Decryptor decryptor) {
    super(conf, null);
    this.decryptor = decryptor;
  }

  static class EncryptedKvDecoder extends KeyValueCodecWithTags.KeyValueDecoder {

    private Decryptor decryptor;
    private byte[] iv;

    public EncryptedKvDecoder(InputStream in) {
      super(in);
    }

    public EncryptedKvDecoder(InputStream in, Decryptor decryptor) {
      super(in);
      this.decryptor = decryptor;
      if (decryptor != null) {
        this.iv = new byte[decryptor.getIvLength()];
      }
    }

    @Override
    protected Cell parseCell() throws IOException {
      if (this.decryptor == null) {
        return super.parseCell();
      }
      int ivLength = 0;

      ivLength = StreamUtils.readRawVarint32(in);

      // TODO: An IV length of 0 could signify an unwrapped cell, when the
      // encoder supports that just read the remainder in directly

      if (ivLength != this.iv.length) {
        throw new IOException("Incorrect IV length: expected=" + iv.length + " have=" +
          ivLength);
      }
      IOUtils.readFully(in, this.iv);

      int codedLength = StreamUtils.readRawVarint32(in);
      byte[] codedBytes = new byte[codedLength];
      IOUtils.readFully(in, codedBytes);

      decryptor.setIv(iv);
      decryptor.reset();

      InputStream cin = decryptor.createDecryptionStream(new ByteArrayInputStream(codedBytes));

      // TODO: Add support for WAL compression

      int keylength = StreamUtils.readRawVarint32(cin);
      int vlength = StreamUtils.readRawVarint32(cin);
      int tagsLength = StreamUtils.readRawVarint32(cin);
      int length = 0;
      if (tagsLength == 0) {
        length = KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength;
      } else {
        length = KeyValue.KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE + keylength + vlength + tagsLength;
      }

      byte[] backingArray = new byte[length];
      int pos = 0;
      pos = Bytes.putInt(backingArray, pos, keylength);
      pos = Bytes.putInt(backingArray, pos, vlength);

      // Row
      int elemLen = StreamUtils.readRawVarint32(cin);
      pos = Bytes.putShort(backingArray, pos, (short)elemLen);
      IOUtils.readFully(cin, backingArray, pos, elemLen);
      pos += elemLen;
      // Family
      elemLen = StreamUtils.readRawVarint32(cin);
      pos = Bytes.putByte(backingArray, pos, (byte)elemLen);
      IOUtils.readFully(cin, backingArray, pos, elemLen);
      pos += elemLen;
      // Qualifier
      elemLen = StreamUtils.readRawVarint32(cin);
      IOUtils.readFully(cin, backingArray, pos, elemLen);
      pos += elemLen;
      // Remainder
      IOUtils.readFully(cin, backingArray, pos, length - pos);
      return new KeyValue(backingArray, 0, length);
    }

  }

  static class EncryptedKvEncoder extends KeyValueCodecWithTags.KeyValueEncoder {

    private Encryptor encryptor;
    private final ThreadLocal<byte[]> iv = new ThreadLocal<byte[]>() {
      @Override
      protected byte[] initialValue() {
        byte[] iv = new byte[encryptor.getIvLength()];
        Bytes.secureRandom(iv);
        return iv;
      }
    };

    protected byte[] nextIv() {
      byte[] b = iv.get(), ret = new byte[b.length];
      System.arraycopy(b, 0, ret, 0, b.length);
      return ret;
    }

    protected void incrementIv(int v) {
      Encryption.incrementIv(iv.get(), 1 + (v / encryptor.getBlockSize()));
    }

    public EncryptedKvEncoder(OutputStream os) {
      super(os);
    }

    public EncryptedKvEncoder(OutputStream os, Encryptor encryptor) {
      super(os);
      this.encryptor = encryptor;
    }

    @Override
    public void write(Cell cell) throws IOException {
      if (encryptor == null) {
        super.write(cell);
        return;
      }

      byte[] iv = nextIv();
      encryptor.setIv(iv);
      encryptor.reset();

      // TODO: Check if this is a cell for an encrypted CF. If not, we can
      // write a 0 here to signal an unwrapped cell and just dump the KV bytes
      // afterward

      StreamUtils.writeRawVInt32(out, iv.length);
      out.write(iv);

      // TODO: Add support for WAL compression

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      OutputStream cout = encryptor.createEncryptionStream(baos);
      ByteBufferWriterOutputStream bos = new ByteBufferWriterOutputStream(cout);
      int tlen = cell.getTagsLength();
      // Write the KeyValue infrastructure as VInts.
      StreamUtils.writeRawVInt32(bos, KeyValueUtil.keyLength(cell));
      StreamUtils.writeRawVInt32(bos, cell.getValueLength());
      // To support tags
      StreamUtils.writeRawVInt32(bos, tlen);

      // Write row, qualifier, and family
      short rowLength = cell.getRowLength();
      StreamUtils.writeRawVInt32(bos, rowLength);
      PrivateCellUtil.writeRow(bos, cell, rowLength);
      byte familyLength = cell.getFamilyLength();
      StreamUtils.writeRawVInt32(bos, familyLength);
      PrivateCellUtil.writeFamily(bos, cell, familyLength);
      int qualifierLength = cell.getQualifierLength();
      StreamUtils.writeRawVInt32(bos, qualifierLength);
      PrivateCellUtil.writeQualifier(bos, cell, qualifierLength);
      // Write the rest ie. ts, type, value and tags parts
      StreamUtils.writeLong(bos, cell.getTimestamp());
      bos.write(cell.getTypeByte());
      PrivateCellUtil.writeValue(bos, cell, cell.getValueLength());
      if (tlen > 0) {
        PrivateCellUtil.writeTags(bos, cell, tlen);
      }
      bos.close();

      StreamUtils.writeRawVInt32(out, baos.size());
      baos.writeTo(out);

      // Increment IV given the final payload length
      incrementIv(baos.size());
    }

  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return new EncryptedKvDecoder(is, decryptor);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new EncryptedKvEncoder(os, encryptor);
  }

  public static WALCellCodec getCodec(Configuration conf, Encryptor encryptor) {
    return new SecureWALCellCodec(conf, encryptor);
  }

  public static WALCellCodec getCodec(Configuration conf, Decryptor decryptor) {
    return new SecureWALCellCodec(conf, decryptor);
  }

}
