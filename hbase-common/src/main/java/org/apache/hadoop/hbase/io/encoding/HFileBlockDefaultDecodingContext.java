/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A default implementation of {@link HFileBlockDecodingContext}. It assumes the
 * block data section is compressed as a whole.
 *
 * @see HFileBlockDefaultEncodingContext for the default compression context
 *
 */
@InterfaceAudience.Private
public class HFileBlockDefaultDecodingContext implements
    HFileBlockDecodingContext {
  private final HFileContext fileContext;
  private TagCompressionContext tagCompressionContext;
  
  public HFileBlockDefaultDecodingContext(HFileContext fileContext) {
    this.fileContext = fileContext;
  }

  @Override
  public void prepareDecoding(int onDiskSizeWithoutHeader, int uncompressedSizeWithoutHeader,
      ByteBuffer blockBufferWithoutHeader, ByteBuffer onDiskBlock) throws IOException {
    InputStream in = new DataInputStream(new ByteBufferInputStream(onDiskBlock));

    Encryption.Context cryptoContext = fileContext.getEncryptionContext();
    if (cryptoContext != Encryption.Context.NONE) {

      Cipher cipher = cryptoContext.getCipher();
      Decryptor decryptor = cipher.getDecryptor();
      decryptor.setKey(cryptoContext.getKey());

      // Encrypted block format:
      // +--------------------------+
      // | byte iv length           |
      // +--------------------------+
      // | iv data ...              |
      // +--------------------------+
      // | encrypted block data ... |
      // +--------------------------+

      int ivLength = in.read();
      if (ivLength > 0) {
        byte[] iv = new byte[ivLength];
        IOUtils.readFully(in, iv);
        decryptor.setIv(iv);
        // All encrypted blocks will have a nonzero IV length. If we see an IV
        // length of zero, this means the encoding context had 0 bytes of
        // plaintext to encode.
        decryptor.reset();
        in = decryptor.createDecryptionStream(in);
      }
      onDiskSizeWithoutHeader -= Bytes.SIZEOF_BYTE + ivLength;
    }

    Compression.Algorithm compression = fileContext.getCompression();
    assert blockBufferWithoutHeader.hasArray();
    if (compression != Compression.Algorithm.NONE) {
      Compression.decompress(blockBufferWithoutHeader.array(),
        blockBufferWithoutHeader.arrayOffset(), in, onDiskSizeWithoutHeader,
        uncompressedSizeWithoutHeader, compression);
    } else {
      IOUtils.readFully(in, blockBufferWithoutHeader.array(),
        blockBufferWithoutHeader.arrayOffset(), onDiskSizeWithoutHeader);
    }
  }

  @Override
  public HFileContext getHFileContext() {
    return this.fileContext;
  }

  public TagCompressionContext getTagCompressionContext() {
    return tagCompressionContext;
  }

  public void setTagCompressionContext(TagCompressionContext tagCompressionContext) {
    this.tagCompressionContext = tagCompressionContext;
  }
}
