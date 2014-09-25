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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Do different kinds of data block encoding according to column family
 * options.
 */
@InterfaceAudience.Private
public class HFileDataBlockEncoderImpl implements HFileDataBlockEncoder {
  private final DataBlockEncoding encoding;

  /**
   * Do data block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileDataBlockEncoderImpl(DataBlockEncoding encoding) {
    this.encoding = encoding != null ? encoding : DataBlockEncoding.NONE;
  }

  public static HFileDataBlockEncoder createFromFileInfo(
      FileInfo fileInfo) throws IOException {
    DataBlockEncoding encoding = DataBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(DATA_BLOCK_ENCODING);
    if (dataBlockEncodingType != null) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException("Invalid data block encoding type in file info: "
          + dataBlockEncodingStr, ex);
      }
    }

    if (encoding == DataBlockEncoding.NONE) {
      return NoOpDataBlockEncoder.INSTANCE;
    }
    return new HFileDataBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
    writer.appendFileInfo(DATA_BLOCK_ENCODING, encoding.getNameInBytes());
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  /**
   * Precondition: a non-encoded buffer. Postcondition: on-disk encoding.
   *
   * The encoded results can be stored in {@link HFileBlockEncodingContext}.
   *
   * @throws IOException
   */
  @Override
  public void beforeWriteToDisk(ByteBuffer in,
      HFileBlockEncodingContext encodeCtx,
      BlockType blockType) throws IOException {
    if (encoding == DataBlockEncoding.NONE) {
      // there is no need to encode the block before writing it to disk
      ((HFileBlockDefaultEncodingContext) encodeCtx).compressAfterEncodingWithBlockType(
          in.array(), blockType);
      return;
    }
    encodeBufferToHFileBlockBuffer(in, encoding, encodeCtx);
  }

  @Override
  public boolean useEncodedScanner() {
    return encoding != DataBlockEncoding.NONE;
  }

  /**
   * Encode a block of key value pairs.
   *
   * @param in input data to encode
   * @param algo encoding algorithm
   * @param encodeCtx where will the output data be stored
   */
  private void encodeBufferToHFileBlockBuffer(ByteBuffer in, DataBlockEncoding algo,
      HFileBlockEncodingContext encodeCtx) {
    DataBlockEncoder encoder = algo.getEncoder();
    try {
      encoder.encodeKeyValues(in, encodeCtx);
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Bug in data block encoder "
              + "'%s', it probably requested too much data, " +
              "exception message: %s.",
              algo.toString(), e.getMessage()), e);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(encoding=" + encoding + ")";
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(
      byte[] dummyHeader, HFileContext fileContext) {
    DataBlockEncoder encoder = encoding.getEncoder();
    if (encoder != null) {
      return encoder.newDataBlockEncodingContext(encoding, dummyHeader, fileContext);
    }
    return new HFileBlockDefaultEncodingContext(null, dummyHeader, fileContext);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext fileContext) {
    DataBlockEncoder encoder = encoding.getEncoder();
    if (encoder != null) {
      return encoder.newDataBlockDecodingContext(fileContext);
    }
    return new HFileBlockDefaultDecodingContext(fileContext);
  }
}
