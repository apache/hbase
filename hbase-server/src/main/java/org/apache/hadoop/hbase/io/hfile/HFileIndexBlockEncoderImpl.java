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
package org.apache.hadoop.hbase.io.hfile;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Do different kinds of index block encoding according to column family options.
 */
@InterfaceAudience.Private
public class HFileIndexBlockEncoderImpl implements HFileIndexBlockEncoder {
  private final IndexBlockEncoding indexBlockEncoding;

  /**
   * Do index block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileIndexBlockEncoderImpl(IndexBlockEncoding encoding) {
    this.indexBlockEncoding = encoding != null ? encoding : IndexBlockEncoding.NONE;
  }

  public static HFileIndexBlockEncoder createFromFileInfo(HFileInfo fileInfo) throws IOException {
    IndexBlockEncoding encoding = IndexBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(INDEX_BLOCK_ENCODING);
    if (dataBlockEncodingType != null) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = IndexBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException(
          "Invalid data block encoding type in file info: " + dataBlockEncodingStr, ex);
      }
    }

    if (encoding == IndexBlockEncoding.NONE) {
      return NoOpIndexBlockEncoder.INSTANCE;
    }
    return new HFileIndexBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
    writer.appendFileInfo(INDEX_BLOCK_ENCODING, indexBlockEncoding.getNameInBytes());
  }

  @Override
  public IndexBlockEncoding getIndexBlockEncoding() {
    return indexBlockEncoding;
  }

  @Override
  public void encode(BlockIndexChunk blockIndexChunk, boolean rootIndexBlock, DataOutput out)
    throws IOException {
    // TODO
  }

  @Override
  public EncodedSeeker createSeeker() {
    return null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(indexBlockEncoding=" + indexBlockEncoding + ")";
  }
}
