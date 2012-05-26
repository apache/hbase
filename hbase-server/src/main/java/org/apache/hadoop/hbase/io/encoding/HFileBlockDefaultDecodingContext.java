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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;

/**
 * A default implementation of {@link HFileBlockDecodingContext}. It assumes the
 * block data section is compressed as a whole.
 *
 * @see HFileBlockDefaultEncodingContext for the default compression context
 *
 */
public class HFileBlockDefaultDecodingContext implements
    HFileBlockDecodingContext {

  private final Compression.Algorithm compressAlgo;

  public HFileBlockDefaultDecodingContext(
      Compression.Algorithm compressAlgo) {
    this.compressAlgo = compressAlgo;
  }

  @Override
  public void prepareDecoding(HFileBlock block,
      byte[] onDiskBlock, int offset) throws IOException {
    DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(
            onDiskBlock, offset,
            block.getOnDiskSizeWithoutHeader()));

    ByteBuffer buffer = block.getBufferWithoutHeader();
    Compression.decompress(buffer.array(), buffer.arrayOffset(),
        (InputStream) dis, block.getOnDiskSizeWithoutHeader(),
        block.getUncompressedSizeWithoutHeader(), compressAlgo);
  }

  @Override
  public Algorithm getCompression() {
    return compressAlgo;
  }

}
