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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.hfile.HFileContext;

/**
 * Does not perform any kind of encoding/decoding.
 */
@InterfaceAudience.Private
public class NoOpDataBlockEncoder implements HFileDataBlockEncoder {

  public static final NoOpDataBlockEncoder INSTANCE =
      new NoOpDataBlockEncoder();

  /** Cannot be instantiated. Use {@link #INSTANCE} instead. */
  private NoOpDataBlockEncoder() {
  }

  @Override
  public HFileBlock diskToCacheFormat(HFileBlock block, boolean isCompaction) {
    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      throw new IllegalStateException("Unexpected encoded block");
    }
    return block;
  }

  @Override
  public void beforeWriteToDisk(ByteBuffer in,
      HFileBlockEncodingContext encodeCtx, BlockType blockType)
      throws IOException {
    if (!(encodeCtx.getClass().getName().equals(
        HFileBlockDefaultEncodingContext.class.getName()))) {
      throw new IOException (this.getClass().getName() + " only accepts " +
          HFileBlockDefaultEncodingContext.class.getName() + ".");
    }

    HFileBlockDefaultEncodingContext defaultContext =
        (HFileBlockDefaultEncodingContext) encodeCtx;
    defaultContext.compressAfterEncodingWithBlockType(in.array(), blockType);
  }

  @Override
  public boolean useEncodedScanner(boolean isCompaction) {
    return false;
  }

  @Override
  public void saveMetadata(HFile.Writer writer) {
  }

  @Override
  public DataBlockEncoding getEncodingOnDisk() {
    return DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEncodingInCache() {
    return DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return DataBlockEncoding.NONE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public HFileBlockEncodingContext newOnDiskDataBlockEncodingContext(
      byte[] dummyHeader, HFileContext meta) {
    return new HFileBlockDefaultEncodingContext(null, dummyHeader, meta);
  }

  @Override
  public HFileBlockDecodingContext newOnDiskDataBlockDecodingContext(HFileContext meta) {
    return new HFileBlockDefaultDecodingContext(meta);
  }

}
