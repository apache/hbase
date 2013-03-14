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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Does not perform any kind of encoding/decoding.
 */
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
  public Pair<ByteBuffer, BlockType> beforeWriteToDisk(
      ByteBuffer in, boolean includesMemstoreTS, byte[] dummyHeader) {
    return new Pair<ByteBuffer, BlockType>(in, BlockType.DATA);
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

}
