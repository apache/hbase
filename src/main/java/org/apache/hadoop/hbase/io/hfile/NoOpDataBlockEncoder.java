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

import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Does not perform any kind of encoding/decoding.
 */
public class NoOpDataBlockEncoder implements HFileDataBlockEncoder {

  @Override
  public HFileBlock afterReadFromDisk(HFileBlock block) {
    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      throw new IllegalStateException("Unexpected encoded block");
    }
    return block;
  }

  @Override
  public HFileBlock afterReadFromDiskAndPuttingInCache(HFileBlock block,
        boolean isCompaction, boolean includesMemstoreTS) {
    return block;
  }

  @Override
  public Pair<ByteBuffer, BlockType> beforeWriteToDisk(
      ByteBuffer in, boolean includesMemstoreTS) {
    return new Pair<ByteBuffer, BlockType>(in, BlockType.DATA);
  }

  @Override
  public HFileBlock beforeBlockCache(HFileBlock block,
      boolean includesMemstoreTS) {
    return block;
  }

  @Override
  public HFileBlock afterBlockCache(HFileBlock block, boolean isCompaction,
      boolean includesMemstoreTS) {
    return block;
  }

  @Override
  public boolean useEncodedScanner(boolean isCompaction) {
    return false;
  }

  @Override
  public void saveMetadata(StoreFile.Writer storeFileWriter) {
  }
}
