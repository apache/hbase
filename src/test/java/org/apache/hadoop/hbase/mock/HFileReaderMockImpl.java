/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mock;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.io.RawComparator;

public class HFileReaderMockImpl implements Reader {

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public HFileBlock readBlock(long offset, long onDiskBlockSize,
      boolean cacheBlock, boolean isCompaction, boolean cacheOnPreload,
      BlockType expectedBlockType, DataBlockEncoding expectedDataBlockEncoding,
      KeyValueContext kvContext) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTableName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SchemaMetrics getSchemaMetrics() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getColumnFamilyName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RawComparator<byte[]> getComparator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HFileScanner getScanner(boolean cacheBlocks, boolean isCompaction) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HFileScanner getScanner(boolean cacheBlocks, boolean isCompaction,
      boolean preloadBlocks) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ByteBuffer getMetaBlock(String metaBlockName, boolean cacheBlock)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], byte[]> loadFileInfo() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getLastKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] midkey() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long length() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getEntries() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public byte[] getFirstKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long indexSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public byte[] getFirstRowKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getLastRowKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockIndexReader getDataBlockIndexReader() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HFileScanner getScanner(boolean cacheBlocks) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Algorithm getCompressionAlgorithm() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DataInput getDeleteColumnBloomFilterMetadata() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DataInput getRowKeyPrefixBloomFilterMetadata() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Path getPath() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close(boolean evictOnClose) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close(boolean evictL1OnClose, boolean evictL2OnClose)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public DataBlockEncoding getEncodingOnDisk() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toShortString() {
    // TODO Auto-generated method stub
    return null;
  }

}
