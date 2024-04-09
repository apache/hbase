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
package org.apache.hadoop.hbase.io.compress.xz;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.CompressionUtil;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop lzma codec implemented with XZ for Java.
 * <p>
 * Deprecated. See HBASE-28506.
 */
@InterfaceAudience.Private
@Deprecated
public class LzmaCodec implements Configurable, CompressionCodec {

  public static final String LZMA_LEVEL_KEY = "hbase.io.compress.lzma.level";
  public static final int LZMA_LEVEL_DEFAULT = 6;
  public static final String LZMA_BUFFERSIZE_KEY = "hbase.io.compress.lzma.buffersize";
  public static final int LZMA_BUFFERSIZE_DEFAULT = 256 * 1024;

  private Configuration conf;
  private int bufferSize;
  private int level;

  public LzmaCodec() {
    conf = new Configuration();
    bufferSize = getBufferSize(conf);
    level = getLevel(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.bufferSize = getBufferSize(conf);
    this.level = getLevel(conf);
  }

  @Override
  public Compressor createCompressor() {
    return new LzmaCompressor(level, bufferSize);
  }

  @Override
  public Decompressor createDecompressor() {
    return new LzmaDecompressor(bufferSize);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor d)
    throws IOException {
    return new BlockDecompressorStream(in, d, bufferSize);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return createOutputStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor c)
    throws IOException {
    return new BlockCompressorStream(out, c, bufferSize,
      CompressionUtil.compressionOverhead(bufferSize));
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return LzmaCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return LzmaDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".lzma";
  }

  // Package private

  static int getLevel(Configuration conf) {
    return conf.getInt(LZMA_LEVEL_KEY, LZMA_LEVEL_DEFAULT);
  }

  static int getBufferSize(Configuration conf) {
    return conf.getInt(LZMA_BUFFERSIZE_KEY, LZMA_BUFFERSIZE_DEFAULT);
  }

}
