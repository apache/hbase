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
package org.apache.hadoop.hbase.io.compress.zstd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop ZStandard codec implemented with zstd-jni.
 * <p>
 * This is data format compatible with Hadoop's native ZStandard codec.
 */
@InterfaceAudience.Private
public class ZstdCodec implements Configurable, CompressionCodec {

  public static final String ZSTD_LEVEL_KEY = "hbase.io.compress.zstd.level";
  public static final String ZSTD_BUFFER_SIZE_KEY = "hbase.io.compress.zstd.buffersize";

  private Configuration conf;

  public ZstdCodec() {
    conf = new Configuration();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Compressor createCompressor() {
    return new ZstdCompressor(getLevel(conf), getBufferSize(conf));
  }

  @Override
  public Decompressor createDecompressor() {
    return new ZstdDecompressor(getBufferSize(conf));
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor d)
      throws IOException {
    return new BlockDecompressorStream(in, d, getBufferSize(conf));
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return createOutputStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor c)
      throws IOException {
    int bufferSize = getBufferSize(conf);
    int compressionOverhead = (bufferSize / 6) + 32;
    return new BlockCompressorStream(out, c, bufferSize, compressionOverhead);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return ZstdCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return ZstdDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".zst";
  }

  // Package private

  static int getLevel(Configuration conf) {
    return conf.getInt(ZSTD_LEVEL_KEY,
      conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT));
  }

  static int getBufferSize(Configuration conf) {
    int size = conf.getInt(ZSTD_BUFFER_SIZE_KEY,
      conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT));
    return size > 0 ? size : 256 * 1024; // Don't change this default
  }

}
