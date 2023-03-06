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
package org.apache.hadoop.hbase.io.compress.brotli;

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
 * Hadoop brotli codec implemented with Brotli4j
 */
@InterfaceAudience.Private
public class BrotliCodec implements Configurable, CompressionCodec {

  public static final String BROTLI_LEVEL_KEY = "hbase.io.compress.brotli.level";
  // Our default is 6, based on https://blog.cloudflare.com/results-experimenting-brotli/
  public static final int BROTLI_LEVEL_DEFAULT = 6; // [0,11] or -1
  public static final String BROTLI_WINDOW_KEY = "hbase.io.compress.brotli.window";
  public static final int BROTLI_WINDOW_DEFAULT = -1; // [10-24] or -1
  public static final String BROTLI_BUFFERSIZE_KEY = "hbase.io.compress.brotli.buffersize";
  public static final int BROTLI_BUFFERSIZE_DEFAULT = 256 * 1024;

  private Configuration conf;
  private int bufferSize;
  private int level;
  private int window;

  public BrotliCodec() {
    conf = new Configuration();
    bufferSize = getBufferSize(conf);
    level = getLevel(conf);
    window = getWindow(conf);
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
    this.window = getWindow(conf);
  }

  @Override
  public Compressor createCompressor() {
    return new BrotliCompressor(level, window, bufferSize);
  }

  @Override
  public Decompressor createDecompressor() {
    return new BrotliDecompressor(bufferSize);
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
    return BrotliCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return BrotliDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".br";
  }

  // Package private

  static int getLevel(Configuration conf) {
    return conf.getInt(BROTLI_LEVEL_KEY, BROTLI_LEVEL_DEFAULT);
  }

  static int getWindow(Configuration conf) {
    return conf.getInt(BROTLI_WINDOW_KEY, BROTLI_WINDOW_DEFAULT);
  }

  static int getBufferSize(Configuration conf) {
    return conf.getInt(BROTLI_BUFFERSIZE_KEY, BROTLI_BUFFERSIZE_DEFAULT);
  }

}
