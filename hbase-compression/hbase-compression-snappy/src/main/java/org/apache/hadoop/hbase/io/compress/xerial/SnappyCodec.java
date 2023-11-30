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
package org.apache.hadoop.hbase.io.compress.xerial;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Hadoop Snappy codec implemented with Xerial Snappy.
 * <p>
 * This is data format compatible with Hadoop's native snappy codec.
 */
@InterfaceAudience.Private
public class SnappyCodec implements Configurable, CompressionCodec {

  public static final String SNAPPY_BUFFER_SIZE_KEY = "hbase.io.compress.snappy.buffersize";

  private static final Logger LOG = LoggerFactory.getLogger(SnappyCodec.class);
  private Configuration conf;
  private int bufferSize;
  private static boolean loaded = false;
  private static Throwable loadError;

  static {
    try {
      Snappy.getNativeLibraryVersion();
      loaded = true;
    } catch (Throwable t) {
      loadError = t;
      LOG.error("The Snappy native libraries could not be loaded", t);
    }
  }

  /** Return true if the native shared libraries were loaded; false otherwise. */
  public static boolean isLoaded() {
    return loaded;
  }

  public SnappyCodec() {
    if (!isLoaded()) {
      throw new RuntimeException("Snappy codec could not be loaded", loadError);
    }
    conf = new Configuration();
    bufferSize = getBufferSize(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.bufferSize = getBufferSize(conf);
  }

  @Override
  public Compressor createCompressor() {
    return new SnappyCompressor(bufferSize);
  }

  @Override
  public Decompressor createDecompressor() {
    return new SnappyDecompressor(bufferSize);
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
      Snappy.maxCompressedLength(bufferSize) - bufferSize); // overhead only
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return SnappyCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return SnappyDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".snappy";
  }

  // Package private

  static int getBufferSize(Configuration conf) {
    return conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
      conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT));
  }

}
