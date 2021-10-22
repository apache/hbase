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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop ZStandard codec implemented with zstd-jni.
 * <p>
 * This is data format compatible with Hadoop's native ZStandard codec.
 */
@InterfaceAudience.Private
public class ZstdCodec implements Configurable, CompressionCodec {

  public static final String ZSTD_LEVEL_KEY = "hbase.io.compress.zstd.level";
  public static final String ZSTD_BUFFER_SIZE_KEY = "hbase.io.compress.zstd.buffersize";
  public static final String ZSTD_DICTIONARY_KEY = "hbase.io.compress.zstd.dictionary";
  public static final String ZSTD_DICTIONARY_MAX_SIZE_KEY =
    "hbase.io.compress.zstd.dictionary.max.size";
  public static final int DEFAULT_ZSTD_DICTIONARY_MAX_SIZE = 10 * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(ZstdCodec.class);
  private static volatile LoadingCache<String, byte[]> DICTIONARY_CACHE;
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
    return new ZstdCompressor(getLevel(conf), getBufferSize(conf), getDictionary(conf));
  }

  @Override
  public Decompressor createDecompressor() {
    return new ZstdDecompressor(getBufferSize(conf), getDictionary(conf));
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

  static byte[] getDictionary(final Configuration conf) {
    // Get the dictionary path, if set
    final String s = conf.get(ZSTD_DICTIONARY_KEY);
    if (s == null || s.isEmpty()) {
      return null;
    }

    // Create the dictionary loading cache if we haven't already
    if (DICTIONARY_CACHE == null) {
      synchronized (ZstdCodec.class) {
        if (DICTIONARY_CACHE == null) {
          DICTIONARY_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(
              new CacheLoader<String, byte[]>() {
                public byte[] load(String s) throws Exception {
                  final Path path = new Path(s);
                  final FileSystem fs = FileSystem.get(path.toUri(), conf);
                  final FileStatus stat = fs.getFileStatus(path);
                  if (!stat.isFile()) {
                    throw new IllegalArgumentException(s + " is not a file");
                  }
                  final int limit = conf.getInt(ZSTD_DICTIONARY_MAX_SIZE_KEY,
                    DEFAULT_ZSTD_DICTIONARY_MAX_SIZE);
                  if (stat.getLen() > limit) {
                    throw new IllegalArgumentException("Dictionary " + s + " is too large" +
                      ", size=" + stat.getLen() + ", limit=" + limit);
                  }
                  final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                  final byte[] buffer = new byte[8192];
                  try (final FSDataInputStream in = fs.open(path)) {
                    int n;
                    do {
                      n = in.read(buffer);
                      if (n > 0) {
                        baos.write(buffer, 0, n);
                         }
                    } while (n > 0);
                  }
                  LOG.info("Loaded {} from {} (size {})", ZSTD_DICTIONARY_KEY, s, stat.getLen());
                  return baos.toByteArray();
                }
            });
        }
      }
    }

    // Get or load the dictionary for the given path
    try {
      return DICTIONARY_CACHE.get(s);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

}
