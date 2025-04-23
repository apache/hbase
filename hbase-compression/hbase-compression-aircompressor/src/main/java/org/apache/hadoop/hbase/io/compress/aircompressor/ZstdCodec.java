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
package org.apache.hadoop.hbase.io.compress.aircompressor;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
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
 * Hadoop codec implementation for Zstandard, implemented with aircompressor.
 * <p>
 * Unlike the other codecs this one should be considered as under development and unstable (as in
 * changing), reflecting the status of aircompressor's zstandard implementation.
 * <p>
 * NOTE: This codec is NOT data format compatible with the Hadoop native zstandard codec. There are
 * issues with both framing and limitations of the aircompressor zstandard compressor. This codec
 * can be used as an alternative to the native codec, if the native codec cannot be made available
 * and/or an eventual migration will never be necessary (i.e. this codec's performance meets
 * anticipated requirements). Once you begin using this alternative you will be locked into it.
 */
@InterfaceAudience.Private
public class ZstdCodec implements Configurable, CompressionCodec {

  public static final String ZSTD_BUFFER_SIZE_KEY = "hbase.io.compress.zstd.buffersize";
  public static final int ZSTD_BUFFER_SIZE_DEFAULT = 256 * 1024;

  private Configuration conf;
  private int bufferSize;

  public ZstdCodec() {
    conf = new Configuration();
    bufferSize = getBufferSize(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.bufferSize = getBufferSize(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Compressor createCompressor() {
    return new HadoopZstdCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new HadoopZstdDecompressor();
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
    return HadoopZstdCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return HadoopZstdDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".zst";
  }

  @InterfaceAudience.Private
  public class HadoopZstdCompressor extends HadoopCompressor<ZstdCompressor> {

    HadoopZstdCompressor(ZstdCompressor compressor) {
      super(compressor, ZstdCodec.getBufferSize(conf));
    }

    HadoopZstdCompressor() {
      this(new ZstdCompressor());
    }

    @Override
    int getLevel(Configuration conf) {
      return 0;
    }

    @Override
    int getBufferSize(Configuration conf) {
      return ZstdCodec.getBufferSize(conf);
    }

  }

  @InterfaceAudience.Private
  public class HadoopZstdDecompressor extends HadoopDecompressor<ZstdDecompressor> {

    HadoopZstdDecompressor(ZstdDecompressor decompressor) {
      super(decompressor, getBufferSize(conf));
    }

    HadoopZstdDecompressor() {
      this(new ZstdDecompressor());
    }

  }

  // Package private

  static int getBufferSize(Configuration conf) {
    int size = conf.getInt(ZSTD_BUFFER_SIZE_KEY,
      conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT));
    return size > 0 ? size : ZSTD_BUFFER_SIZE_DEFAULT;
  }

}
