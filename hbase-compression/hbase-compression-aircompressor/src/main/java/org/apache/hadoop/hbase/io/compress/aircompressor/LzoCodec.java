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

import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
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
 * Hadoop Lzo codec implemented with aircompressor.
 * <p>
 * This is data format compatible with Hadoop's native lzo codec.
 */
@InterfaceAudience.Private
public class LzoCodec implements Configurable, CompressionCodec {

  public static final String LZO_BUFFER_SIZE_KEY = "hbase.io.compress.lzo.buffersize";

  private Configuration conf;
  private int bufferSize;

  public LzoCodec() {
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
    return new HadoopLzoCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new HadoopLzoDecompressor();
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
    return HadoopLzoCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return HadoopLzoDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".lzo";
  }

  @InterfaceAudience.Private
  public class HadoopLzoCompressor extends HadoopCompressor<LzoCompressor> {

    HadoopLzoCompressor(LzoCompressor compressor) {
      super(compressor, LzoCodec.getBufferSize(conf));
    }

    HadoopLzoCompressor() {
      this(new LzoCompressor());
    }

    @Override
    int getLevel(Configuration conf) {
      return 0;
    }

    @Override
    int getBufferSize(Configuration conf) {
      return LzoCodec.getBufferSize(conf);
    }

  }

  @InterfaceAudience.Private
  public class HadoopLzoDecompressor extends HadoopDecompressor<LzoDecompressor> {

    HadoopLzoDecompressor(LzoDecompressor decompressor) {
      super(decompressor, getBufferSize(conf));
    }

    HadoopLzoDecompressor() {
      this(new LzoDecompressor());
    }

  }

  // Package private

  static int getBufferSize(Configuration conf) {
    return conf.getInt(LZO_BUFFER_SIZE_KEY,
      conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT));
  }

}
