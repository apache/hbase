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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.Decoder;
import org.apache.hadoop.hbase.codec.Encoder;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.regionserver.wal.KeyValueCompression.CompressedKvEncoder;

public class WALEditCodec implements Codec {
  /** Configuration key for a custom class to use when serializing the WALEdits to the HLog */
  public static final String WAL_EDIT_CODEC_CLASS_KEY = "hbase.regionserver.wal.codec";

  private CompressionContext compression;

  /**
   * Nullary Constructor - all subclass must support this to load from configuration. Setup can be
   * completed in the {@link #init} method.
   * <p>
   * This implementation defaults to having no compression on the resulting {@link Encoder}/
   * {@link Decoder}, though it can be added via {@link #setCompression(CompressionContext)}
   */
  public WALEditCodec() {
  }

  /**
   * Initialize <tt>this</tt> - called exactly once after the object is instantiated and before any
   * other method in this class. By default, does nothing.
   * @param conf {@link Configuration} from which to configure <tt>this</tt>
   */
  public void init(Configuration conf) {
  }

  public void setCompression(CompressionContext compression) {
    this.compression = compression;
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return
        (compression == null) ? new KeyValueCodec.KeyValueDecoder((DataInputStream) is)
            : new KeyValueCompression.CompressedKvDecoder((DataInputStream) is, compression);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return
        (compression == null) ? new KeyValueCodec.KeyValueEncoder((DataOutputStream) os)
        : new CompressedKvEncoder((DataOutputStream) os, compression);
  }

  /**
   * Create and setup a {@link WALEditCodec} from the {@link Configuration}, if one has been
   * specified. Fully prepares the codec for use in serialization.
   * @param conf {@link Configuration} to read for the user-specified codec. If none is specified,
   *          uses a {@link WALEditCodec}.
   * @param compressionContext compression to setup on the codec.
   * @return a {@link WALEditCodec} ready for use.
   * @throws IOException if the codec cannot be created
   */
  public static WALEditCodec create(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    Class<? extends WALEditCodec> codecClazz = conf.getClass(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY,
      WALEditCodec.class, WALEditCodec.class);
    try {
      WALEditCodec codec = codecClazz.newInstance();
      codec.init(conf);
      codec.setCompression(compressionContext);
      return codec;
    } catch (InstantiationException e) {
      throw new IOException("Couldn't instantiate the configured WALEditCodec!", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Couldn't instantiate the configured WALEditCodec!", e);
    }
  }
}