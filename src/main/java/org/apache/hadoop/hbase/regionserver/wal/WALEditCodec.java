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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.Decoder;
import org.apache.hadoop.hbase.codec.Encoder;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.regionserver.wal.KeyValueCompression.CompressedKvEncoder;

public class WALEditCodec implements Codec {
  private CompressionContext compression;

  public WALEditCodec(CompressionContext compression) {
    this.compression = compression;
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
}