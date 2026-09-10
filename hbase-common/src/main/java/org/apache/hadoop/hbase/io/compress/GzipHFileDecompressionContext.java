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
package org.apache.hadoop.hbase.io.compress;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Holds HFile-level settings used by GzipByteBuffDecompressor. It's expensive to pull these from a
 * Configuration object every time we decompress a block, so pull them upon opening an HFile, and
 * reuse them in every block that gets decompressed.
 */
@InterfaceAudience.Private
public final class GzipHFileDecompressionContext extends Compression.HFileDecompressionContext {

  public static final long FIXED_OVERHEAD =
    ClassSize.estimateBase(GzipHFileDecompressionContext.class, false);

  public static final String ALLOW_BYTE_BUFF_DECOMPRESSION_KEY =
    "hbase.io.compress.gz.allowByteBuffDecompression";

  private final boolean allowByteBuffDecompression;

  private GzipHFileDecompressionContext(boolean allowByteBuffDecompression) {
    this.allowByteBuffDecompression = allowByteBuffDecompression;
  }

  public boolean isAllowByteBuffDecompression() {
    return allowByteBuffDecompression;
  }

  public static GzipHFileDecompressionContext fromConfiguration(Configuration conf) {
    return new GzipHFileDecompressionContext(
      conf.getBoolean(ALLOW_BYTE_BUFF_DECOMPRESSION_KEY, true));
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public long heapSize() {
    return FIXED_OVERHEAD;
  }

  @Override
  public String toString() {
    return "GzipHFileDecompressionContext{allowByteBuffDecompression=" + allowByteBuffDecompression
      + '}';
  }
}
