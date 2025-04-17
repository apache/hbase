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
package org.apache.hadoop.hbase.io.compress.zstd;

import com.github.luben.zstd.ZstdDictDecompress;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Holds HFile-level settings used by ZstdByteBuffDecompressor. It's expensive to pull these from a
 * Configuration object every time we decompress a block, so pull them upon opening an HFile, and
 * reuse them in every block that gets decompressed.
 */
@InterfaceAudience.Private
public class ZstdHFileDecompressionContext extends Compression.HFileDecompressionContext {

  public static final long FIXED_OVERHEAD =
    ClassSize.estimateBase(ZstdHFileDecompressionContext.class, false);

  @Nullable
  private final ZstdDictDecompress dict;
  private final int dictId;
  // Intended to be set to false by some unit tests
  private final boolean allowByteBuffDecompression;

  private ZstdHFileDecompressionContext(@Nullable ZstdDictDecompress dict, int dictId,
    boolean allowByteBuffDecompression) {
    this.dict = dict;
    this.dictId = dictId;
    this.allowByteBuffDecompression = allowByteBuffDecompression;
  }

  @Nullable
  public ZstdDictDecompress getDict() {
    return dict;
  }

  public int getDictId() {
    return dictId;
  }

  public boolean isAllowByteBuffDecompression() {
    return allowByteBuffDecompression;
  }

  public static ZstdHFileDecompressionContext fromConfiguration(Configuration conf) {
    boolean allowByteBuffDecompression =
      conf.getBoolean("hbase.io.compress.zstd.allowByteBuffDecompression", true);
    Pair<ZstdDictDecompress, Integer> dictAndId = ZstdCodec.getDecompressDictionary(conf);
    if (dictAndId != null) {
      return new ZstdHFileDecompressionContext(dictAndId.getFirst(), dictAndId.getSecond(),
        allowByteBuffDecompression);
    } else {
      return new ZstdHFileDecompressionContext(null, 0, allowByteBuffDecompression);
    }
  }

  @Override
  public void close() throws IOException {
    if (dict != null) {
      dict.close();
    }
  }

  @Override
  public long heapSize() {
    // ZstdDictDecompress objects are cached and shared between ZstdHFileDecompressionContexts, so
    // don't include ours in our heap size.
    return FIXED_OVERHEAD;
  }

  @Override
  public String toString() {
    return "ZstdHFileDecompressionContext{dictId=" + dictId + ", allowByteBuffDecompression="
      + allowByteBuffDecompression + '}';
  }
}
