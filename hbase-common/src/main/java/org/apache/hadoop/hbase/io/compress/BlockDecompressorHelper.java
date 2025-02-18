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
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BlockDecompressorHelper {

  public interface RawDecompressor {
    int decompress(ByteBuff output, ByteBuff input, int inputLen) throws IOException;
  }

  /**
   * Helper to decompress a ByteBuff that was created by a
   * {@link org.apache.hadoop.io.compress.BlockCompressorStream}, or is at least in the same format.
   * Parses the binary format and delegates actual decompression work to the provided
   * {@link RawDecompressor}.
   */
  public static int decompress(ByteBuff output, ByteBuff input, int inputSize,
    RawDecompressor rawDecompressor) throws IOException {
    int totalDecompressedBytes = 0;
    int compressedBytesConsumed = 0;

    while (compressedBytesConsumed < inputSize) {
      int decompressedBlockSize = rawReadInt(input);
      compressedBytesConsumed += 4;
      int decompressedBytesInBlock = 0;

      while (decompressedBytesInBlock < decompressedBlockSize) {
        int compressedChunkSize = rawReadInt(input);
        compressedBytesConsumed += 4;
        int n = rawDecompressor.decompress(output, input, compressedChunkSize);
        compressedBytesConsumed += compressedChunkSize;
        decompressedBytesInBlock += n;
        totalDecompressedBytes += n;
      }
    }
    return totalDecompressedBytes;
  }

  private static int rawReadInt(ByteBuff input) {
    int b1 = Byte.toUnsignedInt(input.get());
    int b2 = Byte.toUnsignedInt(input.get());
    int b3 = Byte.toUnsignedInt(input.get());
    int b4 = Byte.toUnsignedInt(input.get());
    return ((b1 << 24) + (b2 << 16) + (b3 << 8) + b4);
  }

}
