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
package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public enum SizeBucket {
  KILOBYTES_1(0, 1 * 1024, "[0, 1)"),
  KILOBYTES_2(1 * 1024, 2 * 1024, "[1, 2)"),
  KILOBYTES_4(2 * 1024, 4 * 1024, "[2, 4)"),
  KILOBYTES_8(4 * 1024, 8 * 1024, "[4, 8)"),
  KILOBYTES_16(8 * 1024, 16 * 1024, "[8, 16)"),
  KILOBYTES_32(16 * 1024, 32 * 1024, "[16, 32)"),
  KILOBYTES_64(32 * 1024, 64 * 1024, "[32, 64)"),
  KILOBYTES_128(64 * 1024, 128 * 1024, "[64, 128)"),
  KILOBYTES_256(128 * 1024, 256 * 1024, "[128, 256)"),
  KILOBYTES_512(256 * 1024, 512 * 1024, "[256, 512)"),
  KILOBYTES_MAX(512 * 1024, Long.MAX_VALUE, "[512, inf)");

  private final long minBytes;
  private final long maxBytes;
  private final String bucket;

  SizeBucket(long minBytes, long maxBytes, String bucket) {
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
    this.bucket = bucket;
  }

  public long minBytes() {
    return minBytes;
  }

  public long maxBytes() {
    return maxBytes;
  }

  public String bucket() {
    return bucket;
  }
}
