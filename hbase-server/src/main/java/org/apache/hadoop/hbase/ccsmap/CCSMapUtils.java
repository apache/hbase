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

package org.apache.hadoop.hbase.ccsmap;

import java.util.Arrays;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public final class CCSMapUtils {

  public static final int DEFAULT_PAGE_SIZE = 2 * 1024 * 1024; // 2MB
  public static final int DEFAULT_PAGES = 4096;
  public static final int DEFAULT_HEAPKV_PAGE_SIZE = 1024;
  public static final int DEFAULT_HEAPKV_PAGES = 1024;

  public static int getShiftFromX(int x) {
    if ((x & ~x) != 0) {
      throw new IllegalArgumentException("Page size must be power of 2");
    }
    int ret = 0;
    while ((x >>= 1) > 0) ret++;
    return ret;
  }

  public static void clear(byte[] data, int localoffset, int len) {
    Arrays.fill(data, localoffset, localoffset + len, (byte)0);
  }

  public static void writeSeqId(byte[] data, int offset, int length, long seqId) {
    int pos = offset + lengthWithoutMemstore(length);
    data[pos] = (byte) (seqId >> 56);
    data[pos + 1] = (byte) (seqId >> 48);
    data[pos + 2] = (byte) (seqId >> 40);
    data[pos + 3] = (byte) (seqId >> 32);
    data[pos + 4] = (byte) (seqId >> 24);
    data[pos + 5] = (byte) (seqId >> 16);
    data[pos + 6] = (byte) (seqId >> 8);
    data[pos + 7] = (byte) (seqId);
  }

  public static long readSeqId(byte[] data, int offset, int len) {
    int mo = offset + lengthWithoutMemstore(len);
    return  (data[mo] & 0xFFL) << 56 | (data[mo + 1] & 0xFFL) << 48
      | (data[mo + 2] & 0xFFL) << 40 | (data[mo + 3] & 0xFFL) << 32
      | (data[mo + 4] & 0xFFL) << 24 | (data[mo + 5] & 0xFFL) << 16
      | (data[mo + 6] & 0xFFL) << 8  | (data[mo + 7] & 0xFFL);
  }

  public static int lengthWithoutMemstore(int len) {
    return len - Bytes.SIZEOF_LONG;
  }

}
