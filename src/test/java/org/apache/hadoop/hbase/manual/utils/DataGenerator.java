/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.manual.utils;

import java.util.Random;

public class DataGenerator {
  static Random random_ = new Random();
  /* one byte fill pattern */
  public static final String fill1B_     = "-";
  /* 64 byte fill pattern */
  public static final String fill64B_    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789  ";
  /* alternate 64 byte fill pattern */
  public static final String fill64BAlt_ = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789+-";
  /* 1K fill pattern */
  public static final String fill1K_     = fill64BAlt_+fill64BAlt_+fill64BAlt_+fill64BAlt_+
                                           fill64BAlt_+fill64BAlt_+fill64BAlt_+fill64BAlt_+
                                           fill64BAlt_+fill64BAlt_+fill64BAlt_+fill64BAlt_+
                                           fill64BAlt_+fill64BAlt_+fill64BAlt_+fill64BAlt_;

  int minDataSize_ = 0;
  int maxDataSize_ = 0;

  static public String paddedKey(long key) {
      // left-pad key with zeroes to 10 decimal places.
      String paddedKey = String.format("%010d", key);

      // flip the key to randomize
      return (new StringBuffer(paddedKey)).reverse().toString();
  }

  public DataGenerator(int minDataSize, int maxDataSize) {
    minDataSize_ = minDataSize;
    maxDataSize_ = maxDataSize;
  }

  public byte[] getDataInSize(long key) {
    int dataSize = minDataSize_ + random_.nextInt(Math.abs(maxDataSize_ - minDataSize_));
    StringBuilder sb = new StringBuilder();

    // write the key first
    int sizeLeft = dataSize;
    String keyAsString = DataGenerator.paddedKey(key);
    sb.append(keyAsString);
    sizeLeft -= keyAsString.length();

    for(int i = 0; i < sizeLeft/1024; ++i)
    {
      sb.append(fill1K_);
    }
    sizeLeft = sizeLeft % 1024;
    for(int i = 0; i < sizeLeft/64; ++i)
    {
      sb.append(fill64B_);
    }
    sizeLeft = sizeLeft % 64;
    for(int i = 0; i < dataSize%64; ++i)
    {
      sb.append(fill1B_);
    }

    return sb.toString().getBytes();
  }

  public static boolean verify(String rowKey, String actionId, String data) {
    if(!data.startsWith(rowKey)) {
      return false;
    }
    return true;
  }
}
