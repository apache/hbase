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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class DataGenerator {
  static Random random_ = new Random();

  int minDataSize_ = 0;
  int maxDataSize_ = 0;

  static public String md5PrefixedKey(long key) {
    String stringKey = Long.toString(key);
    String md5hash = MD5Hash.getMD5AsHex(Bytes.toBytes(stringKey));

    // flip the key to randomize
    return md5hash + ":" + stringKey; 
  }

  public DataGenerator(int minDataSize, int maxDataSize) {
    minDataSize_ = minDataSize;
    maxDataSize_ = maxDataSize;
  }

  private static byte[] getDataForKeyColumn(String rowKey, String column, int dataSize) {
    // Need a different local random object since multiple threads might invoke
    // this method at the same time.
    Random random = new Random(rowKey.hashCode() + column.hashCode());
    byte[] rbytes = new byte[dataSize];
    random.nextBytes(rbytes);
    return rbytes;
  }

  public byte[] getDataInSize(long key, String column) {
    String rowKey = DataGenerator.md5PrefixedKey(key);
    int dataSize = minDataSize_ + random_.nextInt(Math.abs(maxDataSize_ - minDataSize_));
    return getDataForKeyColumn(rowKey, column, dataSize);
  }

  public static boolean verify(String rowKey, String actionId, byte[] data) {
    byte[] expectedData = getDataForKeyColumn(rowKey, actionId, data.length);
    return (Bytes.equals(expectedData, data));
  }
}
