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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.KeyValue;

import java.util.Random;

/**
 * These helper methods generate random byte[]'s data for KeyValues
 */
public class RandomKeyValueUtil {
  public static final String COLUMN_FAMILY_NAME = "_-myColumnFamily-_";
  private static final int MIN_ROW_OR_QUALIFIER_LENGTH = 64;
  private static final int MAX_ROW_OR_QUALIFIER_LENGTH = 128;

  public static final char randomReadableChar(Random rand) {
    int i = rand.nextInt(26 * 2 + 10 + 1);
    if (i < 26)
      return (char) ('A' + i);
    i -= 26;

    if (i < 26)
      return (char) ('a' + i);
    i -= 26;

    if (i < 10)
      return (char) ('0' + i);
    i -= 10;

    assert i == 0;
    return '_';
  }

  public static KeyValue randomKeyValue(Random rand) {
    return new KeyValue(randomRowOrQualifier(rand),
        COLUMN_FAMILY_NAME.getBytes(), randomRowOrQualifier(rand),
        randomValue(rand));
  }

  public static byte[] randomRowOrQualifier(Random rand) {
    StringBuilder field = new StringBuilder();
    int fieldLen = MIN_ROW_OR_QUALIFIER_LENGTH
        + rand.nextInt(MAX_ROW_OR_QUALIFIER_LENGTH
        - MIN_ROW_OR_QUALIFIER_LENGTH + 1);
    for (int i = 0; i < fieldLen; ++i)
      field.append(randomReadableChar(rand));
    return field.toString().getBytes();
  }

  public static byte[] randomValue(Random rand) {
    StringBuilder v = new StringBuilder();
    for (int j = 0; j < 1 + rand.nextInt(2000); ++j) {
      v.append((char) (32 + rand.nextInt(95)));
    }

    byte[] valueBytes = v.toString().getBytes();
    return valueBytes;
  }

  /**
   * Generates a random key that is guaranteed to increase as the given index i
   * increases. The result consists of a prefix, which is a deterministic
   * increasing function of i, and a random suffix.
   *
   * @param rand
   *          random number generator to use
   * @param i
   * @return
   */
  public static byte[] randomOrderedKey(Random rand, int i) {
    StringBuilder k = new StringBuilder();

    // The fixed-length lexicographically increasing part of the key.
    for (int bitIndex = 31; bitIndex >= 0; --bitIndex) {
      if ((i & (1 << bitIndex)) == 0)
        k.append("a");
      else
        k.append("b");
    }

    // A random-length random suffix of the key.
    for (int j = 0; j < rand.nextInt(50); ++j)
      k.append(randomReadableChar(rand));

    byte[] keyBytes = k.toString().getBytes();
    return keyBytes;
  }

  public static byte[] randomOrderedFixedLengthKey(Random rand, int i, int suffixLength) {
    StringBuilder k = new StringBuilder();

    // The fixed-length lexicographically increasing part of the key.
    for (int bitIndex = 31; bitIndex >= 0; --bitIndex) {
      if ((i & (1 << bitIndex)) == 0)
        k.append("a");
      else
        k.append("b");
    }

    // A random suffix of the key.
    for (int j = 0; j < suffixLength; ++j)
      k.append(randomReadableChar(rand));

    byte[] keyBytes = k.toString().getBytes();
    return keyBytes;
  }

  public static byte[] randomFixedLengthValue(Random rand, int valueLength) {
    StringBuilder v = new StringBuilder();
    for (int j = 0; j < valueLength; ++j) {
      v.append((char) (32 + rand.nextInt(95)));
    }

    byte[] valueBytes = v.toString().getBytes();
    return valueBytes;
  }
}
