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
package org.apache.hadoop.hbase.regionserver.idx.support;

/**
 * Bits utility class.
 * TODO consider delete this class since the Java SE implemetations are as good.
 */
public final class Bits {

  /**
   * De bruijn 64 number to implement set bit index extraction.
   */
  private static final long DEBRUIJN64 = 0x022fdd63cc95386dl;

  private static final int[] DEBRUIJN64_TABLE = new int[]{
          0, 1, 2, 53, 3, 7, 54, 27,
          4, 38, 41, 8, 34, 55, 48, 28,
          62, 5, 39, 46, 44, 42, 22, 9,
          24, 35, 59, 56, 49, 18, 29, 11,
          63, 52, 6, 26, 37, 40, 33, 47,
          61, 45, 43, 21, 23, 58, 17, 10,
          51, 25, 36, 32, 60, 20, 57, 16,
          50, 31, 19, 15, 30, 14, 13, 12,
  };

  private static final int DEBRUIJN64_SHIFT = 58;

  private Bits() {
    //utility class private constructor
  }


  /**
   * Finds the index of the lowest set bit.
   *
   * @param word the word to check. Should not be zero.
   * @return the index of the lowest set bit.
   */
  public static int lowestSetBitIndex(long word) {
    assert word != 0;
    word &= -word;
    return DEBRUIJN64_TABLE[(int) ((word * DEBRUIJN64) >>> DEBRUIJN64_SHIFT)];
  }

  /**
   * Finds the index of the highest set bit.
   *
   * @param word the word to check. Should not be zero.
   * @return the index of the highest set bit.
   */
  public static int highestSetBitIndex(long word) {
    word |= word >> 1;
    word |= word >> 2;
    word |= word >> 4;
    word |= word >> 8;
    word |= word >> 16;
    word |= word >> 32;
    word = word + 1 >>> 1;
    return DEBRUIJN64_TABLE[(int) ((word * DEBRUIJN64) >>> DEBRUIJN64_SHIFT)];
  }
}
