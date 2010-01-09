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

import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestCase;

import java.math.BigDecimal;
import java.util.Random;

/**
 * Tests for the bits algorithms.
 */
public class TestBits extends HBaseTestCase {
  private static final int PERFORMANCE_COUNT = 10000;
  private static final BigDecimal ONE_MILLION = new BigDecimal(1000000);
  private static final int WARMUP_COUNT = 1000;

  public void testLowestSetBitIndex() {
    for (long word = 1L; word < 1000000l; word++) {
      Assert.assertEquals("word=" + word, Bits.lowestSetBitIndex(word),
        Long.numberOfTrailingZeros(word));
      long highWord = Long.MAX_VALUE - (word - 1) * 17;
      Assert.assertEquals("word=" + highWord, Bits.lowestSetBitIndex(highWord),
        Long.numberOfTrailingZeros(highWord));
    }
  }

  public void testHighestSetBitIndex() {
    for (long word = 1L; word < 1000000l; word++) {
      Assert.assertEquals("word=" + word, Bits.highestSetBitIndex(word),
        63 - Long.numberOfLeadingZeros(word));
      long highWord = Long.MAX_VALUE - (word - 1) * 11;
      Assert.assertEquals("word=" + highWord, Bits.highestSetBitIndex(highWord),
        63 - Long.numberOfLeadingZeros(highWord));
    }
  }


  public void testLowestSetBitPerformance() {
    long[] randomSequence = new long[PERFORMANCE_COUNT];
    Random random = new Random();
    for (int i = 0; i < randomSequence.length; i++) {
      randomSequence[i] = random.nextLong();
    }

    for (int i = 0; i < WARMUP_COUNT; i++) {
      Long.numberOfTrailingZeros(random.nextLong());
    }

    long start = System.nanoTime();
    for (int i = 0; i < randomSequence.length; i++) {
      Long.numberOfTrailingZeros(randomSequence[i]);
    }
    long finish = System.nanoTime();
    long nanos = (finish - start) / PERFORMANCE_COUNT;
    System.out.printf("Long.numberOfTrailingZeros: %s\n",
      new BigDecimal(nanos).divide(ONE_MILLION) + ";");

    for (int i = 0; i < WARMUP_COUNT; i++) {
      Bits.lowestSetBitIndex(random.nextLong());
    }


    start = System.nanoTime();
    for (int i = 0; i < randomSequence.length; i++) {
      Bits.lowestSetBitIndex(randomSequence[i]);
    }
    finish = System.nanoTime();
    nanos = (finish - start) / PERFORMANCE_COUNT;
    System.out.printf("Bits.lowestSetBitIndex: %s\n",
      new BigDecimal(nanos).divide(ONE_MILLION) + ";");
  }

  public void testHigestSetBitPerformance() {
    long[] randomSequence = new long[PERFORMANCE_COUNT];
    Random random = new Random();
    for (int i = 0; i < randomSequence.length; i++) {
      randomSequence[i] = random.nextLong();
    }

    for (int i = 0; i < WARMUP_COUNT; i++) {
      Long.numberOfLeadingZeros(random.nextLong());
    }

    long start = System.nanoTime();
    for (int i = 0; i < randomSequence.length; i++) {
      Long.numberOfLeadingZeros(randomSequence[i]);
    }
    long finish = System.nanoTime();
    long nanos = (finish - start) / PERFORMANCE_COUNT;
    System.out.printf("Long.numberOfLeadingZeros: %s\n",
      new BigDecimal(nanos).divide(ONE_MILLION) + ";");

    for (int i = 0; i < WARMUP_COUNT; i++) {
      Bits.highestSetBitIndex(random.nextLong());
    }


    start = System.nanoTime();
    for (int i = 0; i < randomSequence.length; i++) {
      Bits.highestSetBitIndex(randomSequence[i]);
    }
    finish = System.nanoTime();
    nanos = (finish - start) / PERFORMANCE_COUNT;
    System.out.printf("Bits.highestSetBitIndex: %s\n",
      new BigDecimal(nanos).divide(ONE_MILLION) + ";");
  }

}
