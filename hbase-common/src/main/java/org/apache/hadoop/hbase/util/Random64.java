/**
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

package org.apache.hadoop.hbase.util;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 *
 * An instance of this class is used to generate a stream of
 * pseudorandom numbers. The class uses a 64-bit seed, which is
 * modified using a linear congruential formula.
 *
 * see https://en.wikipedia.org/wiki/Linear_congruential_generator
 */
@InterfaceAudience.Private
public class Random64 {

  private static final long multiplier = 6364136223846793005L;
  private static final long addend = 1442695040888963407L;

  private static final AtomicLong seedUniquifier
        = new AtomicLong(8682522807148012L);

  private long seed;

  /**
   * Copy from {@link Random#seedUniquifier()}
   */
  private static long seedUniquifier() {
    for (; ; ) {
      long current = seedUniquifier.get();
      long next = current * 181783497276652981L;
      if (seedUniquifier.compareAndSet(current, next)) {
        return next;
      }
    }
  }

  public Random64() {
    this(seedUniquifier() ^ System.nanoTime());
  }

  public Random64(long seed) {
    this.seed = seed;
  }

  public long nextLong() {
    return next64(64);
  }

  public void nextBytes(byte[] bytes) {
    for (int i = 0, len = bytes.length; i < len;) {
      // We regard seed as unsigned long, therefore used '>>>' instead of '>>'.
      for (long rnd = nextLong(), n = Math.min(len - i, Long.SIZE / Byte.SIZE);
           n-- > 0; rnd >>>= Byte.SIZE) {
        bytes[i++] = (byte) rnd;
      }
    }
  }

  private long next64(int bits) {
    seed = seed * multiplier + addend;
    return seed >>> (64 - bits);
  }


  /**
   * Random64 is a pseudorandom algorithm(LCG). Therefore, we will get same sequence
   * if seeds are the same. This main will test how many calls nextLong() it will
   * get the same seed.
   *
   * We do not need to save all numbers (that is too large). We could save
   * once every 100000 calls nextLong(). If it get a same seed, we can
   * detect this by calling nextLong() 100000 times continuously.
   *
   */
  public static void main(String[] args) {
    long defaultTotalTestCnt = 1000000000000L; // 1 trillion

    if (args.length == 1) {
      defaultTotalTestCnt = Long.parseLong(args[0]);
    }

    Preconditions.checkArgument(defaultTotalTestCnt > 0, "totalTestCnt <= 0");

    final int precision = 100000;
    final long totalTestCnt = defaultTotalTestCnt + precision;
    final int reportPeriod = 100 * precision;
    final long startTime = System.currentTimeMillis();

    System.out.println("Do collision test, totalTestCnt=" + totalTestCnt);

    Random64 rand = new Random64();
    Set<Long> longSet = new HashSet<>();

    for (long cnt = 1; cnt <= totalTestCnt; cnt++) {
      final long randLong = rand.nextLong();

      if (longSet.contains(randLong)) {
        System.err.println("Conflict! count=" + cnt);
        System.exit(1);
      }

      if (cnt % precision == 0) {
        if (!longSet.add(randLong)) {
          System.err.println("Conflict! count=" + cnt);
          System.exit(1);
        }

        if (cnt % reportPeriod == 0) {
          long cost = System.currentTimeMillis() - startTime;
          long remainingMs = (long) (1.0 * (totalTestCnt - cnt) * cost / cnt);
          System.out.println(
            String.format(
              "Progress: %.3f%%, remaining %d minutes",
              100.0 * cnt / totalTestCnt, remainingMs / 60000
            )
          );
        }
      }

    }

    System.out.println("No collision!");
  }

}
