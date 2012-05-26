/**
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
package org.apache.hadoop.hbase.metrics.histogram;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A random sample of a stream of longs. Uses Vitter's Algorithm R to produce a
 * statistically representative sample.
 *
 * see: http://www.cs.umd.edu/~samir/498/vitter.pdf
 */
public class UniformSample implements Sample {
  
  private static final Random RANDOM = new Random();
  private static final int BITS_PER_LONG = 63;
  
  private final AtomicLong count = new AtomicLong();
  private final AtomicLongArray values;

  /**
   * Creates a new UniformSample
   *
   * @param reservoirSize the number of samples to keep
   */
  public UniformSample(int reservoirSize) {
    this.values = new AtomicLongArray(reservoirSize);
    clear();
  }

  @Override
  public void clear() {
    for (int i = 0; i < values.length(); i++) {
      values.set(i, 0);
    }
    count.set(0);
  }

  @Override
  public int size() {
    final long c = count.get();
    if (c > values.length()) {
      return values.length();
    }
    return (int) c;
  }

  @Override
  public void update(long value) {
    final long c = count.incrementAndGet();
    if (c <= values.length()) {
      values.set((int) c - 1, value);
    } else {
      final long r = nextLong(c);
      if (r < values.length()) {
        values.set((int) r, value);
      }
    }
  }

  /**
   * Get a pseudo-random long uniformly between 0 and n-1. Stolen from
   * {@link java.util.Random#nextInt()}.
   *
   * @param n the bound
   * @return a value select randomly from the range {@code [0..n)}.
   */
  private static long nextLong(long n) {
    long bits, val;
    do {
      bits = RANDOM.nextLong() & (~(1L << BITS_PER_LONG));
      val = bits % n;
    } while (bits - val + (n - 1) < 0L);
    return val;
  }

  @Override
  public Snapshot getSnapshot() {
    final int s = size();
    final List<Long> copy = new ArrayList<Long>(s);
    for (int i = 0; i < s; i++) {
      copy.add(values.get(i));
    }
    return new Snapshot(copy);
  }
}
