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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * High scalable counter. Thread safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Counter {
  private static final int MAX_CELLS_LENGTH = 1 << 20;

  private static class Cell {
    // Pads are added around the value to avoid cache-line contention with
    // another cell's value. The cache-line size is expected to be equal to or
    // less than about 128 Bytes (= 64 Bits * 16).

    @SuppressWarnings("unused")
    volatile long p0, p1, p2, p3, p4, p5, p6;
    volatile long value;
    @SuppressWarnings("unused")
    volatile long q0, q1, q2, q3, q4, q5, q6;

    static final AtomicLongFieldUpdater<Cell> valueUpdater =
        AtomicLongFieldUpdater.newUpdater(Cell.class, "value");

    Cell() {}

    Cell(long initValue) {
      value = initValue;
    }

    long get() {
      return value;
    }

    boolean add(long delta) {
      long current = value;
      return valueUpdater.compareAndSet(this, current, current + delta);
    }
  }

  private static class Container {
    /** The length should be a power of 2. */
    final Cell[] cells;

    /** True if a new extended container is going to replace this. */
    final AtomicBoolean demoted = new AtomicBoolean();

    Container(Cell cell) {
      this(new Cell[] { cell });
    }

    /**
     * @param cells the length should be a power of 2
     */
    Container(Cell[] cells) {
      this.cells = cells;
    }
  }

  private final AtomicReference<Container> containerRef;

  public Counter() {
    this(new Cell());
  }

  public Counter(long initValue) {
    this(new Cell(initValue));
  }

  private Counter(Cell initCell) {
    containerRef = new AtomicReference<Container>(new Container(initCell));
  }

  private static int hash() {
    // The logic is borrowed from high-scale-lib's ConcurrentAutoTable.

    int h = System.identityHashCode(Thread.currentThread());
    // You would think that System.identityHashCode on the current thread
    // would be a good hash fcn, but actually on SunOS 5.8 it is pretty lousy
    // in the low bits.

    h ^= (h >>> 20) ^ (h >>> 12); // Bit spreader, borrowed from Doug Lea
    h ^= (h >>>  7) ^ (h >>>  4);
    return h;
  }

  public void add(long delta) {
    Container container = containerRef.get();
    Cell[] cells = container.cells;
    int mask = cells.length - 1;

    int baseIndex = hash();
    if(cells[baseIndex & mask].add(delta)) {
      return;
    }

    int index = baseIndex + 1;
    while(true) {
      if(cells[index & mask].add(delta)) {
        break;
      }
      index++;
    }

    if(index - baseIndex >= cells.length &&
        cells.length < MAX_CELLS_LENGTH &&
        container.demoted.compareAndSet(false, true)) {

      if(containerRef.get() == container) {
        Cell[] newCells = new Cell[cells.length * 2];
        System.arraycopy(cells, 0, newCells, 0, cells.length);
        for(int i = cells.length; i < newCells.length; i++) {
          newCells[i] = new Cell();
          // Fill all of the elements with instances. Creating a cell on demand
          // and putting it into the array makes a concurrent problem about
          // visibility or, in other words, happens-before relation, because
          // each element of the array is not volatile so that you should
          // establish the relation by some piggybacking.
        }
        containerRef.compareAndSet(container, new Container(newCells));
      }
    }
  }

  public void increment() {
    add(1);
  }

  public void decrement() {
    add(-1);
  }

  public void set(long value) {
    containerRef.set(new Container(new Cell(value)));
  }

  public long get() {
    long sum = 0;
    for(Cell cell : containerRef.get().cells) {
      sum += cell.get();
    }
    return sum;
  }

  @Override
  public String toString() {
    Cell[] cells = containerRef.get().cells;

    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long sum = 0;

    for(Cell cell : cells) {
      long value = cell.get();
      sum += value;
      if(min > value) { min = value; }
      if(max < value) { max = value; }
    }

    return new StringBuilder(100)
    .append("[value=").append(sum)
    .append(", cells=[length=").append(cells.length)
    .append(", min=").append(min)
    .append(", max=").append(max)
    .append("]]").toString();
  }
}
