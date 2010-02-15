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
package org.apache.hadoop.hbase.regionserver.idx.support.sets;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import java.util.Arrays;

/**
 * A bitset implementation of the {@link IntSet} interface.
 * This class is not thread safe.
 */
class BitSet extends IntSetBase {

  /**
   * Number of bits in a word.
   */
  private static final int WORD_BIT_COUNT = 64;
  /**
   * The power of two to reach the word size. This is the shift needed to
   * get the index from a word.
   */
  private static final int INDEX_SHIFT = 6;
  /**
   * The fixed part in the heap size calcualtion.
   */
  static final int FIXED_SIZE = ClassSize.align(ClassSize.OBJECT +
    Bytes.SIZEOF_INT * 4 + ClassSize.ARRAY +
    ClassSize.REFERENCE + Bytes.SIZEOF_LONG);

  /**
   *
   */
  private int capacity;
  /*
    * Memebers deliberatly package protected to allow direct interaction.
   */
  long[] words;
  /**
   * The number of bits which are not zero.
   */
  int size;
  /**
   * The min bit which is not zero.
   */
  int minElement;
  /**
   * The max bit which is not zero.
   */
  int maxElement;
  /**
   * The heap size.
   */
  private long heapSize;

  /**
   * Construct a new bitset with a given maximum element.
   *
   * @param capacity the max element of this set (exclusive).
   */
  public BitSet(int capacity) {
    assert capacity >= SMALLEST && capacity <= LARGEST;
    this.capacity = capacity;
    clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    calcSizeMinMax();
    return size;
  }

  /**
   * Calculates the size/min/max fields. Used to lazily calculate these
   * as they are computationally expensive.
   */
  private void calcSizeMinMax() {
    if (size < 0) {
      size = 0;
      minElement = -1;
      maxElement = -1;
      int base = 0;
      for (int i = 0; i < words.length; i++) {
        if (words[i] != 0) {
          size += Long.bitCount(words[i]);
          minElement = minElement >= 0 ? minElement :
            base + Long.numberOfTrailingZeros(words[i]);
          maxElement = base + WORD_BIT_COUNT
            - 1 - Long.numberOfLeadingZeros(words[i]);
        }
        base += WORD_BIT_COUNT;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    size = 0;
    minElement = -1;
    maxElement = -1;
    if (words == null) {
      words = new long[capacity / 64 + (capacity % 64 > 0 ? 1 : 0)];
      heapSize = ClassSize.align(FIXED_SIZE + words.length * Long.SIZE);
    } else {
      for (int i = 0; i < words.length; i++) {
        words[i] = 0L;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int capacity() {
    return capacity;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  void addNext(int element) {
    assert element > maxElement && element < capacity;
    assert size >= 0;
    minElement = minElement == -1 ? element : minElement;
    maxElement = element;
    int word = element >> INDEX_SHIFT;
    words[word] |= (1L << element);
    size++;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(int element) {
    assert element >= 0 && element <= capacity;
    int word = element >> INDEX_SHIFT;
    return (words[word] & (1l << element)) != 0L;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet complement() {
    if (capacity > 0) {
      size = -1;
      for (int i = 0; i < words.length; i++) {
        words[i] = ~words[i];
      }
      words[words.length - 1] ^= capacity % WORD_BIT_COUNT == 0 ?
        0 : -1L << capacity;  // get rid of the trailing ones
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet intersect(IntSet other) {
    assert this.capacity == other.capacity();
    return other.getClass() == BitSet.class ? intersect((BitSet) other) :
      intersect((SparseBitSet) other);
  }

  /**
   * Interserct with a BitSet.
   *
   * @param other the other bitset.
   * @return the intersection result (the modified this)
   */
  private IntSet intersect(BitSet other) {
    size = -1;
    for (int i = 0; i < words.length; i++) {
      words[i] &= other.words[i];
    }
    return this;
  }

  /**
   * Intersect with a SparseBitSet
   *
   * @param other the sparse bit set
   * @return the intersection result (the modified this)
   */
  private IntSet intersect(SparseBitSet other) {
    size = -1;
    int index;
    int prevIndex = -1;
    for (int i = 0; i < other.length; i++) {
      index = other.indices[i];
      for (int j = prevIndex + 1; j < index; j++) {
        words[j] = 0;
      }
      words[index] &= other.words[i];
      prevIndex = index;
    }
    for (int j = prevIndex + 1; j < words.length; j++) {
      words[j] = 0;
    }

    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet unite(IntSet other) {
    assert this.capacity == other.capacity();
    return other.getClass() == BitSet.class ? unite((BitSet) other) :
      unite((SparseBitSet) other);
  }

  private IntSet unite(BitSet other) {
    size = -1;
    for (int i = 0; i < words.length; i++) {
      words[i] |= other.words[i];
    }
    return this;
  }

  private IntSet unite(SparseBitSet other) {
    size = -1;
    for (int i = 0; i < other.length; i++) {
      words[other.indices[i]] |= other.words[i];
    }
    return this;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet subtract(IntSet other) {
    assert this.capacity == other.capacity();
    return other.getClass() == BitSet.class ? subtract((BitSet) other) :
      subtract((SparseBitSet) other);
  }

  private IntSet subtract(BitSet other) {
    size = -1;
    for (int i = 0; i < words.length; i++) {
      words[i] &= ~other.words[i];
    }
    return this;
  }

  private IntSet subtract(SparseBitSet other) {
    size = -1;
    for (int i = 0; i < other.length; i++) {
      words[other.indices[i]] &= ~other.words[i];
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet difference(IntSet other) {
    assert this.capacity == other.capacity();
    return other.getClass() == BitSet.class ? difference((BitSet) other) :
      difference((SparseBitSet) other);
  }

  private IntSet difference(BitSet other) {
    size = -1;
    for (int i = 0; i < words.length; i++) {
      words[i] ^= other.words[i];
    }
    return this;
  }

  private IntSet difference(SparseBitSet other) {
    size = -1;
    for (int i = 0; i < other.length; i++) {
      words[other.indices[i]] ^= other.words[i];
    }
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BitSet clone() {
    BitSet result = (BitSet) super.clone();
    result.words = this.words.clone();
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSetIterator iterator() {
    calcSizeMinMax();
    return new IntSetIteratorImpl();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long heapSize() {
    return heapSize;
  }

  private class IntSetIteratorImpl implements IntSetIterator {
    private static final int PREFETCH_SIZE = 11;

    private int[] nextValues;
    private int nextValueIndex;
    private int numValues;
    private int nextWordIndex;
    private int largestNextValue;
    private int limit;

    private IntSetIteratorImpl() {
      nextValues = new int[PREFETCH_SIZE];
      nextWordIndex = minElement >> INDEX_SHIFT;
      limit = maxElement;
      largestNextValue = -1;
      fill();
    }

    /**
     * Fills up the array of set bits.
     */
    private void fill() {
      numValues = 0;
      nextValueIndex = 0;
      while (largestNextValue < limit && nextWordIndex < words.length) {
        if (words[nextWordIndex] != 0) {
          long word = words[nextWordIndex];
          int base = nextWordIndex * WORD_BIT_COUNT;
          int shiftAmount = largestNextValue - base + 1;
          if (shiftAmount > 0) {
            word = ((word >>> shiftAmount) << shiftAmount);
          }
          while (word != 0 && numValues < nextValues.length) {
            int setBitIndex = Long.numberOfTrailingZeros(word);
            largestNextValue = nextValues[numValues++] = setBitIndex + base;
            word ^= 1L << setBitIndex;
          }
          if (word == 0) {
            nextWordIndex++;
          }
          if (numValues == nextValues.length || largestNextValue >= limit ||
            nextWordIndex >= words.length) {
            break;
          }
        } else {
          nextWordIndex++;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return nextValueIndex < numValues || largestNextValue < maxElement;
    }

    @Override
    public int next() {
      if (nextValueIndex < numValues) {
        return nextValues[nextValueIndex++];
      } else {
        fill();
        if (nextValueIndex < numValues) {
          return nextValues[nextValueIndex++];
        }
      }
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    BitSet otherBitSet = (BitSet) other;
    this.calcSizeMinMax();
    otherBitSet.calcSizeMinMax();

    return heapSize == otherBitSet.heapSize &&
      capacity == otherBitSet.capacity &&
      maxElement == otherBitSet.maxElement &&
      minElement == otherBitSet.minElement &&
      size == otherBitSet.size &&
      Arrays.equals(words, otherBitSet.words);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(words);
  }
}
