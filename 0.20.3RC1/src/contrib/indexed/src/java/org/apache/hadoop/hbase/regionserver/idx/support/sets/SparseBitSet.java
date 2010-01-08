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
 * A very simple sparse bitset which assumes most bits are not set.
 * It is composed of an array of indices (int) and an array of longs for the
 * words.
 * <p/>
 * This class is designed so that set operations with the {@link BitSet}
 * types can be implemented efficiently.
 * <p/>
 * This class is not thread safe.
 */
class SparseBitSet extends IntSetBase {
  /**
   * The fixed part in the heap size calcualtion.
   */
  static final int FIXED_SIZE = ClassSize.align(ClassSize.OBJECT +
    Bytes.SIZEOF_INT * 3 + (ClassSize.ARRAY + ClassSize.REFERENCE) * 2 +
    Bytes.SIZEOF_LONG);


  /**
   * Default initial size of the array backing this list.
   */
  private static final int DEFAULT_SIZE = 1;

  /**
   * The scaling factor we use to resize the backing buffer when the list
   * needs to grow.
   */
  private static final float SCALE_FACTOR = 1.5f;

  /**
   * Number of bits in a word.
   */
  private static final int WORD_BIT_COUNT = 64;
  /**
   * The power of two to reach the word size. This is the shift needed to get
   * the index from a word.
   */
  private static final int INDEX_SHIFT = 6;

  /**
   * If the length is greater than this threshold than binary search will be
   * used instead of scanning.
   */
  private static final int BINARY_SEARCH_THRESHOLD = 16;

  /**
   * The array of indices. Length must match the length of the array of words.
   * Each index denote the index of the mathcing word in the words array in a
   * deflated  BitSet.
   * <p/>
   * Package protected to allow direct access by friends.
   */
  int[] indices;
  /**
   * An array of non-zero words. The indexes of these words are kept in the
   * indices array.
   * <p/>
   * Package protected to allow direct access by friends.
   */
  long[] words;

  /**
   * Number of used words.
   * <p/>
   * Package protected to allow direct access by friends.
   */
  int length;
  /**
   * Number of non-zero bits.
   */
  private int size;
  /**
   * The maximum integer that can be stored in this set.
   * <p/>
   * Useful when converting this class to a {@link BitSet}.
   */
  private int capacity;
  /**
   * The heap size.
   */
  private long heapSize;

  SparseBitSet() {
    capacity = LARGEST;
    clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    return size;
  }

  /**
   * Converts this sparse bit set to deflated {@link BitSet}.
   *
   * @return the deflated bit set
   */
  public BitSet toBitSet() {
    assert capacity <= LARGEST;
    BitSet bitSet = new BitSet(capacity);
    if (size > 0) {
      for (int i = 0; i < length; i++) {
        bitSet.words[indices[i]] = words[i];
      }
      bitSet.size = this.size;
      bitSet.minElement = indices[0] * WORD_BIT_COUNT +
        Long.numberOfTrailingZeros(words[0]);
      bitSet.maxElement = indices[length - 1] * WORD_BIT_COUNT + WORD_BIT_COUNT
        - 1 - Long.numberOfLeadingZeros(words[length - 1]);
    }
    return bitSet;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    indices = null;
    words = null;
    size = 0;
    length = 0;
    heapSize = FIXED_SIZE;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public int capacity() {
    return capacity;
  }

  /**
   * Adjusts the capacity of this set. Can only increase the capacity.
   *
   * @param capacity the capacity of this set.
   */
  public void setCapacity(int capacity) {
    assert capacity >= SMALLEST && capacity <= LARGEST;
    assert length == 0 || capacity >= indices[length - 1] << INDEX_SHIFT;
    this.capacity = capacity;
  }


  /**
   * Grows the backing array to the requested size.
   *
   * @param element the element for which the capacity needs to be ensured
   * @return the word index for this element
   */
  private int ensureArrayCapacity(int element) {
    assert element >= SMALLEST && element < capacity;
    int word = element >> INDEX_SHIFT;
    assert length < 0 || word >= indices[length - 1];
    // If we need to resize
    if (length == indices.length && word > indices[length - 1]) {
      // Calculate the new size, growing slowly at the start to avoid
      // overallocation too early.
      int newArrayLength = (int) (indices.length * SCALE_FACTOR + 1);

      // Create the new array
      int[] newIndices = new int[newArrayLength];
      long[] newWords = new long[newArrayLength];

      // Populate the new backing array
      System.arraycopy(indices, 0, newIndices, 0, length);
      System.arraycopy(words, 0, newWords, 0, length);
      indices = newIndices;
      words = newWords;

      heapSize = FIXED_SIZE +
        (Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT) * newArrayLength;
    }
    return word;
  }

  /**
   * Add the next element in sorted order to this bitset.
   *
   * @param element the element to add
   */
  void addNext(int element) {
    if (length == 0) {
      addFirstElement(element);
    } else {
      addNextElement(element);
    }
  }

  /**
   * Adds the next elemenet. Used to add all the elements except the first.
   *
   * @param element the element to add
   */
  private void addNextElement(int element) {
    int word = ensureArrayCapacity(element);
    if (word != indices[length - 1]) {
      indices[length++] = word;
    }
    assert Long.highestOneBit(words[length - 1]) < (1L << element) ||
      (words[length - 1] >= 0 && (1L << element) < 0)
      : "element=" + element;
    words[length - 1] |= (1L << element);
    size++;
  }

  /**
   * Adds the first element. This method allocates memory.
   *
   * @param firstElement the first element of this set
   */
  private void addFirstElement(int firstElement) {
    assert firstElement >= SMALLEST && firstElement < capacity;
    indices = new int[DEFAULT_SIZE];
    words = new long[DEFAULT_SIZE];
    length = 1;
    size = 1;
    // put the first element
    indices[0] = firstElement >> INDEX_SHIFT;
    words[0] |= (1L << firstElement);
    heapSize = FIXED_SIZE +
      (Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT) * DEFAULT_SIZE;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(int element) {
    int word = element >> INDEX_SHIFT;
    if (length < BINARY_SEARCH_THRESHOLD) {
      for (int i = 0; i < indices.length; i++) {
        if (indices[i] == word) {
          return (words[i] & (1L << element)) != 0L;
        }
      }
    } else {
      int index = Arrays.binarySearch(indices, 0, length, word);
      if (index >= 0) {
        return (words[index] & (1L << element)) != 0L;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet complement() {
    return toBitSet().complement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet intersect(IntSet other) {
    return toBitSet().intersect(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet unite(IntSet other) {
    return toBitSet().unite(other);
  }

  @Override
  public IntSet subtract(IntSet other) {
    return toBitSet().subtract(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet difference(IntSet other) {
    return toBitSet().difference(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSetIterator iterator() {
    return new IntSetIteratorImpl();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Clones by creating a full bitset out of this object.
   */
  @Override
  public IntSet clone() {
    SparseBitSet result = (SparseBitSet) super.clone();
    if (this.indices != null) {
      result.indices = this.indices.clone();
      result.words = this.words.clone();
    }
    return result;
  }

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

    private IntSetIteratorImpl() {
      nextValues = new int[PREFETCH_SIZE];
      nextWordIndex = 0;
      largestNextValue = -1;
      fill();
    }

    /**
     * Fills the cache with the next set of bits.
     */
    private void fill() {
      numValues = 0;
      nextValueIndex = 0;
      while (nextWordIndex < length && numValues < nextValues.length) {
        long word = words[nextWordIndex];
        int base = indices[nextWordIndex] * WORD_BIT_COUNT;
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
      }
    }

    @Override
    public boolean hasNext() {
      return nextWordIndex < length || nextValueIndex < numValues;
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

    SparseBitSet otherBitSet = (SparseBitSet) other;

    return heapSize == otherBitSet.heapSize &&
      capacity == otherBitSet.capacity &&
      length == otherBitSet.length &&
      Arrays.equals(indices, otherBitSet.indices) &&
      Arrays.equals(words, otherBitSet.words);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(words) ^ Arrays.hashCode(indices);
  }


}
