/*
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
package org.apache.hadoop.hbase.regionserver.idx.support.arrays;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An object array list implemented using the same methodology as the primitve
 * arrays but without the extra heap size and {@link List} methods.
 * <p/>
 * NOTE: This class is completely unsynchronised.
 *
 * @param <T> element type
 */
public class ObjectArrayList<T> implements Iterable<T> {

  /**
   * Default initial size of the array backing this list.
   */
  private static final int DEFAULT_SIZE = 1;

  /**
   * The scaling factor we use to resize the backing buffer when the list needs to grow.
   */
  private static final float SCALE_FACTOR = 1.5f;

  /**
   * The array backing this list.
   */
  private T[] values;

  /**
   * The number of values present in the list.
   */
  private int size;

  /**
   * Constructor that initialises with the default size.
   */
  public ObjectArrayList() {
    this(DEFAULT_SIZE);
  }

  /**
   * Constructor which initialises with the specified initial capacity.
   *
   * @param initialCapacity the initial capacity of the backing array
   */
  @SuppressWarnings("unchecked")
  public ObjectArrayList(int initialCapacity) {
    values = (T[]) new Object[initialCapacity];
  }

  /**
   * Constructor which initialises the content from the supplied array list.
   *
   * @param initial the initial contents
   */
  public ObjectArrayList(ObjectArrayList<T> initial) {
    // Initialise the internal storage to the appropriate size
    this(initial.size);

    // Copy over the references/values
    System.arraycopy(initial.values, 0, this.values, 0, initial.size);
    this.size = initial.size;
  }

  /**
   * Checks the contents of the collection for equality.
   * <p/>
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object compareTo) {
    if (this == compareTo) {
      return true;
    }
    if (!(compareTo instanceof ObjectArrayList)) {
      return false;
    }

    ObjectArrayList<?> that = (ObjectArrayList<?>) compareTo;

    return Arrays.equals(this.values, that.values);
  }

  /**
   * Adds the element to the end of the list.
   *
   * @param e the new element
   */
  public void add(T e) {
    ensureCapacity(size + 1);
    values[size] = e;
    size++;
  }

  /**
   * Grows the backing array to the requested size.
   *
   * @param requested the new capacity.
   */
  @SuppressWarnings("unchecked")
  private void ensureCapacity(int requested) {
    // If we need to resize
    if (requested > values.length) {
      // Calculate the new size, growing slowly at the start to avoid overallocation too early.
      int newSize = Math.max(requested, (int) (values.length * SCALE_FACTOR + 1));

      // Create the new array
      T[] newValues = (T[]) new Object[newSize];

      // Populate the new backing array
      System.arraycopy(values, 0, newValues, 0, size);
      values = newValues;
    }
  }

  /**
   * Retrieves the element at the requested index.
   *
   * @param index the element index you wish to retrieve
   * @return the value at that index
   */
  public T get(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException("Attempted to access index " + index + " but array is " + size + " elements");
    }

    return values[index];
  }

  /**
   * Searches the list for the nominated value.
   *
   * @param searchFor the value you are looking for
   * @return the first index the value was found at or -1 if not found
   */
  public int indexOf(T searchFor) {
    // Check each of the values. Don't bother with get() since we don't need its protection.
    for (int i = 0; i < size; i++) {
      if (values[i].equals(searchFor)) {
        return i;
      }
    }

    // Didn't find it.
    return -1;
  }

  /**
   * Simple iterator that runs over the values in the list.
   */
  private static final class InternalIterator<T>
    implements Iterator<T> {

    private T[] values;
    private int size;
    private int current = 0;

    private InternalIterator(T[] values, int size) {
      this.values = values;
      this.size = size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasNext() {
      return current < size;
    }

    /**
     * {@inheritDoc}
     */
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return values[current++];
    }

    /**
     * Not supported.
     */
    public void remove() {
      throw new UnsupportedOperationException("remove() is not supported");
    }
  }

  /**
   * Returns an iterator over the underlying content. Note that this is completely unsynchronised and the contents can change under you.
   */
  public Iterator<T> iterator() {
    return new InternalIterator<T>(values, size);
  }

  /**
   * Checks if the list is empty.
   *
   * @return true if the list is empty
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Sets the specified index to the nominated value.
   *
   * @param index    the list index
   * @param newValue the value
   */
  public void set(int index, T newValue) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException("Attempted to access index " + index + " but array is " + size + " elements");
    }

    values[index] = newValue;
  }

  /**
   * Removes the specified index from the list.
   *
   * @param index the index to remove
   * @return the original value
   */
  public T remove(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException("Attempted to access index " + index + " but array is " + size + " elements");
    }

    T original = values[index];
    System.arraycopy(values, index + 1, values, index, size - index - 1);
    values[size - 1] = null;
    size--;
    return original;
  }

  /**
   * Inserts at the specified index to the list.
   *
   * @param index    the index to insert
   * @param newValue the value to insert
   */
  public void insert(int index, T newValue) {
    if (index > size) {
      throw new ArrayIndexOutOfBoundsException("Attempted to access index " + index + " but array is " + size + " elements");
    }

    ensureCapacity(size + 1);
    if (index != size) {
      System.arraycopy(values, index, values, index + 1, size - index);
    }
    values[index] = newValue;
    size++;
  }


  /**
   * Removes the last item in the list.
   *
   * @return the original value
   */
  public T removeLast() {
    if (size < 1) {
      throw new ArrayIndexOutOfBoundsException("Attempted to remove last element from array with size 0");
    }

    T result = values[size - 1];
    values[size - 1] = null;
    size--;

    return result;
  }

  /**
   * Returns the current number of elements in this list.
   *
   * @return the number of elements.
   */
  public int size() {
    return size;
  }

  /**
   * Return a nice view of the list.
   * {@inheritDoc}
   */
  public String toString() {
    return Arrays.toString(Arrays.copyOf(values, size));
  }


}
