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
package org.apache.hadoop.hbase.regionserver.idx.support.arrays;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * An interface for an array list, typically used as a key store.
 *
 * @param <T> the element type
 */
public interface List<T> extends Iterable<T>, BinarySearch.Searchable<T>,
  HeapSize {
  
  long FIXED_OVERHEAD =
    ClassSize.align(ClassSize.ARRAY + Bytes.SIZEOF_INT + ClassSize.OBJECT);

  /**
   * Adds the element to the end of the list.
   *
   * @param e the new element
   */
  void add(byte[] e);

  /**
   * Sets the specified index to the nominated value.
   *
   * @param index    the list index
   * @param newValue the value
   */
  void set(int index, byte[] newValue);

  /**
   * Inserts at the specified index to the list.
   *
   * @param index    the index to insert
   * @param newValue the value to insert
   */
  void insert(int index, byte[] newValue);

  /**
   * Checks if the list is empty.
   *
   * @return true if the list is empty
   */
  boolean isEmpty();

  /**
   * Returns the current number of elements in this list.
   *
   * @return the number of elements.
   */
  int size();


}
