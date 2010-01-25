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

/**
 * A generic implementation of the binary search algorithm that can run on
 * any type of object that conforms to the {@link Searchable} or any of the
 * directly supported primitive types or buffers.
 */
public final class BinarySearch {

  /**
   * Suppress construction for utility classes.
   */
  private BinarySearch() {
  }


  /**
   * Conducts a binary search for the requested value in the supplied target
   * object.
   * <p/>
   * If not found, returns -(insertionPoint + 1)
   * This is always a negative number. To convert this negative number into
   * the real the insertion point, call convertToInsertionIndex(int)
   *
   * @param haystack       the object you wish to search
   * @param haystackLength the number of objects in the haystack
   * @param needle         the value you are looking for
   * @param <H>            the class that we are searching
   * @param <N>            the class of needle we are looking for
   * @return the position the key was found in
   */
  public static <H extends Searchable<N>, N> int search(H haystack,
    int haystackLength, N needle) {
    // Check argument validity
    if (haystack == null) {
      throw new IllegalArgumentException("Argument 'Check' cannot be null");
    }
    if (needle == null) {
      throw new IllegalArgumentException("Argument 'needle' cannot be null");
    }

    // Initialise boundaries
    int high = haystackLength;
    int low = -1;

    // Search until the high and low markers are next to each other
    while (high - low > 1) {
      // Calculate the mid-point to check
      int probe = (low + high) >>> 1;

      // Move the markers. Note that the comparison returns < 0 if the needle is
      // less than the comparison index so this test is opposite to the standard
      int comparison = haystack.compare(needle, probe);
      if (comparison > 0) {
        low = probe;
      } else {
        high = probe;
      }
    }

    // If the high marker hasn't moved (still off the end of the target), or
    // the value we landed on isnt what we were looking for, we didn't find it
    if (high == haystackLength || haystack.compare(needle, high) != 0) {
      // Return the encoded insertion position.
      return -(high + 1);
    } else {
      // Return the match position
      return high;
    }
  }

  /**
   * Conducts a binary search for the requested value in the supplied target
   * object.
   * <p/>
   * If not found, returns -(insertionPoint + 1)
   * This is always a negative number. To convert this negative number into the
   * real the insertion point, call convertToInsertionIndex(int)
   *
   * @param haystack       the object you wish to search
   * @param haystackLength the number of objects in the haystack
   * @param needle         the value you are looking for
   * @param <H>            the class that we are searching
   * @param <N>            the class of needle we are looking for
   * @return the position the key was found in
   */
  public static <H extends Searchable<N>, N> int search(H haystack,
    int haystackLength, byte[] needle) {
    return search(haystack, haystackLength, haystack.fromBytes(needle));
  }

  /**
   * This interface enforces the required methods to search an arbitrary
   * object with a binary search algorithm.
   */
  public interface Searchable<N> {
    /**
     * Create the needle for a byte array.
     *
     * @param bytes the byte array to use
     * @return the needle instance
     */
    N fromBytes(byte[] bytes);

    /**
     * Compares the two requested elements.
     *
     * @param needle         the value we are looking for
     * @param compareToIndex the index of the element to compare the needle to
     * @return -ve, 0, +ve if the needle is <, = or > than the element to check
     */
    int compare(N needle, int compareToIndex);
  }
}
