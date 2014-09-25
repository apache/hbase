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

package org.apache.hadoop.hbase.io.util;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Dictionary interface
 *
 * Dictionary indexes should be either bytes or shorts, only positive. (The
 * first bit is reserved for detecting whether something is compressed or not).
 */
@InterfaceAudience.Private
public interface Dictionary {
  byte NOT_IN_DICTIONARY = -1;

  void init(int initialSize);
  /**
   * Gets an entry from the dictionary.
   * 
   * @param idx index of the entry
   * @return the entry, or null if non existent
   */
  byte[] getEntry(short idx);

  /**
   * Finds the index of an entry.
   * If no entry found, we add it.
   * 
   * @param data the byte array that we're looking up
   * @param offset Offset into <code>data</code> to add to Dictionary.
   * @param length Length beyond <code>offset</code> that comprises entry; must be > 0.
   * @return the index of the entry, or {@link #NOT_IN_DICTIONARY} if not found
   */
  short findEntry(byte[] data, int offset, int length);

  /**
   * Adds an entry to the dictionary.
   * Be careful using this method.  It will add an entry to the
   * dictionary even if it already has an entry for the same data.
   * Call {{@link #findEntry(byte[], int, int)}} to add without duplicating
   * dictionary entries.
   * 
   * @param data the entry to add
   * @param offset Offset into <code>data</code> to add to Dictionary.
   * @param length Length beyond <code>offset</code> that comprises entry; must be > 0.
   * @return the index of the entry
   */

  short addEntry(byte[] data, int offset, int length);

  /**
   * Flushes the dictionary, empties all values.
   */
  void clear();
}
