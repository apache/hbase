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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pool of string values encoded to integer IDs for use in BlockCacheKey. This allows for avoiding
 * duplicating string values for file names, region and CF values on various BlockCacheKey
 * instances. Normally, single hfiles have many blocks. This means all blocks from the same file
 * will have the very same file, region and CF names. On very large BucketCache setups (i.e. file
 * based cache with TB size order), can save few GBs of memory by avoiding repeating these common
 * string values on blocks from the same file. The FilePathStringPool is implemented as a singleton,
 * since the same pool should be shared by all BlockCacheKey instances, as well as the BucketCache
 * object itself. The Id for an encoded string is an integer. Any new String added to the pool is
 * assigned the next available integer ID, starting from 0 upwards. That sets the total pool
 * capacity to Integer.MAX_VALUE. In the event of ID exhaustion (integer overflow when Id values
 * reach Integer.MAX_VALUE), the encode() method will restart iterating over int values
 * incrementally from 0 until it finds an unused ID. Strings can be removed from the pool using the
 * remove() method. BucketCache should call this when evicting all blocks for a given file (see
 * BucketCache.evictFileBlocksFromCache()).
 * <p>
 * Thread-safe implementation that maintains bidirectional mappings between strings and IDs.
 * </p>
 */
@InterfaceAudience.Private
public class FilePathStringPool {
  private static final Logger LOG = LoggerFactory.getLogger(FilePathStringPool.class);

  // Bidirectional mappings for string objects re-use
  private final ConcurrentHashMap<String, Integer> stringToId = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, String> idToString = new ConcurrentHashMap<>();
  private final AtomicInteger nextId = new AtomicInteger(0);

  private static FilePathStringPool instance;

  public static FilePathStringPool getInstance() {
    synchronized (FilePathStringPool.class) {
      if (instance == null) {
        instance = new FilePathStringPool();
      }
    }
    return instance;
  }

  private FilePathStringPool() {
    // Private constructor for singleton
  }

  /**
   * Gets or creates an integer ID for the given String.
   * @param string value for the file/region/CF name.
   * @return the integer ID encoding this string in the pool.
   */
  public int encode(String string) {
    if (string == null) {
      throw new IllegalArgumentException("string cannot be null");
    }
    return stringToId.computeIfAbsent(string, name -> {
      if (stringToId.size() == Integer.MAX_VALUE) {
        throw new IllegalStateException(
          "String pool has reached maximum capacity of " + Integer.MAX_VALUE + " unique strings.");
      }
      int id = nextId.getAndIncrement();
      while (idToString.containsKey(id)) {
        id = nextId.getAndIncrement();
        if (id == Integer.MAX_VALUE) {
          nextId.set(0);
          LOG.info("Id values reached Integer.MAX_VALUE, restarting from 0");
        }
      }
      idToString.put(id, name);
      LOG.trace("Encoded new string to ID {}: {}", id, name);
      return id;
    });
  }

  /**
   * Decodes an integer ID back to its original file name.
   * @param id the integer ID
   * @return the original file name, or null if not found
   */
  public String decode(int id) {
    return idToString.get(id);
  }

  /**
   * Checks if a given string ID is already being used.
   * @param id the integer ID to check
   * @return true if the ID exists
   */
  public boolean contains(int id) {
    return idToString.containsKey(id);
  }

  /**
   * Checks if a given string has been encoded.
   * @param string the value to check
   * @return true if the string value has been encoded
   */
  public boolean contains(String string) {
    return stringToId.containsKey(string);
  }

  /**
   * Gets the number of unique file names currently tracked.
   * @return the number of entries in the codec
   */
  public int size() {
    return stringToId.size();
  }

  /**
   * Removes a string value and its ID from the pool. This should only be called when all blocks for
   * a file have been evicted from the cache.
   * @param string the file name to remove
   * @return true if the file name was removed, false if it wasn't present
   */
  public boolean remove(String string) {
    if (string == null) {
      return false;
    }
    Integer id = stringToId.remove(string);
    if (id != null) {
      idToString.remove(id);
      LOG.debug("Removed string value from pool: {} (ID: {})", string, id);
      return true;
    }
    return false;
  }

  /**
   * Clears all mappings from the codec.
   */
  public void clear() {
    stringToId.clear();
    idToString.clear();
    nextId.set(0);
    LOG.info("Cleared all file name mappings from codec");
  }

  /**
   * Gets statistics about memory savings from string pooling.
   * @return a formatted string with compression statistics
   */
  public String getPoolStats() {
    long uniqueStrings = stringToId.size();
    if (uniqueStrings == 0) {
      return "No strings encoded";
    }
    // Calculate average string length
    long totalChars = stringToId.keySet().stream().mapToLong(String::length).sum();
    double avgLength = (double) totalChars / uniqueStrings;
    return String.format("FilePathStringPool stats: %d unique strings, avg length: %.1f chars, ",
      uniqueStrings, avgLength);
  }
}
