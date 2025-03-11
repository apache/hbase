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
package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory cache for PBEKeyData entries, using key metadata as the cache key.
 */
@InterfaceAudience.Private
public class PBEKeyDataCache {
  private final Map<String, PBEKeyData> cache;
  private final Map<String, Map<Bytes, List<PBEKeyData>>> prefixCache;
  private final ReentrantLock lock;

  public PBEKeyDataCache() {
    this.prefixCache = new HashMap<>();
    this.cache = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  /**
   * Adds a new entry to the cache.
   *
   * @param pbeKeyData the PBEKeyData entry to be added
   */
  public void addEntry(PBEKeyData pbeKeyData) {
    lock.lock();
    try {
      Bytes pbePrefix = new Bytes(pbeKeyData.getPBEPrefix());
      String keyNamespace = pbeKeyData.getKeyNamespace();

      cache.put(pbeKeyData.getKeyMetadata(), pbeKeyData);

      Map<Bytes, List<PBEKeyData>> nsCache = prefixCache.get(keyNamespace);
      if (nsCache == null) {
        nsCache = new HashMap<>();
        prefixCache.put(keyNamespace, nsCache);
      }

      List<PBEKeyData> keyList = nsCache.get(pbePrefix);
      if (keyList == null) {
        keyList = new ArrayList<>();
        prefixCache.get(keyNamespace).put(pbePrefix, keyList);
      }

      keyList.add(pbeKeyData);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieves an entry from the cache based on its key metadata.
   *
   * @param keyMetadata the key metadata of the entry to be retrieved
   * @return the corresponding PBEKeyData entry, or null if not found
   */
  public PBEKeyData getEntry(String keyMetadata) {
    lock.lock();
    try {
      return cache.get(keyMetadata);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Removes an entry from the cache based on its key metadata.
   *
   * @param keyMetadata the key metadata of the entry to be removed
   * @return the removed PBEKeyData entry, or null if not found
   */
  public PBEKeyData removeEntry(String keyMetadata) {
    lock.lock();
    try {
      PBEKeyData removedEntry = cache.remove(keyMetadata);
      if (removedEntry != null) {
        Bytes pbePrefix = new Bytes(removedEntry.getPBEPrefix());
        String keyNamespace = removedEntry.getKeyNamespace();
        Map<Bytes, List<PBEKeyData>> nsCache = prefixCache.get(keyNamespace);
        List<PBEKeyData> keyList = nsCache != null ? nsCache.get(pbePrefix) : null;
        if (keyList != null) {
          keyList.remove(removedEntry);
          if (keyList.isEmpty()) {
            prefixCache.get(keyNamespace).remove(pbePrefix);
          }
        }
      }
      return removedEntry;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieves a random entry from the cache based on its PBE prefix, key namespace, and filters out entries with
   * a status other than ACTIVE.
   *
   * @param pbe_prefix    the PBE prefix to search for
   * @param keyNamespace the key namespace to search for
   * @return a random PBEKeyData entry with the given PBE prefix and ACTIVE status, or null if not found
   */
  public PBEKeyData getRandomEntryForPrefix(byte[] pbe_prefix, String keyNamespace) {
    lock.lock();
    try {
      List<PBEKeyData> activeEntries = new ArrayList<>();

      Bytes pbePrefix = new Bytes(pbe_prefix);
      Map<Bytes, List<PBEKeyData>> nsCache = prefixCache.get(keyNamespace);
      List<PBEKeyData> keyList = nsCache != null ? nsCache.get(pbePrefix) : null;
      if (keyList != null) {
        for (PBEKeyData entry : keyList) {
          if (entry.getKeyStatus() == PBEKeyStatus.ACTIVE) {
            activeEntries.add(entry);
          }
        }
      }

      if (activeEntries.isEmpty()) {
        return null;
      }

      return activeEntries.get((int) (Math.random() * activeEntries.size()));
    } finally {
      lock.unlock();
    }
  }
}
