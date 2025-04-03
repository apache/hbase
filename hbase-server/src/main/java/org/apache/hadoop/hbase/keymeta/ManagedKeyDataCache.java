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

import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory cache for ManagedKeyData entries, using key metadata as the cache key.
 */
@InterfaceAudience.Private
public class ManagedKeyDataCache {
  private final Map<String, ManagedKeyData> cache;
  private final Map<String, Map<Bytes, List<ManagedKeyData>>> prefixCache;
  private final ReentrantLock lock;

  public ManagedKeyDataCache() {
    this.prefixCache = new HashMap<>();
    this.cache = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  /**
   * Adds a new entry to the cache.
   *
   * @param keyData the ManagedKeyData entry to be added
   */
  public void addEntry(ManagedKeyData keyData) {
    lock.lock();
    try {
      Bytes keyCust = new Bytes(keyData.getKeyCustodian());
      String keyNamespace = keyData.getKeyNamespace();

      cache.put(keyData.getKeyMetadata(), keyData);

      Map<Bytes, List<ManagedKeyData>> nsCache = prefixCache.get(keyNamespace);
      if (nsCache == null) {
        nsCache = new HashMap<>();
        prefixCache.put(keyNamespace, nsCache);
      }

      List<ManagedKeyData> keyList = nsCache.get(keyCust);
      if (keyList == null) {
        keyList = new ArrayList<>();
        prefixCache.get(keyNamespace).put(keyCust, keyList);
      }

      keyList.add(keyData);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieves an entry from the cache based on its key metadata.
   *
   * @param keyMetadata the key metadata of the entry to be retrieved
   * @return the corresponding ManagedKeyData entry, or null if not found
   */
  public ManagedKeyData getEntry(String keyMetadata) {
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
   * @return the removed ManagedKeyData entry, or null if not found
   */
  public ManagedKeyData removeEntry(String keyMetadata) {
    lock.lock();
    try {
      ManagedKeyData removedEntry = cache.remove(keyMetadata);
      if (removedEntry != null) {
        Bytes keyCust = new Bytes(removedEntry.getKeyCustodian());
        String keyNamespace = removedEntry.getKeyNamespace();
        Map<Bytes, List<ManagedKeyData>> nsCache = prefixCache.get(keyNamespace);
        List<ManagedKeyData> keyList = nsCache != null ? nsCache.get(keyCust) : null;
        if (keyList != null) {
          keyList.remove(removedEntry);
          if (keyList.isEmpty()) {
            prefixCache.get(keyNamespace).remove(keyCust);
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
   * @param key_cust     The key custodian.
   * @param keyNamespace the key namespace to search for
   * @return a random ManagedKeyData entry with the given PBE prefix and ACTIVE status, or null if not found
   */
  public ManagedKeyData getRandomEntryForPrefix(byte[] key_cust, String keyNamespace) {
    lock.lock();
    try {
      List<ManagedKeyData> activeEntries = new ArrayList<>();

      Bytes keyCust = new Bytes(key_cust);
      Map<Bytes, List<ManagedKeyData>> nsCache = prefixCache.get(keyNamespace);
      List<ManagedKeyData> keyList = nsCache != null ? nsCache.get(keyCust) : null;
      if (keyList != null) {
        for (ManagedKeyData entry : keyList) {
          if (entry.getKeyStatus() == ManagedKeyStatus.ACTIVE) {
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
