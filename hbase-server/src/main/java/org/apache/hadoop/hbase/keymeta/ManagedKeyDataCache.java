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

import java.io.IOException;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * In-memory cache for ManagedKeyData entries, using key metadata as the cache key.
 * Uses two independent Caffeine caches: one for general key data and one for active keys only
 * with hierarchical structure for efficient random key selection.
 */
@InterfaceAudience.Private
public class ManagedKeyDataCache extends KeyManagementBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKeyDataCache.class);

  private final Cache<String, ManagedKeyData> cache;
  private final Cache<CacheKey, List<ManagedKeyData>> activeKeysCache;
  private final KeymetaTableAccessor keymetaAccessor;

  /**
   * Composite key for active keys cache containing custodian and namespace.
   */
  private static class CacheKey {
    private final byte[] custodian;
    private final String namespace;

    public CacheKey(byte[] custodian, String namespace) {
      this.custodian = custodian;
      this.namespace = namespace;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      CacheKey cacheKey = (CacheKey) obj;
      return Bytes.equals(custodian, cacheKey.custodian) &&
             Objects.equals(namespace, cacheKey.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Bytes.hashCode(custodian), namespace);
    }
  }

  public ManagedKeyDataCache(Server server) {
    this(server, null);
  }

  public ManagedKeyDataCache(Server server, KeymetaTableAccessor keymetaAccessor) {
    super(server);
    this.keymetaAccessor = keymetaAccessor;

    Configuration conf = server.getConfiguration();
    int maxSize = conf.getInt(HConstants.CRYPTO_MANAGED_KEYS_CACHE_MAX_SIZE_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_CACHE_MAX_SIZE_DEFAULT);
    int activeKeysMaxSize = conf.getInt(HConstants.CRYPTO_MANAGED_KEYS_ACTIVE_CACHE_MAX_SIZE_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_ACTIVE_CACHE_MAX_SIZE_DEFAULT);

    this.cache = Caffeine.newBuilder()
        .maximumSize(maxSize)
        .build();

    this.activeKeysCache = Caffeine.newBuilder()
        .maximumSize(activeKeysMaxSize)
        .build();
  }



  /**
   * Retrieves an entry from the cache, loading it from KeymetaTableAccessor if not present.
   * This method uses a lambda function to automatically load missing entries.
   *
   * @param key_cust the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata the key metadata of the entry to be retrieved
   * @param wrappedKey The DEK key material encrypted with the corresponding KEK, if available.
   * @return the corresponding ManagedKeyData entry, or null if not found
   * @throws IOException if an error occurs while loading from KeymetaTableAccessor
   * @throws KeyException if an error occurs while loading from KeymetaTableAccessor
   */
  public ManagedKeyData getEntry(byte[] key_cust, String keyNamespace, String keyMetadata, byte[] wrappedKey)
      throws IOException, KeyException {
    return cache.get(keyMetadata, metadata -> {
      // First check if it's in the active keys cache
      ManagedKeyData activeKey = getFromActiveKeysCache(key_cust, keyNamespace, keyMetadata);
      if (activeKey != null) {
        // Found in active cache, add to main cache and return
        cache.put(metadata, activeKey);
        return activeKey;
      }

      // First try to load from KeymetaTableAccessor
      if (keymetaAccessor != null) {
        try {
          ManagedKeyData keyData = keymetaAccessor.getKey(key_cust, keyNamespace, metadata);
          if (keyData != null) {
            return keyData;
          }
        } catch (IOException | KeyException e) {
          LOG.warn("Failed to load key from KeymetaTableAccessor for metadata: {}", metadata, e);
        }
      }

      // If not found in KeymetaTableAccessor and dynamic lookup is enabled, try with Key Provider
      if (isDynamicLookupEnabled()) {
        try {
          ManagedKeyProvider provider = getKeyProvider();
          ManagedKeyData keyData = provider.unwrapKey(metadata, wrappedKey);
          if (keyData != null) {
            LOG.info("Got key data with status: {} and metadata: {} for prefix: {}",
              keyData.getKeyState(), keyData.getKeyMetadata(),
              ManagedKeyProvider.encodeToStr(key_cust));
            // Add to KeymetaTableAccessor for future L2 lookups
            if (keymetaAccessor != null) {
              try {
                keymetaAccessor.addKey(keyData);
              } catch (IOException e) {
                LOG.warn("Failed to add key to KeymetaTableAccessor for metadata: {}", metadata, e);
              }
            }
            return keyData;
          }
        } catch (Exception e) {
          LOG.warn("Failed to load key from provider for metadata: {}", metadata, e);
        }
      }

      LOG.info("Failed to get key data with metadata: {} for prefix: {}",
        metadata, ManagedKeyProvider.encodeToStr(key_cust));
      return null;
    });
  }

  /**
   * Retrieves a key from the active keys cache using 2-level lookup.
   *
   * @param key_cust the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata the key metadata
   * @return the ManagedKeyData if found, null otherwise
   */
  private ManagedKeyData getFromActiveKeysCache(byte[] key_cust, String keyNamespace, String keyMetadata) {
    CacheKey cacheKey = new CacheKey(key_cust, keyNamespace);
    List<ManagedKeyData> keyList = activeKeysCache.getIfPresent(cacheKey);
    if (keyList == null) {
      return null;
    }

    for (ManagedKeyData keyData : keyList) {
      if (keyData.getKeyMetadata().equals(keyMetadata)) {
        return keyData;
      }
    }
    return null;
  }

  /**
   * Removes an entry from the cache based on its key metadata.
   * Removes from both the main cache and the active keys cache.
   *
   * @param keyMetadata the key metadata of the entry to be removed
   * @return the removed ManagedKeyData entry, or null if not found
   */
  public ManagedKeyData removeEntry(String keyMetadata) {
    ManagedKeyData removedEntry = cache.asMap().remove(keyMetadata);

    // Also remove from active keys cache if present
      if (removedEntry != null) {
      CacheKey cacheKey = new CacheKey(removedEntry.getKeyCustodian(), removedEntry.getKeyNamespace());
      List<ManagedKeyData> keyList = activeKeysCache.getIfPresent(cacheKey);
      if (keyList != null) {
        keyList.removeIf(keyData -> keyData.getKeyMetadata().equals(keyMetadata));
        // If the list is now empty, remove the entire cache entry
        if (keyList.isEmpty()) {
          activeKeysCache.invalidate(cacheKey);
        }
      }
    }

    return removedEntry;
  }

  /**
   * @return the approximate number of entries across both caches (main cache + active keys cache).
   * This is an estimate and may include some double-counting if entries exist in both caches.
   */
  public int getEntryCount() {
    int mainCacheCount = (int) cache.estimatedSize();
    int activeCacheCount = 0;

    // Count entries in active keys cache
    for (List<ManagedKeyData> keyList : activeKeysCache.asMap().values()) {
      activeCacheCount += keyList.size();
    }

    return mainCacheCount + activeCacheCount;
  }

  /**
   * Adds an entry to the cache directly. This method is primarily for testing purposes.
   *
   * @param keyData the ManagedKeyData entry to be added
   */
  public void addEntryForTesting(ManagedKeyData keyData) {
    cache.put(keyData.getKeyMetadata(), keyData);
  }

  /**
   * Retrieves a random entry from the cache based on its key custodian, key namespace, and filters
   * out entries with a status other than ACTIVE. This method also loads active keys from provider
   * if not found in cache.
   *
   * @param key_cust     The key custodian.
   * @param keyNamespace the key namespace to search for
   * @return a random ManagedKeyData entry with the given custodian and ACTIVE status, or null if
   *   not found
   */
  public ManagedKeyData getRandomEntry(byte[] key_cust, String keyNamespace) {
    CacheKey cacheKey = new CacheKey(key_cust, keyNamespace);

    List<ManagedKeyData> keyList = activeKeysCache.get(cacheKey, key -> {
      // On-demand loading of active keys from KeymetaTableAccessor only
      List<ManagedKeyData> activeEntries = new ArrayList<>();

      // Try to load from KeymetaTableAccessor
      if (keymetaAccessor != null) {
        try {
          List<ManagedKeyData> loadedKeys = keymetaAccessor.getActiveKeys(key_cust, keyNamespace);
          for (ManagedKeyData keyData : loadedKeys) {
            if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
              activeEntries.add(keyData);
          }
        }
        } catch (IOException | KeyException e) {
          LOG.warn("Failed to load active keys from KeymetaTableAccessor for custodian: {} namespace: {}",
            ManagedKeyProvider.encodeToStr(key_cust), keyNamespace, e);
        }
      }

      return activeEntries;
    });

    // Return a random entry from active keys cache only
    if (keyList == null || keyList.isEmpty()) {
        return null;
      }

    return keyList.get((int) (Math.random() * keyList.size()));
  }







  /**
   * Invalidates all entries in the cache.
   */
  public void invalidateAll() {
    cache.invalidateAll();
    activeKeysCache.invalidateAll();
  }


}
