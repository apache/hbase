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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
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
 * In-memory cache for ManagedKeyData entries, using key metadata as the cache key.  Uses two
 * independent Caffeine caches: one for general key data and one for active keys only with
 * hierarchical structure for efficient random key selection.
 */
@InterfaceAudience.Private
public class ManagedKeyDataCache extends KeyManagementBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKeyDataCache.class);

  private Cache<String, ManagedKeyData> cache;
  private Cache<ActiveKeysCacheKey, List<ManagedKeyData>> activeKeysCache;
  private final KeymetaTableAccessor keymetaAccessor;

  /**
   * Composite key for active keys cache containing custodian and namespace.
   * NOTE: Pair won't work out of the box because it won't work with byte[] as is.
   */
  @InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.UNITTEST })
  public static class ActiveKeysCacheKey {
    private final byte[] custodian;
    private final String namespace;

    public ActiveKeysCacheKey(byte[] custodian, String namespace) {
      this.custodian = custodian;
      this.namespace = namespace;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null || getClass() != obj.getClass())
        return false;
      ActiveKeysCacheKey cacheKey = (ActiveKeysCacheKey) obj;
      return Bytes.equals(custodian, cacheKey.custodian) &&
          Objects.equals(namespace, cacheKey.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Bytes.hashCode(custodian), namespace);
    }
  }

  /**
   * Constructs the ManagedKeyDataCache with the given configuration and keymeta accessor. When
   * keymetaAccessor is null, L2 lookup is disabled and dynamic lookup is enabled.
   *
   * @param conf The configuration, can't be null.
   * @param keymetaAccessor The keymeta accessor, can be null.
   */
  public ManagedKeyDataCache(Configuration conf, KeymetaTableAccessor keymetaAccessor) {
    super(conf);
    this.keymetaAccessor = keymetaAccessor;
    if (keymetaAccessor == null) {
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_ENABLED_CONF_KEY, true);
    }

    int maxEntries = conf.getInt(
        HConstants.CRYPTO_MANAGED_KEYS_L1_CACHE_MAX_ENTRIES_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_L1_CACHE_MAX_ENTRIES_DEFAULT);
    int activeKeysMaxEntries = conf.getInt(
        HConstants.CRYPTO_MANAGED_KEYS_L1_ACTIVE_CACHE_MAX_NS_ENTRIES_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_L1_ACTIVE_CACHE_MAX_NS_ENTRIES_DEFAULT);
    this.cache = Caffeine.newBuilder()
        .maximumSize(maxEntries)
        .build();
    this.activeKeysCache = Caffeine.newBuilder()
        .maximumSize(activeKeysMaxEntries)
        .build();
  }

  /**
   * Retrieves an entry from the cache, loading it from L2 if KeymetaTableAccessor is available.
   * When L2 is not available, it will try to load from provider, unless dynamic lookup is disabled.
   *
   * @param key_cust     the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata  the key metadata of the entry to be retrieved
   * @param wrappedKey   The DEK key material encrypted with the corresponding
   *   KEK, if available.
   * @return the corresponding ManagedKeyData entry, or null if not found
   * @throws IOException  if an error occurs while loading from KeymetaTableAccessor
   * @throws KeyException if an error occurs while loading from KeymetaTableAccessor
   */
  public ManagedKeyData getEntry(byte[] key_cust, String keyNamespace, String keyMetadata, byte[] wrappedKey)
      throws IOException, KeyException {
    ManagedKeyData entry = cache.get(keyMetadata, metadata -> {
      // First check if it's in the active keys cache
      ManagedKeyData keyData = getFromActiveKeysCache(key_cust, keyNamespace, keyMetadata);

      // Try to load from L2
      if (keyData == null && keymetaAccessor != null) {
        try {
          keyData = keymetaAccessor.getKey(key_cust, keyNamespace, metadata);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load key from KeymetaTableAccessor for metadata: {}", metadata, e);
        }
      }

      // If not found in L2 and dynamic lookup is enabled, try with Key Provider
      if (keyData == null && isDynamicLookupEnabled()) {
        try {
          ManagedKeyProvider provider = getKeyProvider();
          keyData = provider.unwrapKey(metadata, wrappedKey);
          LOG.info("Got key data with status: {} and metadata: {} for prefix: {}",
              keyData.getKeyState(), keyData.getKeyMetadata(),
              ManagedKeyProvider.encodeToStr(key_cust));
          // Add to KeymetaTableAccessor for future L2 lookups
          if (keymetaAccessor != null) {
            try {
              keymetaAccessor.addKey(keyData);
            } catch (IOException | RuntimeException e) {
              LOG.warn("Failed to add key to KeymetaTableAccessor for metadata: {}", metadata, e);
            }
          }
        } catch (IOException | RuntimeException e) {
          LOG.warn("Failed to load key from provider for metadata: {}", metadata, e);
        }
      }

      if (keyData == null) {
        keyData = new ManagedKeyData(key_cust, keyNamespace, null, ManagedKeyState.FAILED, keyMetadata);
      }

      if (ManagedKeyState.isUsable(keyData.getKeyState())) {
        LOG.info("Failed to get usable key data with metadata: {} for prefix: {}",
            metadata, ManagedKeyProvider.encodeToStr(key_cust));
      }
      return keyData;
    });
    if (ManagedKeyState.isUsable(entry.getKeyState())) {
      return entry;
    }
    return null;
  }

  /**
   * Retrieves an existing key from the active keys.
   *
   * @param key_cust     the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata  the key metadata
   * @return the ManagedKeyData if found, null otherwise
   */
  private ManagedKeyData getFromActiveKeysCache(byte[] key_cust, String keyNamespace, String keyMetadata) {
    ActiveKeysCacheKey cacheKey = new ActiveKeysCacheKey(key_cust, keyNamespace);
    List<ManagedKeyData> keyList = activeKeysCache.getIfPresent(cacheKey);
    if (keyList != null) {
      for (ManagedKeyData keyData : keyList) {
        if (keyData.getKeyMetadata().equals(keyMetadata)) {
          return keyData;
        }
      }
    }
    return null;
  }

  /**
   * Removes an entry from generic cache based on its key metadata.
   *
   * @param keyMetadata the key metadata of the entry to be removed
   * @return the removed ManagedKeyData entry, or null if not found
   */
  public ManagedKeyData removeEntry(String keyMetadata) {
    return cache.asMap().remove(keyMetadata);
  }

  public ManagedKeyData removeFromActiveKeys(byte[] key_cust, String key_namespace,
      String keyMetadata) {
    ActiveKeysCacheKey cacheKey = new ActiveKeysCacheKey(key_cust, key_namespace);
    List<ManagedKeyData> keyList = activeKeysCache.getIfPresent(cacheKey);
    if (keyList != null) {
      // Find and remove the matching key
      ManagedKeyData removedEntry = null;
      for (int i = 0; i < keyList.size(); i++) {
        if (keyList.get(i).getKeyMetadata().equals(keyMetadata)) {
          removedEntry = keyList.remove(i);
          break;
        }
      }
      // If the list is now empty, remove the entire cache entry
      if (keyList.isEmpty()) {
        activeKeysCache.invalidate(cacheKey);
      }
      return removedEntry;
    }
    return null;
  }

  /**
   * @return the approximate number of entries in the main cache which is meant for general lookup
   * by key metadata.
   */
  public int getGenericCacheEntryCount() {
    return (int) cache.estimatedSize();
  }

  /**
   * @return the approximate number of entries in the active keys cache which is meant for random
   * key selection.
   */
  public int getActiveCacheEntryCount() {
    int activeCacheCount = 0;
    for (List<ManagedKeyData> keyList : activeKeysCache.asMap().values()) {
      activeCacheCount += keyList.size();
    }
    return activeCacheCount;
  }

  /**
   * Retrieves a random active entry from the cache based on its key custodian, key namespace, and
   * filters out entries with a status other than ACTIVE. This method also loads active keys from
   * provider if not found in cache.
   *
   * @param key_cust     The key custodian.
   * @param keyNamespace the key namespace to search for
   * @return a random ManagedKeyData entry with the given custodian and ACTIVE status, or null if
   *   not found
   */
  public ManagedKeyData getAnActiveEntry(byte[] key_cust, String keyNamespace) {
    ActiveKeysCacheKey cacheKey = new ActiveKeysCacheKey(key_cust, keyNamespace);

    List<ManagedKeyData> keyList = activeKeysCache.get(cacheKey, key -> {
      List<ManagedKeyData> activeEntries = new ArrayList<>();

      // Try to load from KeymetaTableAccessor
      if (keymetaAccessor != null) {
        try {
          List<ManagedKeyData> loadedKeys = keymetaAccessor.getActiveKeys(key_cust, keyNamespace);
          activeEntries.addAll(loadedKeys);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load active keys from KeymetaTableAccessor for custodian: {} namespace: {}",
              ManagedKeyProvider.encodeToStr(key_cust), keyNamespace, e);
        }
      }

      // If this happens, it means there were no keys in L2, which shouldn't happpen if L2 is
      // enabled and keys were injected using control path for this custodian and namespace. In
      // this case, we need to retrieve the keys from provider, but before that as a quick
      // optimization, we check if there are any active keys in the other cache, which should be
      // suitable for standalone tools.
      if (activeEntries.isEmpty()) {
        this.cache.asMap().values().stream()
            .filter(keyData -> Bytes.equals(keyData.getKeyCustodian(), key_cust)
                && keyData.getKeyNamespace().equals(keyNamespace)
                && keyData.getKeyState() == ManagedKeyState.ACTIVE)
            .forEach(keyData -> {
              activeEntries.add(keyData);
            });
      }

      // As a last ditch effort, load active keys from provider. This typically happens for
      // standalone tools.
      if (activeEntries.isEmpty() && isDynamicLookupEnabled()) {
        try {
          String keyCust = ManagedKeyProvider.encodeToStr(key_cust);
          Set<ManagedKeyData> retrievedKeys = retrieveManagedKeys(keyCust, key_cust, keyNamespace,
              getPerCustodianNamespaceActiveKeyConfCount(), new HashSet<>());
          if (keymetaAccessor != null) {
            for (ManagedKeyData keyData : retrievedKeys) {
              keymetaAccessor.addKey(keyData);
            }
          }
          retrievedKeys.stream().filter(keyData -> keyData.getKeyState() == ManagedKeyState.ACTIVE)
              .forEach(activeEntries::add);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load active keys from provider for custodian: {} namespace: {}",
              ManagedKeyProvider.encodeToStr(key_cust), keyNamespace, e);
        }
      }

      // We don't mind returning an empty list here because it will help prevent future L2/provider
      // lookups.
      return activeEntries;
    });

    // Return a random entry from active keys cache only
    if (keyList.isEmpty()) {
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
