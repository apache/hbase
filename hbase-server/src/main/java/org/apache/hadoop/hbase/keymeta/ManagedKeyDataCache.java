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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.security.KeyException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
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

/**
 * In-memory cache for ManagedKeyData entries, using key metadata as the cache key. Uses two
 * independent Caffeine caches: one for general key data and one for active keys only with
 * hierarchical structure for efficient single key retrieval.
 */
@InterfaceAudience.Private
public class ManagedKeyDataCache extends KeyManagementBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKeyDataCache.class);

  private Cache<String, ManagedKeyData> cacheByMetadata;
  private Cache<ActiveKeysCacheKey, ManagedKeyData> activeKeysCache;
  private final KeymetaTableAccessor keymetaAccessor;

  /**
   * Composite key for active keys cache containing custodian and namespace. NOTE: Pair won't work
   * out of the box because it won't work with byte[] as is.
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
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      ActiveKeysCacheKey cacheKey = (ActiveKeysCacheKey) obj;
      return Bytes.equals(custodian, cacheKey.custodian)
        && Objects.equals(namespace, cacheKey.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Bytes.hashCode(custodian), namespace);
    }
  }

  /**
   * Constructs the ManagedKeyDataCache with the given configuration and keymeta accessor. When
   * keymetaAccessor is null, L2 lookup is disabled and dynamic lookup is enabled.
   * @param conf            The configuration, can't be null.
   * @param keymetaAccessor The keymeta accessor, can be null.
   */
  public ManagedKeyDataCache(Configuration conf, KeymetaTableAccessor keymetaAccessor) {
    super(conf);
    this.keymetaAccessor = keymetaAccessor;
    if (keymetaAccessor == null) {
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_ENABLED_CONF_KEY, true);
    }

    int maxEntries = conf.getInt(HConstants.CRYPTO_MANAGED_KEYS_L1_CACHE_MAX_ENTRIES_CONF_KEY,
      HConstants.CRYPTO_MANAGED_KEYS_L1_CACHE_MAX_ENTRIES_DEFAULT);
    int activeKeysMaxEntries =
      conf.getInt(HConstants.CRYPTO_MANAGED_KEYS_L1_ACTIVE_CACHE_MAX_NS_ENTRIES_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_L1_ACTIVE_CACHE_MAX_NS_ENTRIES_DEFAULT);
    this.cacheByMetadata = Caffeine.newBuilder().maximumSize(maxEntries).build();
    this.activeKeysCache = Caffeine.newBuilder().maximumSize(activeKeysMaxEntries).build();
  }

  /**
   * Retrieves an entry from the cache, loading it from L2 if KeymetaTableAccessor is available.
   * When L2 is not available, it will try to load from provider, unless dynamic lookup is disabled.
   * @param keyCust      the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata  the key metadata of the entry to be retrieved
   * @param wrappedKey   The DEK key material encrypted with the corresponding KEK, if available.
   * @return the corresponding ManagedKeyData entry, or null if not found
   * @throws IOException  if an error occurs while loading from KeymetaTableAccessor
   * @throws KeyException if an error occurs while loading from KeymetaTableAccessor
   */
  public ManagedKeyData getEntry(byte[] keyCust, String keyNamespace, String keyMetadata,
    byte[] wrappedKey) throws IOException, KeyException {
    ManagedKeyData entry = cacheByMetadata.get(keyMetadata, metadata -> {
      // First check if it's in the active keys cache
      ManagedKeyData keyData = getFromActiveKeysCache(keyCust, keyNamespace, keyMetadata);

      // Try to load from L2
      if (keyData == null && keymetaAccessor != null) {
        try {
          keyData = keymetaAccessor.getKey(keyCust, keyNamespace, metadata);
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
            ManagedKeyProvider.encodeToStr(keyCust));
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
        keyData =
          new ManagedKeyData(keyCust, keyNamespace, null, ManagedKeyState.FAILED, keyMetadata);
      }

      // Also update activeKeysCache if relevant and is missing.
      if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
        activeKeysCache.asMap().putIfAbsent(new ActiveKeysCacheKey(keyCust, keyNamespace), keyData);
      }

      if (!ManagedKeyState.isUsable(keyData.getKeyState())) {
        LOG.info("Failed to get usable key data with metadata: {} for prefix: {}", metadata,
          ManagedKeyProvider.encodeToStr(keyCust));
      }
      return keyData;
    });
    // This should never be null, but adding a check just to satisfy spotbugs.
    if (entry != null && ManagedKeyState.isUsable(entry.getKeyState())) {
      return entry;
    }
    return null;
  }

  /**
   * Retrieves an existing key from the active keys cache.
   * @param keyCust      the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata  the key metadata
   * @return the ManagedKeyData if found, null otherwise
   */
  private ManagedKeyData getFromActiveKeysCache(byte[] keyCust, String keyNamespace,
    String keyMetadata) {
    ActiveKeysCacheKey cacheKey = new ActiveKeysCacheKey(keyCust, keyNamespace);
    ManagedKeyData keyData = activeKeysCache.getIfPresent(cacheKey);
    if (keyData != null && keyData.getKeyMetadata().equals(keyMetadata)) {
      return keyData;
    }
    return null;
  }

  /**
   * Eject the key identified by the given custodian, namespace and metadata hash from the active
   * keys cache.
   * @param keyCust         the key custodian
   * @param keyNamespace    the key namespace
   * @param keyMetadataHash the hash of the key metadata
   */
  public boolean ejectKeyFromActiveKeysCache(byte[] keyCust, String keyNamespace,
    byte[] keyMetadataHash) {
    ActiveKeysCacheKey cacheKey = new ActiveKeysCacheKey(keyCust, keyNamespace);
    AtomicBoolean ejected = new AtomicBoolean(false);
    activeKeysCache.asMap().computeIfPresent(cacheKey, (key, value) -> {
      if (Bytes.equals(value.getKeyMetadataHash(), keyMetadataHash)) {
        ejected.set(true);
        return null;
      }
      return value;
    });
    return ejected.get();
  }

  /**
   * Clear all the cached entries.
   */
  public void clearCache() {
    cacheByMetadata.invalidateAll();
    activeKeysCache.invalidateAll();
  }

  /**
   * @return the approximate number of entries in the main cache which is meant for general lookup
   *         by key metadata.
   */
  public int getGenericCacheEntryCount() {
    return (int) cacheByMetadata.estimatedSize();
  }

  /** Returns the approximate number of entries in the active keys cache */
  public int getActiveCacheEntryCount() {
    return (int) activeKeysCache.estimatedSize();
  }

  /**
   * Retrieves the active entry from the cache based on its key custodian and key namespace. This
   * method also loads active keys from provider if not found in cache.
   * @param keyCust      The key custodian.
   * @param keyNamespace the key namespace to search for
   * @return the ManagedKeyData entry with the given custodian and ACTIVE status, or null if not
   *         found
   */
  public ManagedKeyData getActiveEntry(byte[] keyCust, String keyNamespace) {
    ActiveKeysCacheKey cacheKey = new ActiveKeysCacheKey(keyCust, keyNamespace);

    ManagedKeyData keyData = activeKeysCache.get(cacheKey, key -> {
      ManagedKeyData retrievedKey = null;

      // Try to load from KeymetaTableAccessor if not found in cache
      if (keymetaAccessor != null) {
        try {
          retrievedKey = keymetaAccessor.getActiveKey(keyCust, keyNamespace);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load active key from KeymetaTableAccessor for custodian: {} "
            + "namespace: {}", ManagedKeyProvider.encodeToStr(keyCust), keyNamespace, e);
        }
      }

      // As a last ditch effort, load active key from provider. This typically happens for
      // standalone tools.
      if (retrievedKey == null && isDynamicLookupEnabled()) {
        try {
          String keyCustEnc = ManagedKeyProvider.encodeToStr(keyCust);
          retrievedKey = KeyManagementUtils.retrieveActiveKey(getKeyProvider(), keymetaAccessor,
            keyCustEnc, keyCust, keyNamespace, null);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load active key from provider for custodian: {} namespace: {}",
            ManagedKeyProvider.encodeToStr(keyCust), keyNamespace, e);
        }
      }

      if (retrievedKey == null) {
        retrievedKey =
          new ManagedKeyData(keyCust, keyNamespace, null, ManagedKeyState.FAILED, null);
      }

      return retrievedKey;
    });

    // This should never be null, but adding a check just to satisfy spotbugs.
    if (keyData != null && keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      return keyData;
    }
    return null;
  }

  /**
   * Invalidates all entries in the cache.
   */
  public void invalidateAll() {
    cacheByMetadata.invalidateAll();
    activeKeysCache.invalidateAll();
  }
}
