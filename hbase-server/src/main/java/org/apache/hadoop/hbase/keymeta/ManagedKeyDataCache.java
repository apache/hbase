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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * In-memory cache for ManagedKeyData entries, using key metadata hash as the cache key. Uses two
 * independent Caffeine caches: one for general key data and one for active keys only with
 * hierarchical structure for efficient single key retrieval.
 * <p>
 * Cache keys are {@link ManagedKeyIdentity}; any implementation (e.g.
 * {@link KeyIdentityBytesBacked}, {@link KeyIdentitySingleArrayBacked}) is supported and
 * interoperable—lookups match entries regardless of concrete type because equality and hashCode are
 * content-based (see {@link ManagedKeyIdentity#contentEquals contentEquals}/
 * {@link ManagedKeyIdentity#contentHashCode contentHashCode}).
 */
@InterfaceAudience.Private
public class ManagedKeyDataCache extends KeyManagementBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKeyDataCache.class);

  private Cache<ManagedKeyIdentity, ManagedKeyData> cacheByIdentity;
  private Cache<ManagedKeyIdentity, ManagedKeyData> activeKeysCache;
  private final KeymetaTableAccessor keymetaAccessor;

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
    this.cacheByIdentity = Caffeine.newBuilder().maximumSize(maxEntries).build();
    this.activeKeysCache = Caffeine.newBuilder().maximumSize(activeKeysMaxEntries).build();
  }

  /**
   * Retrieves an entry from the cache, loading from L2 or provider if not found. Exactly one of
   * partialIdentity or keyMetadata must be non-null. When keyMetadata is null, the provider is not
   * consulted and resolution is from cache and L2 only. When no entry is found and keyMetadata is
   * null, this method returns null and does not cache a placeholder so a future call can retry.
   * <p>
   * Uses Caffeine's get() so that the result is always cached in cacheByMetadataHash, whether the
   * entry was found in the active-key cache, L2, or provider.
   * @param fullKeyIdentity the full key identity
   * @param keyMetadata     the key metadata string, or null if partialIdentity is provided
   * @param wrappedKey      the DEK key material encrypted with the corresponding KEK, if available
   * @return the corresponding ManagedKeyData entry, or null if not found
   * @throws IOException  if an error occurs while loading from KeymetaTableAccessor
   * @throws KeyException if an error occurs while loading from KeymetaTableAccessor
   */
  public ManagedKeyData getEntry(ManagedKeyIdentity fullKeyIdentity, String keyMetadata,
    byte[] wrappedKey) throws IOException, KeyException {
    Preconditions.checkArgument(
      fullKeyIdentity.getPartialIdentityLength() > 0 || keyMetadata != null,
      "Exactly one of partialIdentity or keyMetadata must be non-empty");

    if (fullKeyIdentity.getPartialIdentityLength() <= 0) {
      fullKeyIdentity = ManagedKeyIdentityUtils.fullKeyIdentityFromMetadata(
        fullKeyIdentity.getCustodianView(), fullKeyIdentity.getNamespaceView(), keyMetadata);
    }

    ManagedKeyData entry = cacheByIdentity.getIfPresent(fullKeyIdentity);
    if (entry != null) {
      // Treat FAILED as "not found" so callers get null when L2 failed or key was missing
      return entry.getKeyState() == ManagedKeyState.FAILED ? null : entry;
    }

    // Technically we don't need to clone the fullKeyIdentity as all existing execution paths ensure
    // that the underlying byte[] are not reused, but doing so avoids hard to track
    // bugs, in case there are some new code paths not following this practice.
    // However, since we do lazy cloning, it avoids most of the extra cost.
    fullKeyIdentity = fullKeyIdentity.clone();
    entry = cacheByIdentity.get(fullKeyIdentity, keyIdentity -> {
      // Try active keys cache first (same as L2/provider path so result is cached via get())
      ManagedKeyData keyData = getFromActiveKeysCache(keyIdentity);
      if (keyData != null) {
        return keyData;
      }

      // L2 + provider
      if (keymetaAccessor != null) {
        try {
          keyData = keymetaAccessor.getKey(keyIdentity);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn(
            "Failed to load key from L2 for (custodian: {}, namespace: {}) with partial identity: {}",
            keyIdentity.getCustodianEncoded(), keyIdentity.getNamespaceString(),
            keyIdentity.getPartialIdentityEncoded(), e);
        }
      }

      if (keyData == null && isDynamicLookupEnabled() && keyMetadata != null) {
        try {
          keyData = KeyManagementUtils.retrieveKey(getKeyProvider(), keymetaAccessor, keyIdentity,
            keyMetadata, wrappedKey);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn(
            "Failed to retrieve key from provider for (custodian: {}, namespace: {}) with "
              + "metadata hash: {}",
            keyIdentity.getCustodianEncoded(), keyIdentity.getNamespaceString(),
            keyIdentity.getPartialIdentityEncoded(), e);
        }
      }

      if (keyData == null) {
        if (keyMetadata == null) {
          return null;
        }
        keyData = new ManagedKeyData(keyIdentity, null, ManagedKeyState.FAILED, keyMetadata);
      }

      // Also update activeKeysCache if relevant and is missing.
      if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
        activeKeysCache.asMap().putIfAbsent(new KeyIdentityPrefixBytesBacked(
          keyIdentity.getCustodianView(), keyIdentity.getNamespaceView()), keyData);
      }
      return keyData;
    });
    if (entry != null && ManagedKeyState.isUsable(entry.getKeyState())) {
      return entry;
    }
    return null;
  }

  /**
   * Retrieves an existing key from the active keys cache.
   * @param keyCust         the key custodian
   * @param keyNamespace    the key namespace
   * @param partialIdentity the partial identity (digest of key metadata)
   * @return the ManagedKeyData if found, null otherwise
   */
  private ManagedKeyData getFromActiveKeysCache(ManagedKeyIdentity fullKeyIdentity) {
    KeyIdentityPrefixBytesBacked cacheKey = new KeyIdentityPrefixBytesBacked(
      fullKeyIdentity.getCustodianView(), fullKeyIdentity.getNamespaceView());
    ManagedKeyData keyData = activeKeysCache.getIfPresent(cacheKey);
    if (
      keyData != null
        && fullKeyIdentity.getPartialIdentityView().equals(keyData.getPartialIdentity())
    ) {
      return keyData;
    }
    return null;
  }

  /**
   * Ejects the key with specified full key identity from all the caches.  Ejects from the active
   * keys cache if a key exists for the specified prefix and the full identity matches.
   * @param fullKeyIdentity the full key identity
   * @return true if the key was ejected from the active keys cache, false otherwise
   */
  public boolean ejectKey(ManagedKeyIdentity fullKeyIdentity) {
    KeyIdentityPrefixBytesBacked cacheKey = new KeyIdentityPrefixBytesBacked(
      fullKeyIdentity.getCustodianView(), fullKeyIdentity.getNamespaceView());
    AtomicBoolean ejected = new AtomicBoolean(false);

    // Eject from active keys cache only when partial identity matches.
    // Custodian and namespace are already matched by computeIfPresent(cacheKey, ...).
    activeKeysCache.asMap().computeIfPresent(cacheKey, (key, value) -> {
      if (fullKeyIdentity.comparePartialIdentity(value.getPartialIdentity()) == 0) {
        ejected.set(true);
        return null;
      }
      return value;
    });

    // Also remove from generic cache by partial identity, with collision check
    cacheByIdentity.asMap().computeIfPresent(fullKeyIdentity, (hash, value) -> {
      ejected.set(true);
      return null;
    });

    return ejected.get();
  }

  /**
   * Clear all the cached entries.
   */
  public void clearCache() {
    cacheByIdentity.invalidateAll();
    activeKeysCache.invalidateAll();
  }

  /**
   * @return the approximate number of entries in the main cache which is meant for general lookup
   *         by key metadata hash.
   */
  public int getGenericCacheEntryCount() {
    return (int) cacheByIdentity.estimatedSize();
  }

  /** Returns the approximate number of entries in the active keys cache */
  public int getActiveCacheEntryCount() {
    return (int) activeKeysCache.estimatedSize();
  }

  /**
   * Retrieves the active entry from the cache based on its key custodian and key namespace. This
   * method also loads active keys from provider if not found in cache.
   * @param keyCust      The key custodian.
   * @param keyNamespace the key namespace to search for (as Bytes)
   * @return the ManagedKeyData entry with the given custodian and ACTIVE status, or null if not
   *         found
   */
  public ManagedKeyData getActiveEntry(final Bytes keyCust, final Bytes keyNamespace) {
    ManagedKeyIdentity cacheKey = new KeyIdentityPrefixBytesBacked(keyCust, keyNamespace);
    ManagedKeyData keyData = activeKeysCache.getIfPresent(cacheKey);
    if (keyData != null && keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      return keyData;
    }

    // Doing lazy cloning of the key custodian and namespace for long-term storage.
    cacheKey = new KeyIdentityPrefixBytesBacked(keyCust.clone(), keyNamespace.clone());

    keyData = activeKeysCache.get(cacheKey, key -> {
      ManagedKeyData retrievedKey = null;

      // Try to load from KeymetaTableAccessor if not found in cache
      if (keymetaAccessor != null) {
        try {
          retrievedKey = keymetaAccessor.getKeyManagementStateMarker(key);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load active key from KeymetaTableAccessor for custodian: {} "
            + "namespace: {}", key.getCustodianEncoded(), key.getNamespaceString(), e);
        }
      }

      // As a last ditch effort, load active key from provider. This typically happens for
      // standalone tools.
      if (retrievedKey == null && isDynamicLookupEnabled()) {
        try {
          retrievedKey = KeyManagementUtils.retrieveActiveKey(getKeyProvider(), keymetaAccessor,
            key.getCustodianEncoded(), key, null);
        } catch (IOException | KeyException | RuntimeException e) {
          LOG.warn("Failed to load active key from provider for custodian: {} namespace: {}",
            key.getCustodianEncoded(), key.getNamespaceString(), e);
        }
      }

      if (retrievedKey == null) {
        retrievedKey = new ManagedKeyData(key, ManagedKeyState.FAILED);
      }

      return retrievedKey;
    });

    // This should never be null, but adding a check just to satisfy spotbugs.
    if (keyData != null && keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      return keyData;
    }
    return null;
  }
}
