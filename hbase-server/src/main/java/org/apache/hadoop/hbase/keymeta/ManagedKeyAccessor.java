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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.security.KeyException;
import java.util.List;

/**
 * This class provides unified access on top of both {@code ManagedKeyDataCache} (L1) and
 * {@code KeymetaTableAccessor} (L2) to access managed keys. When the getter is called, it first
 * checks if L1 cache has the key, if not, it tries to get the key from L2.
 */
@InterfaceAudience.Private
public class ManagedKeyAccessor extends KeyManagementBase {
  private final ManagedKeyDataCache keyDataCache;
  private final KeymetaTableAccessor keymetaAccessor;

  public ManagedKeyAccessor(KeymetaTableAccessor keymetaAccessor,
      ManagedKeyDataCache keyDataCache) {
    super(keymetaAccessor.getServer());
    this.keymetaAccessor = keymetaAccessor;
    this.keyDataCache = keyDataCache;
  }

  /**
   * Get key data by key metadata.
   *
   * @param key_cust     The key custodian.
   * @param keyNamespace The namespace of the key
   * @param keyMetadata The metadata of the key
   * @return The key data or {@code null}
   * @throws IOException if an error occurs while retrieving the key
   */
  public ManagedKeyData getKey(byte[] key_cust, String keyNamespace, String keyMetadata)
      throws IOException, KeyException {
    assertKeyManagementEnabled();
    // 1. Check L1 cache.
    ManagedKeyData keyData = keyDataCache.getEntry(keyMetadata);
    if (keyData == null) {
      // 2. Check L2 cache.
      keyData = keymetaAccessor.getKey(key_cust, keyNamespace, keyMetadata);
      if (keyData == null) {
        // 3. Check with Key Provider.
        ManagedKeyProvider provider = getKeyProvider();
        keyData = provider.unwrapKey(keyMetadata);
        if (keyData != null) {
          LOG.info("Got key data with status: {} and metadata: {} for prefix: {}",
            keyData.getKeyStatus(), keyData.getKeyMetadata(),
            ManagedKeyProvider.encodeToStr(key_cust));
          keymetaAccessor.addKey(keyData);
        }
        else {
          LOG.info("Failed to get key data with metadata: {} for prefix: {}",
            keyMetadata, ManagedKeyProvider.encodeToStr(key_cust));
        }
      }
      if (keyData != null) {
        keyDataCache.addEntry(keyData);
      }
    }
    return keyData;
  }

  /**
   * Get an active key for the given prefix suitable for use in encryption.
   *
   * @param key_cust     The key custodian.
   * @param keyNamespace The namespace of the key
   * @return The key data
   * @throws IOException if an error occurs while retrieving the key
   */
  public ManagedKeyData getAnActiveKey(byte[] key_cust, String keyNamespace)
      throws IOException, KeyException {
    assertKeyManagementEnabled();
    ManagedKeyData keyData = keyDataCache.getRandomEntryForPrefix(key_cust, keyNamespace);
    if (keyData == null) {
      List<ManagedKeyData> activeKeys = keymetaAccessor.getActiveKeys(key_cust, keyNamespace);
      if (! activeKeys.isEmpty()) {
        for (ManagedKeyData kd : activeKeys) {
          keyDataCache.addEntry(kd);
        }
        keyData = keyDataCache.getRandomEntryForPrefix(key_cust, keyNamespace);
      }
    }
    return keyData;
  }
}
