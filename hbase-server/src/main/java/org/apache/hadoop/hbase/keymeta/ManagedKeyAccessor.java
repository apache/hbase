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
 * This class provides unified access on top of both {@code PBEKeyDataCache} (L1) and
 * {@code KeymetaTableAccessor} (L2) to access PBE keys. When the getter is called, it first
 * checks if L1 cache has the key, if not, it tries to get the key from L2.
 */
@InterfaceAudience.Private
public class ManagedKeyAccessor extends KeyManagementBase {
  private final ManagedKeyDataCache keyDataCache;
  private final KeymetaTableAccessor keymetaAccessor;

  public ManagedKeyAccessor(KeymetaTableAccessor keymetaAccessor) {
    super(keymetaAccessor.server);
    this.keymetaAccessor = keymetaAccessor;
    keyDataCache = new ManagedKeyDataCache();
  }

  /**
   * Get key data by key metadata.
   *
   * @param cust_spec The custodian spec.
   * @param keyNamespace The namespace of the key
   * @param keyMetadata The metadata of the key
   * @return The key data or {@code null}
   * @throws IOException if an error occurs while retrieving the key
   */
  public ManagedKeyData getKey(byte[] cust_spec, String keyNamespace, String keyMetadata)
      throws IOException, KeyException {
    checkPBEEnabled();
    // 1. Check L1 cache.
    ManagedKeyData keyData = keyDataCache.getEntry(keyMetadata);
    if (keyData == null) {
      // 2. Check L2 cache.
      keyData = keymetaAccessor.getKey(cust_spec, keyNamespace, keyMetadata);
      if (keyData == null) {
        // 3. Check with Key Provider.
        ManagedKeyProvider provider = getKeyProvider();
        keyData = provider.unwrapKey(keyMetadata);
        LOG.info("Got key data with status: {} and metadata: {} for prefix: {}",
          keyData.getKeyStatus(), keyData.getKeyMetadata(),
          ManagedKeyProvider.encodeToStr(cust_spec));
        keymetaAccessor.addKey(keyData);
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
   * @param cust_spec The custodian specification
   * @param keyNamespace The namespace of the key
   * @return The key data
   * @throws IOException if an error occurs while retrieving the key
   */
  public ManagedKeyData getAnActiveKey(byte[] cust_spec, String keyNamespace)
    throws IOException, KeyException {
    checkPBEEnabled();
    ManagedKeyData keyData = keyDataCache.getRandomEntryForPrefix(cust_spec, keyNamespace);
    if (keyData == null) {
      List<ManagedKeyData> activeKeys = keymetaAccessor.getActiveKeys(cust_spec, keyNamespace);
      for (ManagedKeyData kd: activeKeys) {
        keyDataCache.addEntry(kd);
      }
      keyData = keyDataCache.getRandomEntryForPrefix(cust_spec, keyNamespace);
    }
    return keyData;
  }
}
