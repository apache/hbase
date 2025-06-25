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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;

@InterfaceAudience.Private
public class KeymetaAdminImpl extends KeymetaTableAccessor implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminImpl.class);

  public KeymetaAdminImpl(Server server) {
    super(server);
  }

  @Override
  public List<ManagedKeyData> enableKeyManagement(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    LOG.info("Trying to enable key management on custodian: {} under namespace: {}", keyCust,
      keyNamespace);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(keyCust);
    int perCustNamespaceActiveKeyConfCount = getPerCustodianNamespaceActiveKeyConfCount();

    // Check if (cust, namespace) pair is already enabled and if there are enough number of
    // active keys.
    List<ManagedKeyData> activeKeys = getActiveKeys(key_cust, keyNamespace);
    if (activeKeys.size() >= perCustNamespaceActiveKeyConfCount) {
      LOG.info("enableManagedKeys: specified (custodian: {}, namespace: {}) already has "
          + " {} number of  managed keys active, which satisfies the configured minimum: {}",
        keyCust, keyNamespace, activeKeys.size(), perCustNamespaceActiveKeyConfCount);
      return activeKeys;
    }

    Set<ManagedKeyData> existingKeys = new HashSet<>(activeKeys);
    int nKeysToRetrieve = perCustNamespaceActiveKeyConfCount - activeKeys.size();
    Set<ManagedKeyData> retrievedKeys = new HashSet<>(nKeysToRetrieve);
    ManagedKeyProvider provider = getKeyProvider();
    for (int i = 0; i < nKeysToRetrieve; ++i) {
      ManagedKeyData pbeKey = provider.getManagedKey(key_cust, keyNamespace);
      if (pbeKey == null) {
        throw new IOException("Invalid null managed key received from key provider");
      }
      if (retrievedKeys.contains(pbeKey) || existingKeys.contains(pbeKey)) {
        // This typically means, the key provider is not capable of producing multiple active keys.
        LOG.info("enableManagedKeys: specified (custodian: {}, namespace: {}) is configured "
            + " to have {} active keys, but received only {}  unique keys.",
          keyCust, keyNamespace, perCustNamespaceActiveKeyConfCount,
          activeKeys.size() + retrievedKeys.size());
        break;
      }
      retrievedKeys.add(pbeKey);
      LOG.info("enableManagedKeys: got managed key with status: {} and metadata: {} for "
          + "(custodian: {}, namespace: {})", pbeKey.getKeyState(), pbeKey.getKeyMetadata(),
        keyCust, keyNamespace);
      addKey(pbeKey);
      if (pbeKey.getKeyState() != ManagedKeyState.ACTIVE) {
        LOG.info("enableManagedKeys: received non-ACTIVE key with status: {} with metadata: {} for "
            + "(custodian: {}, namespace: {})",
          pbeKey.getKeyState(), pbeKey.getKeyMetadata(), keyCust, keyNamespace);
        break;
      }
    }
    return retrievedKeys.stream().toList();
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    LOG.info("Getting key statuses for custodian: {} under namespace: {}", keyCust,
      keyNamespace);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(keyCust);
    return getAllKeys(key_cust, keyNamespace);
  }
}
