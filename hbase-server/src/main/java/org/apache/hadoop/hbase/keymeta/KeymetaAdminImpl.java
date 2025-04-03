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

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.security.KeyException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Private
public class KeymetaAdminImpl extends KeymetaTableAccessor implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminImpl.class);

  public KeymetaAdminImpl(Server server) {
    super(server);
  }

  @Override
  public ManagedKeyStatus enableManagedKeys(String keyCust, String keyNamespace) throws IOException {
    assertKeyManagementEnabled();
    LOG.info("Trying to enable key management on custodian: {} under namespace: {}", keyCust,
      keyNamespace);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(keyCust);
    ManagedKeyProvider provider = getKeyProvider();
    int perPrefixActiveKeyConfCount = getPerPrefixActiveKeyConfCount();
    Set<ManagedKeyData> retrievedKeys = new HashSet<>(perPrefixActiveKeyConfCount);
    ManagedKeyData pbeKey = null;
    for (int i = 0; i < perPrefixActiveKeyConfCount; ++i) {
      pbeKey = provider.getManagedKey(key_cust, keyNamespace);
      if (pbeKey == null) {
        throw new IOException("Invalid null managed key received from key provider");
      }
      if (retrievedKeys.contains(pbeKey)) {
        // This typically means, the key provider is not capable of producing multiple active keys.
        LOG.info("enableManagedKeys: configured key count per prefix: " +
          perPrefixActiveKeyConfCount + " but received only: " + retrievedKeys.size() +
          " unique keys.");
        break;
      }
      retrievedKeys.add(pbeKey);
      LOG.info("enableManagedKeys: got key data with status: {} and metadata: {} for custodian: {}",
        pbeKey.getKeyStatus(), pbeKey.getKeyMetadata(), keyCust);
      addKey(pbeKey);
    }
    // pbeKey can't be null at this point as perPrefixActiveKeyConfCount will always be > 0,
    // but the null check is needed to avoid any warning.
    return pbeKey == null ? null : pbeKey.getKeyStatus();
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    LOG.info("Getting key statuses for custodian: {} under namespace: {}", keyCust,
      keyNamespace);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(keyCust);
    return super.getAllKeys(key_cust, keyNamespace);
  }
}
