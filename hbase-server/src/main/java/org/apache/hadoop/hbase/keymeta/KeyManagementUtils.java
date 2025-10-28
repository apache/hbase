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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class KeyManagementUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KeyManagementUtils.class);

  /**
   * Utility function to retrieves a managed key from the key provider. If an existing key is
   * provided and the retrieved key is the same as the existing key, it will be ignored.
   * @param provider          the managed key provider
   * @param accessor          the accessor to use to persist the key. If null, the key will not be
   *                          persisted.
   * @param encKeyCust        the encoded key custodian
   * @param key_cust          the key custodian
   * @param keyNamespace      the key namespace
   * @param existingActiveKey the existing key, typically the active key already retrieved from the
   *                          key provider, can be null.
   * @return the retrieved key, or null if no key could be retrieved
   * @throws IOException  if an error occurs
   * @throws KeyException if an error occurs
   */
  public static ManagedKeyData retrieveActiveKey(ManagedKeyProvider provider,
    KeymetaTableAccessor accessor, String encKeyCust, byte[] key_cust, String keyNamespace,
    ManagedKeyData existingActiveKey) throws IOException, KeyException {
    ManagedKeyData pbeKey = provider.getManagedKey(key_cust, keyNamespace);
    if (pbeKey == null) {
      throw new IOException("Invalid null managed key received from key provider");
    }
    /*
     * Will be useful when refresh API is implemented. if (existingActiveKey != null &&
     * existingActiveKey.equals(pbeKey)) {
     * LOG.info("retrieveActiveKey: no change in key for (custodian: {}, namespace: {}", encKeyCust,
     * keyNamespace); return null; } // TODO: If existingActiveKey is not null, we should update the
     * key state to INACTIVE.
     */
    LOG.info(
      "retrieveActiveKey: got active key with status: {} and metadata: {} for "
        + "(custodian: {}, namespace: {})",
      pbeKey.getKeyState(), pbeKey.getKeyMetadata(), encKeyCust, pbeKey.getKeyNamespace());
    if (accessor != null) {
      accessor.addKey(pbeKey);
    }
    return pbeKey;
  }
}
