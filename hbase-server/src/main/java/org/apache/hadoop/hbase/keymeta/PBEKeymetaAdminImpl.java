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
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.security.KeyException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Private
public class PBEKeymetaAdminImpl extends PBEKeymetaTableAccessor implements PBEKeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(PBEKeymetaAdminImpl.class);

  public PBEKeymetaAdminImpl(Server server) {
    super(server);
  }

  @Override
  public PBEKeyStatus enablePBE(String pbePrefix, String keyNamespace) throws IOException {
    checkPBEEnabled();
    LOG.info("Trying to enable PBE on key: {} under namespace: {}", pbePrefix, keyNamespace);
    byte[] pbe_prefix = PBEKeyProvider.decodeToPrefixBytes(pbePrefix);
    PBEKeyProvider provider = getKeyProvider();
    int perPrefixActiveKeyConfCount = getPerPrefixActiveKeyConfCount();
    Set<PBEKeyData> retrievedKeys = new HashSet<>(perPrefixActiveKeyConfCount);
    PBEKeyData pbeKey = null;
    for (int i = 0; i < perPrefixActiveKeyConfCount; ++i) {
      pbeKey = provider.getPBEKey(pbe_prefix, keyNamespace);
      if (pbeKey == null) {
        throw new IOException("Invalid null PBE key received from key provider");
      }
      if (retrievedKeys.contains(pbeKey)) {
        // This typically means, the key provider is not capable of producing multiple active keys.
        LOG.info("enablePBE: configured key count per prefix: " + perPrefixActiveKeyConfCount +
          " but received only: " + retrievedKeys.size() + " unique keys.");
        break;
      }
      retrievedKeys.add(pbeKey);
      LOG.info("enablePBE: got key data with status: {} and metadata: {} for prefix: {}",
        pbeKey.getKeyStatus(), pbeKey.getKeyMetadata(), pbePrefix);
      addKey(pbeKey);
    }
    // pbeKey can't be null at this point as perPrefixActiveKeyConfCount will always be > 0,
    // but the null check is needed to avoid any warning.
    return pbeKey == null ? null : pbeKey.getKeyStatus();
  }

  @Override
  public List<PBEKeyData> getPBEKeyStatuses(String pbePrefix, String keyNamespace)
    throws IOException, KeyException {
    checkPBEEnabled();
    LOG.info("Getting key statuses for PBE on key: {} under namespace: {}", pbePrefix,
      keyNamespace);
    byte[] pbe_prefix = PBEKeyProvider.decodeToPrefixBytes(pbePrefix);
    return super.getAllKeys(pbe_prefix, keyNamespace);
  }
}
