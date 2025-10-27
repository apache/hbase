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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class KeymetaAdminImpl extends KeymetaTableAccessor implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminImpl.class);

  public KeymetaAdminImpl(Server server) {
    super(server);
  }

  @Override
  public ManagedKeyData enableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("Trying to enable key management on custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Check if (cust, namespace) pair is already enabled and has an active key.
    ManagedKeyData activeKey = getActiveKey(keyCust, keyNamespace);
    if (activeKey != null) {
      LOG.info(
        "enableManagedKeys: specified (custodian: {}, namespace: {}) already has "
          + "an active managed key with metadata: {}",
        encodedCust, keyNamespace, activeKey.getKeyMetadata());
      return activeKey;
    }

    // Retrieve a single key from provider
    ManagedKeyData retrievedKey =
      KeyManagementUtils.retrieveActiveKey(this, encodedCust, keyCust, keyNamespace, null);
    return retrievedKey;
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    if (LOG.isInfoEnabled()) {
      LOG.info("Getting key statuses for custodian: {} under namespace: {}",
        ManagedKeyProvider.encodeToStr(keyCust), keyNamespace);
    }
    return getAllKeys(keyCust, keyNamespace);
  }

  @Override
  public boolean rotateSTK() throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof MasterServices)) {
      throw new IOException("rotateSTK can only be called on master");
    }
    MasterServices master = (MasterServices) getServer();

    LOG.info("Checking if System Key is rotated");
    boolean rotated = master.rotateSystemKeyIfChanged();

    if (!rotated) {
      LOG.info("No change in System Key is detected");
      return false;
    }

    Set<ServerName> regionServers = master.getServerManager().getOnlineServers().keySet();

    LOG.info("System Key is rotated, initiating cache refresh on all region servers");
    try {
      FutureUtils.get(getAsyncAdmin(master).refreshSystemKeyCacheOnServers(regionServers));
    } catch (Exception e) {
      throw new IOException(
        "Failed to initiate System Key cache refresh on one or more region servers", e);
    }

    LOG.info("System Key rotation and cache refresh completed successfully");
    return true;
  }

  protected AsyncAdmin getAsyncAdmin(MasterServices master) {
    return master.getAsyncClusterConnection().getAdmin();
  }
}
