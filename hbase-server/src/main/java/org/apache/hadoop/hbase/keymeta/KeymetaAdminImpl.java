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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
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
  public List<ManagedKeyData> enableKeyManagement(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    LOG.info("Trying to enable key management on custodian: {} under namespace: {}", keyCust,
      keyNamespace);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(keyCust);

    // Check if (cust, namespace) pair is already enabled and has an active key.
    ManagedKeyData activeKey = getActiveKey(key_cust, keyNamespace);
    if (activeKey != null) {
      LOG.info(
        "enableManagedKeys: specified (custodian: {}, namespace: {}) already has "
          + "an active managed key with metadata: {}",
        keyCust, keyNamespace, activeKey.getKeyMetadata());
      return Collections.singletonList(activeKey);
    }

    // Retrieve a single key from provider
    ManagedKeyData retrievedKey = retrieveActiveKey(keyCust, key_cust, keyNamespace, this, null);
    return Collections.singletonList(retrievedKey);
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    LOG.info("Getting key statuses for custodian: {} under namespace: {}", keyCust, keyNamespace);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(keyCust);
    return getAllKeys(key_cust, keyNamespace);
  }

  @Override
  public boolean rotateSTK() throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof org.apache.hadoop.hbase.master.MasterServices)) {
      throw new IOException("rotateSTK can only be called on master");
    }
    org.apache.hadoop.hbase.master.MasterServices master =
      (org.apache.hadoop.hbase.master.MasterServices) getServer();

    LOG.info("Checking for System Key rotation");
    org.apache.hadoop.hbase.master.SystemKeyManager systemKeyManager =
      new org.apache.hadoop.hbase.master.SystemKeyManager(master);
    org.apache.hadoop.hbase.io.crypto.ManagedKeyData newKey =
      systemKeyManager.rotateSystemKeyIfChanged();

    if (newKey == null) {
      LOG.info("No change in System Key detected");
      return false;
    }

    LOG.info("New System Key detected, propagating to region servers");
    // Get all online region servers
    java.util.List<org.apache.hadoop.hbase.ServerName> regionServers =
      new java.util.ArrayList<>(master.getServerManager().getOnlineServersList());

    // Call each region server to rebuild its system key cache
    for (org.apache.hadoop.hbase.ServerName serverName : regionServers) {
      try {
        LOG.info("Calling rotateSTK on region server: {}", serverName);
        org.apache.hadoop.hbase.client.AsyncRegionServerAdmin admin =
          master.getAsyncClusterConnection().getRegionServerAdmin(serverName);
        org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RotateSTKRequest request =
          org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RotateSTKRequest
            .newBuilder().build();
        FutureUtils.get(admin.rotateSTK(request));
        LOG.info("Successfully called rotateSTK on region server: {}", serverName);
      } catch (Exception e) {
        LOG.error("Failed to call rotateSTK on region server: {}", serverName, e);
        throw new IOException("Failed to propagate STK rotation to region server: " + serverName,
          e);
      }
    }

    LOG.info("System Key rotation completed successfully");
    return true;
  }
}
