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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.EmptyMsg;

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

    LOG.info("System Key is rotated, initiating cache refresh on all region servers");
    // Get all online region servers
    List<ServerName> regionServers =
      new ArrayList<>(master.getServerManager().getOnlineServersList());

    // Create all futures in parallel
    List<CompletableFuture<EmptyMsg>> futures = new ArrayList<>();
    AdminProtos.RefreshSystemKeyCacheRequest request =
      AdminProtos.RefreshSystemKeyCacheRequest.newBuilder().build();

    for (ServerName serverName : regionServers) {
      LOG.info("Initiating refreshSystemKeyCache on region server: {}", serverName);
      AsyncRegionServerAdmin admin =
        master.getAsyncClusterConnection().getRegionServerAdmin(serverName);
      futures.add(admin.refreshSystemKeyCache(request));
    }

    // Wait for all futures and collect failures
    List<ServerName> failedServers = new ArrayList<>();
    for (int i = 0; i < regionServers.size(); i++) {
      ServerName serverName = regionServers.get(i);
      try {
        FutureUtils.get(futures.get(i));
        LOG.info("refreshSystemKeyCache succeeded on region server: {}", serverName);
      } catch (Exception e) {
        LOG.warn("refreshSystemKeyCache failed on region server: {}", serverName, e);
        failedServers.add(serverName);
      }
    }

    if (!failedServers.isEmpty()) {
      throw new IOException(
        "Failed to initiate System Key cache refresh on region servers: " + failedServers);
    }

    LOG.info("System Key rotation and cache refreshcompleted successfully");
    return true;
  }
}
