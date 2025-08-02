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
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysRequest;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@InterfaceAudience.Public
public class KeymetaAdminClient implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminClient.class);
  private ManagedKeysProtos.ManagedKeysService.BlockingInterface stub;

  public KeymetaAdminClient(Connection conn) throws IOException {
    this.stub = ManagedKeysProtos.ManagedKeysService.newBlockingStub(
        conn.getAdmin().coprocessorService());
  }

  @Override
  public List<ManagedKeyData> enableKeyManagement(String keyCust, String keyNamespace)
      throws IOException {
    try {
      ManagedKeysProtos.GetManagedKeysResponse response = stub.enableKeyManagement(null,
        ManagedKeysRequest.newBuilder().setKeyCust(keyCust).setKeyNamespace(keyNamespace).build());
      return generateKeyDataList(response);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(String keyCust, String keyNamespace)
    throws IOException, KeyException {
    try {
      ManagedKeysProtos.GetManagedKeysResponse statusResponse = stub.getManagedKeys(null,
        ManagedKeysRequest.newBuilder().setKeyCust(keyCust).setKeyNamespace(keyNamespace).build());
      return generateKeyDataList(statusResponse);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  private static List<ManagedKeyData> generateKeyDataList(
      ManagedKeysProtos.GetManagedKeysResponse stateResponse) {
    List<ManagedKeyData> keyStates = new ArrayList<>();
    for (ManagedKeysResponse state: stateResponse.getStateList()) {
      keyStates.add(new ManagedKeyData(
        state.getKeyCustBytes().toByteArray(),
        state.getKeyNamespace(), null,
        ManagedKeyState.forValue((byte) state.getKeyState().getNumber()),
        state.getKeyMetadata(),
        state.getRefreshTimestamp()));
    }
    return keyStates;
  }
}
