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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.BooleanMsg;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.GetManagedKeysResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ManagedKeyResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ManagedKeysProtos;

@InterfaceAudience.Public
public class KeymetaAdminClient implements KeymetaAdmin {
  private ManagedKeysProtos.ManagedKeysService.BlockingInterface stub;

  public KeymetaAdminClient(Connection conn) throws IOException {
    this.stub =
      ManagedKeysProtos.ManagedKeysService.newBlockingStub(conn.getAdmin().coprocessorService());
  }

  @Override
  public ManagedKeyData enableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException {
    try {
      ManagedKeyResponse response = stub.enableKeyManagement(null, ManagedKeyRequest.newBuilder()
        .setKeyCust(ByteString.copyFrom(keyCust)).setKeyNamespace(keyNamespace).build());
      return generateKeyData(response);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    try {
      GetManagedKeysResponse statusResponse =
        stub.getManagedKeys(null, ManagedKeyRequest.newBuilder()
          .setKeyCust(ByteString.copyFrom(keyCust)).setKeyNamespace(keyNamespace).build());
      return generateKeyDataList(statusResponse);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public boolean rotateSTK() throws IOException {
    try {
      BooleanMsg response = stub.rotateSTK(null, EmptyMsg.getDefaultInstance());
      return response.getBoolMsg();
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void ejectManagedKeyDataCacheEntry(byte[] keyCustodian, String keyNamespace,
    String keyMetadata) throws IOException {
    throw new NotImplementedException(
      "ejectManagedKeyDataCacheEntry not supported in KeymetaAdminClient");
  }

  @Override
  public void clearManagedKeyDataCache() throws IOException {
    throw new NotImplementedException(
      "clearManagedKeyDataCache not supported in KeymetaAdminClient");
  }

  @Override
  public List<ManagedKeyData> disableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    try {
      GetManagedKeysResponse response =
        stub.disableKeyManagement(null, ManagedKeyRequest.newBuilder()
          .setKeyCust(ByteString.copyFrom(keyCust)).setKeyNamespace(keyNamespace).build());
      return generateKeyDataList(response);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public ManagedKeyData disableManagedKey(byte[] keyCust, String keyNamespace,
    byte[] keyMetadataHash) throws IOException, KeyException {
    try {
      ManagedKeyResponse response = stub.disableManagedKey(null,
        ManagedKeyEntryRequest.newBuilder()
          .setKeyCustNs(ManagedKeyRequest.newBuilder().setKeyCust(ByteString.copyFrom(keyCust))
            .setKeyNamespace(keyNamespace).build())
          .setKeyMetadataHash(ByteString.copyFrom(keyMetadataHash)).build());
      return generateKeyData(response);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public ManagedKeyData rotateManagedKey(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    try {
      ManagedKeyResponse response = stub.rotateManagedKey(null, ManagedKeyRequest.newBuilder()
        .setKeyCust(ByteString.copyFrom(keyCust)).setKeyNamespace(keyNamespace).build());
      return generateKeyData(response);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void refreshManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    try {
      stub.refreshManagedKeys(null, ManagedKeyRequest.newBuilder()
        .setKeyCust(ByteString.copyFrom(keyCust)).setKeyNamespace(keyNamespace).build());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  private static List<ManagedKeyData> generateKeyDataList(GetManagedKeysResponse stateResponse) {
    List<ManagedKeyData> keyStates = new ArrayList<>();
    for (ManagedKeyResponse state : stateResponse.getStateList()) {
      keyStates.add(generateKeyData(state));
    }
    return keyStates;
  }

  private static ManagedKeyData generateKeyData(ManagedKeyResponse response) {
    // Use hash-only constructor for client-side ManagedKeyData
    byte[] keyMetadataHash =
      response.hasKeyMetadataHash() ? response.getKeyMetadataHash().toByteArray() : null;
    return new ManagedKeyData(response.getKeyCust().toByteArray(), response.getKeyNamespace(),
      ManagedKeyState.forValue((byte) response.getKeyState().getNumber()), keyMetadataHash,
      response.getRefreshTimestamp());
  }
}
