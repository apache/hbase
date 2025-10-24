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
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.GetManagedKeysResponse;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysRequest;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysResponse;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysService;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.RotateSTKResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.EmptyMsg;

/**
 * This class implements a coprocessor service endpoint for the key management metadata operations.
 * It handles the following methods: This endpoint is designed to work in conjunction with the
 * {@link KeymetaAdmin} interface, which provides the actual implementation of the key metadata
 * operations.
 * </p>
 */
@CoreCoprocessor
@InterfaceAudience.Private
public class KeymetaServiceEndpoint implements MasterCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaServiceEndpoint.class);

  private MasterServices master = null;

  private final ManagedKeysService managedKeysService = new KeymetaAdminServiceImpl();

  /**
   * Starts the coprocessor by initializing the reference to the
   * {@link org.apache.hadoop.hbase.master.MasterServices} * instance.
   * @param env The coprocessor environment.
   * @throws IOException If an error occurs during initialization.
   */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }

    master = ((HasMasterServices) env).getMasterServices();
  }

  /**
   * Returns an iterable of the available coprocessor services, which includes the
   * {@link ManagedKeysService} implemented by
   * {@link KeymetaServiceEndpoint.KeymetaAdminServiceImpl}.
   * @return An iterable of the available coprocessor services.
   */
  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(managedKeysService);
  }

  /**
   * The implementation of the {@link ManagedKeysProtos.ManagedKeysService} interface, which
   * provides the actual method implementations for enabling key management.
   */
  @InterfaceAudience.Private
  public class KeymetaAdminServiceImpl extends ManagedKeysService {

    /**
     * Enables key management for a given tenant and namespace, as specified in the provided
     * request.
     * @param controller The RPC controller.
     * @param request    The request containing the tenant and table specifications.
     * @param done       The callback to be invoked with the response.
     */
    @Override
    public void enableKeyManagement(RpcController controller, ManagedKeysRequest request,
      RpcCallback<ManagedKeysResponse> done) {
      ManagedKeysResponse response = null;
      ManagedKeysResponse.Builder builder = ManagedKeysResponse.newBuilder();
      try {
        initManagedKeysResponseBuilder(controller, request, builder);
        ManagedKeyData managedKeyState = master.getKeymetaAdmin()
          .enableKeyManagement(request.getKeyCust().toByteArray(), request.getKeyNamespace());
        response = generateKeyStateResponse(managedKeyState, builder);
      } catch (IOException | KeyException e) {
        CoprocessorRpcUtils.setControllerException(controller, new DoNotRetryIOException(e));
        builder.setKeyState(ManagedKeysProtos.ManagedKeyState.KEY_FAILED);
      }
      if (response == null) {
        response = builder.build();
      }
      done.run(response);
    }

    @Override
    public void getManagedKeys(RpcController controller, ManagedKeysRequest request,
      RpcCallback<GetManagedKeysResponse> done) {
      GetManagedKeysResponse keyStateResponse = null;
      ManagedKeysResponse.Builder builder = ManagedKeysResponse.newBuilder();
      try {
        initManagedKeysResponseBuilder(controller, request, builder);
        List<ManagedKeyData> managedKeyStates = master.getKeymetaAdmin()
          .getManagedKeys(request.getKeyCust().toByteArray(), request.getKeyNamespace());
        keyStateResponse = generateKeyStateResponse(managedKeyStates, builder);
      } catch (IOException | KeyException e) {
        CoprocessorRpcUtils.setControllerException(controller, new DoNotRetryIOException(e));
      }
      if (keyStateResponse == null) {
        keyStateResponse = GetManagedKeysResponse.getDefaultInstance();
      }
      done.run(keyStateResponse);
    }

    /**
     * Rotates the system key (STK) by checking for a new key and propagating it to all region
     * servers.
     * @param controller The RPC controller.
     * @param request    The request (empty).
     * @param done       The callback to be invoked with the response.
     */
    @Override
    public void rotateSTK(RpcController controller, EmptyMsg request,
      RpcCallback<RotateSTKResponse> done) {
      boolean rotated;
      try {
        rotated = master.getKeymetaAdmin().rotateSTK();
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, new DoNotRetryIOException(e));
        rotated = false;
      }
      done.run(RotateSTKResponse.newBuilder().setRotated(rotated).build());
    }
  }

  @InterfaceAudience.Private
  public static ManagedKeysResponse.Builder initManagedKeysResponseBuilder(RpcController controller,
    ManagedKeysRequest request, ManagedKeysResponse.Builder builder) throws IOException {
    builder.setKeyCust(request.getKeyCust());
    builder.setKeyNamespace(request.getKeyNamespace());
    if (request.getKeyCust().isEmpty()) {
      throw new IOException("key_cust must not be empty");
    }
    return builder;
  }

  // Assumes that all ManagedKeyData objects belong to the same custodian and namespace.
  @InterfaceAudience.Private
  public static GetManagedKeysResponse generateKeyStateResponse(
    List<ManagedKeyData> managedKeyStates, ManagedKeysResponse.Builder builder) {
    GetManagedKeysResponse.Builder responseBuilder = GetManagedKeysResponse.newBuilder();
    for (ManagedKeyData keyData : managedKeyStates) {
      responseBuilder.addState(generateKeyStateResponse(keyData, builder));
    }
    return responseBuilder.build();
  }

  private static ManagedKeysResponse generateKeyStateResponse(ManagedKeyData keyData,
    ManagedKeysResponse.Builder builder) {
    builder.setKeyState(ManagedKeysProtos.ManagedKeyState.forNumber(keyData.getKeyState().getVal()))
      .setKeyMetadata(keyData.getKeyMetadata()).setRefreshTimestamp(keyData.getRefreshTimestamp())
      .setKeyNamespace(keyData.getKeyNamespace());
    return builder.build();
  }
}
