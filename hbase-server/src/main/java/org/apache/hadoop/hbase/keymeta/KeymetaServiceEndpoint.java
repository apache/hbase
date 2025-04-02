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

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysRequest;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysResponse;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.GetManagedKeysResponse;
import org.apache.hadoop.hbase.protobuf.generated.ManagedKeysProtos.ManagedKeysService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.security.KeyException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * This class implements a coprocessor service endpoint for the Phoenix Query Server's
 * PBE (Prefix Based Encryption) key metadata operations. It handles the following
 * methods:
 *
 * <ul>
 * <li>enablePBE(): Enables PBE for a given cust_spec and namespace.</li>
 * </ul>
 *
 * This endpoint is designed to work in conjunction with the {@link KeymetaAdmin}
 * interface, which provides the actual implementation of the key metadata operations.
 * </p>
 */
@CoreCoprocessor @InterfaceAudience.Private
public class KeymetaServiceEndpoint implements MasterCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaServiceEndpoint.class);

  private MasterServices master = null;

  private final ManagedKeysService managedKeysService = new KeyMetaAdminServiceImpl();

  /**
   * Starts the coprocessor by initializing the reference to the {@link org.apache.hadoop.hbase.master.MasterServices}
   * instance.
   *
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
   * {@link KeymetaServiceEndpoint.KeyMetaAdminServiceImpl}.
   *
   * @return An iterable of the available coprocessor services.
   */
  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(managedKeysService);
  }

  /**
   * The implementation of the {@link ManagedKeysProtos.ManagedKeysService}
   * interface, which provides the actual method implementations for enabling PBE.
   */
  private class KeyMetaAdminServiceImpl extends ManagedKeysService {
    /**
     * Enables PBE for a given tenant and namespace, as specified in the provided request.
     *
     * @param controller The RPC controller.
     * @param request    The request containing the tenant and table specifications.
     * @param done       The callback to be invoked with the response.
     */
    @Override
    public void enableManagedKeys(RpcController controller, ManagedKeysRequest request,
        RpcCallback<ManagedKeysResponse> done) {
      ManagedKeysResponse.Builder builder = getResponseBuilder(controller, request);
      if (builder.getCustSpec() != null) {
        try {
          ManagedKeyStatus managedKeyStatus = master.getPBEKeymetaAdmin()
            .enableManagedKeys(request.getCustSpec(), request.getKeyNamespace());
          builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.valueOf(
            managedKeyStatus.getVal()));
        } catch (IOException e) {
          CoprocessorRpcUtils.setControllerException(controller, e);
          builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.PBE_FAILED);
        }
      }
      done.run(builder.build());
    }

    @Override
    public void getManagedKeys(RpcController controller, ManagedKeysRequest request,
        RpcCallback<GetManagedKeysResponse> done) {
      GetManagedKeysResponse.Builder responseBuilder =
        GetManagedKeysResponse.newBuilder();
      ManagedKeysResponse.Builder builder = getResponseBuilder(controller, request);
      if (builder.getCustSpec() != null) {
        try {
          List<ManagedKeyData> managedKeyStatuses = master.getPBEKeymetaAdmin()
            .getManagedKeys(request.getCustSpec(), request.getKeyNamespace());
          for (ManagedKeyData keyData: managedKeyStatuses) {
            builder.setPbeStatus(
              ManagedKeysProtos.ManagedKeyStatus.valueOf(keyData.getKeyStatus().getVal()));
            builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.valueOf(
                      keyData.getKeyStatus().getVal()))
                   .setKeyMetadata(keyData.getKeyMetadata())
                   .setRefreshTimestamp(keyData.getRefreshTimestamp())
                   .setReadOpCount(keyData.getReadOpCount())
                   .setWriteOpCount(keyData.getWriteOpCount())
                   ;
            responseBuilder.addStatus(builder.build());
          }
        } catch (IOException e) {
          CoprocessorRpcUtils.setControllerException(controller, e);
          builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.PBE_FAILED);
        } catch (KeyException e) {
          CoprocessorRpcUtils.setControllerException(controller, new IOException(e));
          builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.PBE_FAILED);
        }
      }
      done.run(responseBuilder.build());
    }

    private byte[] convertToPBEBytes(RpcController controller, ManagedKeysRequest request,
      ManagedKeysResponse.Builder builder) {
      byte[] cust_spec = null;
      try {
        cust_spec = Base64.getDecoder().decode(request.getCustSpec());
      } catch (IllegalArgumentException e) {
        builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.PBE_FAILED);
        CoprocessorRpcUtils.setControllerException(controller, new IOException(
          "Failed to decode specified prefix as Base64 string: " + request.getCustSpec(), e));
      }
      return cust_spec;
    }

    private ManagedKeysResponse.Builder getResponseBuilder(RpcController controller,
        ManagedKeysRequest request) {
      ManagedKeysResponse.Builder builder = ManagedKeysResponse.newBuilder()
          .setKeyNamespace(request.getKeyNamespace());
      byte[] cust_spec = null;
      try {
        cust_spec = Base64.getDecoder().decode(request.getCustSpec());
        builder.setCustSpecBytes(ByteString.copyFrom(cust_spec));
      } catch (IllegalArgumentException e) {
        builder.setPbeStatus(ManagedKeysProtos.ManagedKeyStatus.PBE_FAILED);
        CoprocessorRpcUtils.setControllerException(controller, new IOException(
          "Failed to decode specified prefix as Base64 string: " + request.getCustSpec(), e));
      }
      return builder;
    }
  }
}
