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
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos.PBEAdminRequest;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos.PBEAdminResponse;
import org.apache.hadoop.hbase.protobuf.generated.PBEAdminProtos.PBEAdminService;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

@CoreCoprocessor
@InterfaceAudience.Private
public class KeyMetaServiceEndpoint implements MasterCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(KeyMetaServiceEndpoint.class);

  private MasterServices master = null;
  private KeyMetaManager keyMetaManager;

  private final PBEAdminService pbeAdminService = new KeyMetaAdminServiceImpl();

  @Override public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }

    master = ((HasMasterServices) env).getMasterServices();
    keyMetaManager = new KeyMetaManager(master);
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(pbeAdminService);
  }

  private class KeyMetaAdminServiceImpl extends PBEAdminService {

    @Override
    public void enablePBE(RpcController controller, PBEAdminRequest request,
        RpcCallback<PBEAdminResponse> done) {
      PBEAdminResponse.Builder builder =
        PBEAdminResponse.newBuilder().setPbePrefix(request.getPbePrefix());
      byte[] pbe_prefix = null;
      try {
        pbe_prefix = Base64.getDecoder().decode(request.getPbePrefix());
      }
      catch (IllegalArgumentException e) {
        builder.setPbeStatus(PBEAdminProtos.PBEKeyStatus.PBE_FAILED);
        CoprocessorRpcUtils.setControllerException(controller, new IOException(
          "Failed to decode specified prefix as Base64 string: " + request.getPbePrefix(), e));
      }
      if (pbe_prefix != null) {
        try {
          PBEKeyStatus pbeKeyStatus = keyMetaManager.enablePBE(request.getPbePrefix());
          builder.setPbeStatus(PBEAdminProtos.PBEKeyStatus.valueOf(pbeKeyStatus.getVal()));
        } catch (IOException e) {
          CoprocessorRpcUtils.setControllerException(controller, e);
          builder.setPbeStatus(PBEAdminProtos.PBEKeyStatus.PBE_FAILED);
        }
      }
      done.run(builder.build());
    }
  }
}
