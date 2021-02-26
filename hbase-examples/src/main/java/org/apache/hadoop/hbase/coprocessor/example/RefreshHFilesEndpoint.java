/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.protobuf.generated.RefreshHFilesProtos;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coprocessor endpoint to refresh HFiles on replica.
 * <p>
 * <p>
 * For the protocol buffer definition of the RefreshHFilesService, see the source file located under
 * hbase-protocol/src/main/protobuf/RefreshHFiles.proto.
 * </p>
 */
@InterfaceAudience.Private
public class RefreshHFilesEndpoint extends RefreshHFilesProtos.RefreshHFilesService
  implements RegionCoprocessor {
  protected static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesEndpoint.class);
  private RegionCoprocessorEnvironment env;

  public RefreshHFilesEndpoint() {
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  @Override
  public void refreshHFiles(RpcController controller,
      RefreshHFilesProtos.RefreshHFilesRequest request,
      RpcCallback<RefreshHFilesProtos.RefreshHFilesResponse> done) {
    try {
      for (Store store : env.getRegion().getStores()) {
        LOG.debug("Refreshing HFiles for region: " + store.getRegionInfo().getRegionNameAsString() +
                    " and store: " + store.getColumnFamilyName() + "class:" + store.getClass());
        store.refreshStoreFiles();
      }
    } catch (IOException ioe) {
      LOG.error("Exception while trying to refresh store files: ", ioe);
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(RefreshHFilesProtos.RefreshHFilesResponse.getDefaultInstance());
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }
}
