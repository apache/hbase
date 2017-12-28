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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;

/**
 * Utility for this assignment package only.
 */
@InterfaceAudience.Private
class Util {
  private Util() {}

  /**
   * Raw call to remote regionserver to get info on a particular region.
   * @throws IOException Let it out so can report this IOE as reason for failure
   */
  static GetRegionInfoResponse getRegionInfoResponse(final MasterProcedureEnv env,
      final ServerName regionLocation, final RegionInfo hri)
  throws IOException {
    return getRegionInfoResponse(env, regionLocation, hri, false);
  }

  static GetRegionInfoResponse getRegionInfoResponse(final MasterProcedureEnv env,
      final ServerName regionLocation, final RegionInfo hri, boolean includeBestSplitRow)
  throws IOException {
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = env.getMasterServices().getClusterConnection().
        getRpcControllerFactory().newController();
    final AdminService.BlockingInterface admin =
        env.getMasterServices().getClusterConnection().getAdmin(regionLocation);
    GetRegionInfoRequest request = null;
    if (includeBestSplitRow) {
      request = RequestConverter.buildGetRegionInfoRequest(hri.getRegionName(), false, true);
    } else {
      request = RequestConverter.buildGetRegionInfoRequest(hri.getRegionName());
    }
    try {
      return admin.getRegionInfo(controller, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }
}
