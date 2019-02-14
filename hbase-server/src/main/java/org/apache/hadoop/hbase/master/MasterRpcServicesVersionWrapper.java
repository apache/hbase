/**
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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * A wrapper class for MasterRpcServices shortcut that ensures a client version is available
 * to the callee without a current RPC call.
 */
@InterfaceAudience.Private
public class MasterRpcServicesVersionWrapper
    implements RegionServerStatusProtos.RegionServerStatusService.BlockingInterface {

  @FunctionalInterface
  public interface ServiceCallFunction<Req, Resp>
      extends VersionInfoUtil.ServiceCallFunction<RpcController, Req, Resp, ServiceException> {
  }

  private final MasterRpcServices masterRpcServices;
  private final ServiceCallFunction<RegionServerStatusProtos.RegionServerStartupRequest,
    RegionServerStatusProtos.RegionServerStartupResponse> startupCall;
  private final ServiceCallFunction<RegionServerStatusProtos.RegionServerReportRequest,
    RegionServerStatusProtos.RegionServerReportResponse> reportCall;


  public MasterRpcServicesVersionWrapper(MasterRpcServices masterRpcServices) {
    this.masterRpcServices = masterRpcServices;
    this.startupCall = (c, req) -> masterRpcServices.regionServerStartup(c, req);
    this.reportCall = (c, req) -> masterRpcServices.regionServerReport(c, req);
  }

  @Override
  public RegionServerStatusProtos.RegionServerStartupResponse regionServerStartup(
      RpcController controller, RegionServerStatusProtos.RegionServerStartupRequest request)
      throws ServiceException {
    return VersionInfoUtil.callWithVersion(startupCall, controller, request);
  }

  @Override
  public RegionServerStatusProtos.RegionServerReportResponse regionServerReport(
      RpcController controller, RegionServerStatusProtos.RegionServerReportRequest request)
      throws ServiceException {
    return VersionInfoUtil.callWithVersion(reportCall, controller, request);
  }

  @Override
  public RegionServerStatusProtos.ReportRSFatalErrorResponse reportRSFatalError(
      RpcController controller, RegionServerStatusProtos.ReportRSFatalErrorRequest request)
      throws ServiceException {
    return masterRpcServices.reportRSFatalError(controller, request);
  }

  @Override
  public RegionServerStatusProtos.GetLastFlushedSequenceIdResponse getLastFlushedSequenceId(
      RpcController controller, RegionServerStatusProtos.GetLastFlushedSequenceIdRequest request)
      throws ServiceException {
    return masterRpcServices.getLastFlushedSequenceId(controller, request);
  }

  @Override
  public RegionServerStatusProtos.ReportRegionStateTransitionResponse reportRegionStateTransition(
      RpcController controller,
      RegionServerStatusProtos.ReportRegionStateTransitionRequest request)
      throws ServiceException {
    return masterRpcServices.reportRegionStateTransition(controller, request);
  }

  @Override
  public RegionServerStatusProtos.RegionSpaceUseReportResponse reportRegionSpaceUse(
      RpcController controller, RegionServerStatusProtos.RegionSpaceUseReportRequest request)
      throws ServiceException {
    return masterRpcServices.reportRegionSpaceUse(controller, request);
  }

  @Override
  public RegionServerStatusProtos.ReportProcedureDoneResponse reportProcedureDone(
      RpcController controller, RegionServerStatusProtos.ReportProcedureDoneRequest request)
      throws ServiceException {
    return masterRpcServices.reportProcedureDone(controller, request);
  }

  @Override
  public RegionServerStatusProtos.FileArchiveNotificationResponse reportFileArchival(
      RpcController controller, RegionServerStatusProtos.FileArchiveNotificationRequest request)
      throws ServiceException {
    return masterRpcServices.reportFileArchival(controller, request);
  }
}
