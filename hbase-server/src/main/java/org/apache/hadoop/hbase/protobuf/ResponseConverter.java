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
package org.apache.hadoop.hbase.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.RpcController;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.ByteString;

import static org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.UserPermissionsResponse;

/**
 * Helper utility to build protocol buffer responses,
 * or retrieve data from protocol buffer responses.
 */
@InterfaceAudience.Private
public final class ResponseConverter {

  private ResponseConverter() {
  }

// Start utilities for Client

  /**
   * Get the client Results from a protocol buffer ScanResponse
   *
   * @param response the protocol buffer ScanResponse
   * @return the client Results in the response
   */
  public static Result[] getResults(final ScanResponse response) {
    if (response == null) return null;
    int count = response.getResultCount();
    Result[] results = new Result[count];
    for (int i = 0; i < count; i++) {
      results[i] = ProtobufUtil.toResult(response.getResult(i));
    }
    return results;
  }

  /**
   * Get the results from a protocol buffer MultiResponse
   *
   * @param proto the protocol buffer MultiResponse to convert
   * @return the results in the MultiResponse
   * @throws IOException
   */
  public static List<Object> getResults(
      final ClientProtos.MultiResponse proto) throws IOException {
    List<Object> results = new ArrayList<Object>();
    List<ActionResult> resultList = proto.getResultList();
    for (int i = 0, n = resultList.size(); i < n; i++) {
      ActionResult result = resultList.get(i);
      if (result.hasException()) {
        results.add(ProtobufUtil.toException(result.getException()));
      } else if (result.hasValue()) {
        Object value = ProtobufUtil.toObject(result.getValue());
        if (value instanceof ClientProtos.Result) {
          results.add(ProtobufUtil.toResult((ClientProtos.Result)value));
        } else {
          results.add(value);
        }
      } else {
        results.add(new Result());
      }
    }
    return results;
  }

  /**
   * Wrap a throwable to an action result.
   *
   * @param t
   * @return an action result
   */
  public static ActionResult buildActionResult(final Throwable t) {
    ActionResult.Builder builder = ActionResult.newBuilder();
    NameBytesPair.Builder parameterBuilder = NameBytesPair.newBuilder();
    parameterBuilder.setName(t.getClass().getName());
    parameterBuilder.setValue(
      ByteString.copyFromUtf8(StringUtils.stringifyException(t)));
    builder.setException(parameterBuilder.build());
    return builder.build();
  }

  /**
   * Converts the permissions list into a protocol buffer UserPermissionsResponse
   */
  public static UserPermissionsResponse buildUserPermissionsResponse(
      final List<UserPermission> permissions) {
    UserPermissionsResponse.Builder builder = UserPermissionsResponse.newBuilder();
    for (UserPermission perm : permissions) {
      builder.addPermission(ProtobufUtil.toUserPermission(perm));
    }
    return builder.build();
  }

// End utilities for Client
// Start utilities for Admin

  /**
   * Get the list of regions to flush from a RollLogWriterResponse
   *
   * @param proto the RollLogWriterResponse
   * @return the the list of regions to flush
   */
  public static byte[][] getRegions(final RollWALWriterResponse proto) {
    if (proto == null || proto.getRegionToFlushCount() == 0) return null;
    List<byte[]> regions = new ArrayList<byte[]>();
    for (ByteString region: proto.getRegionToFlushList()) {
      regions.add(region.toByteArray());
    }
    return (byte[][])regions.toArray();
  }

  /**
   * Get the list of region info from a GetOnlineRegionResponse
   *
   * @param proto the GetOnlineRegionResponse
   * @return the list of region info
   */
  public static List<HRegionInfo> getRegionInfos(final GetOnlineRegionResponse proto) {
    if (proto == null || proto.getRegionInfoCount() == 0) return null;
    return ProtobufUtil.getRegionInfos(proto);
  }

  /**
   * Get the region opening state from a OpenRegionResponse
   *
   * @param proto the OpenRegionResponse
   * @return the region opening state
   */
  public static RegionOpeningState getRegionOpeningState
      (final OpenRegionResponse proto) {
    if (proto == null || proto.getOpeningStateCount() != 1) return null;
    return RegionOpeningState.valueOf(
      proto.getOpeningState(0).name());
  }

  /**
   * Get a list of region opening state from a OpenRegionResponse
   * 
   * @param proto the OpenRegionResponse
   * @return the list of region opening state
   */
  public static List<RegionOpeningState> getRegionOpeningStateList(
      final OpenRegionResponse proto) {
    if (proto == null) return null;
    List<RegionOpeningState> regionOpeningStates = new ArrayList<RegionOpeningState>();
    for (int i = 0; i < proto.getOpeningStateCount(); i++) {
      regionOpeningStates.add(RegionOpeningState.valueOf(
          proto.getOpeningState(i).name()));
    }
    return regionOpeningStates;
  }

  /**
   * Check if the region is closed from a CloseRegionResponse
   *
   * @param proto the CloseRegionResponse
   * @return the region close state
   */
  public static boolean isClosed
      (final CloseRegionResponse proto) {
    if (proto == null || !proto.hasClosed()) return false;
    return proto.getClosed();
  }

  /**
   * A utility to build a GetServerInfoResponse.
   *
   * @param serverName
   * @param webuiPort
   * @return the response
   */
  public static GetServerInfoResponse buildGetServerInfoResponse(
      final ServerName serverName, final int webuiPort) {
    GetServerInfoResponse.Builder builder = GetServerInfoResponse.newBuilder();
    ServerInfo.Builder serverInfoBuilder = ServerInfo.newBuilder();
    serverInfoBuilder.setServerName(ProtobufUtil.toServerName(serverName));
    if (webuiPort >= 0) {
      serverInfoBuilder.setWebuiPort(webuiPort);
    }
    builder.setServerInfo(serverInfoBuilder.build());
    return builder.build();
  }

  /**
   * A utility to build a GetOnlineRegionResponse.
   *
   * @param regions
   * @return the response
   */
  public static GetOnlineRegionResponse buildGetOnlineRegionResponse(
      final List<HRegionInfo> regions) {
    GetOnlineRegionResponse.Builder builder = GetOnlineRegionResponse.newBuilder();
    for (HRegionInfo region: regions) {
      builder.addRegionInfo(HRegionInfo.convert(region));
    }
    return builder.build();
  }

  /**
   * Creates a response for the catalog scan request
   * @return A CatalogScanResponse
   */
  public static CatalogScanResponse buildCatalogScanResponse(int numCleaned) {
    return CatalogScanResponse.newBuilder().setScanResult(numCleaned).build();
  }

  /**
   * Creates a response for the catalog scan request
   * @return A EnableCatalogJanitorResponse
   */
  public static EnableCatalogJanitorResponse buildEnableCatalogJanitorResponse(boolean prevValue) {
    return EnableCatalogJanitorResponse.newBuilder().setPrevValue(prevValue).build();
  }

// End utilities for Admin

  /**
   * Creates a response for the last flushed sequence Id request
   * @return A GetLastFlushedSequenceIdResponse
   */
  public static GetLastFlushedSequenceIdResponse buildGetLastFlushedSequenceIdResponse(
      long seqId) {
    return GetLastFlushedSequenceIdResponse.newBuilder().setLastFlushedSequenceId(seqId).build();
  }

  /**
   * Stores an exception encountered during RPC invocation so it can be passed back
   * through to the client.
   * @param controller the controller instance provided by the client when calling the service
   * @param ioe the exception encountered
   */
  public static void setControllerException(RpcController controller, IOException ioe) {
    if (controller != null) {
      if (controller instanceof ServerRpcController) {
        ((ServerRpcController)controller).setFailedOn(ioe);
      } else {
        controller.setFailed(StringUtils.stringifyException(ioe));
      }
    }
  }
}
