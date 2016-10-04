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
package org.apache.hadoop.hbase.shaded.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.SingleResponse;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameInt64Pair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos.ScanMetrics;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.util.StringUtils;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Helper utility to build protocol buffer responses,
 * or retrieve data from protocol buffer responses.
 */
@InterfaceAudience.Private
public final class ResponseConverter {
  private static final Log LOG = LogFactory.getLog(ResponseConverter.class);

  private ResponseConverter() {
  }

  // Start utilities for Client
  public static SingleResponse getResult(final ClientProtos.MutateRequest request,
      final ClientProtos.MutateResponse response,
      final CellScanner cells)
          throws IOException {
    SingleResponse singleResponse = new SingleResponse();
    SingleResponse.Entry entry = new SingleResponse.Entry();
    entry.setResult(ProtobufUtil.toResult(response.getResult(), cells));
    entry.setProcessed(response.getProcessed());
    singleResponse.setEntry(entry);
    return singleResponse;
  }

  /**
   * Get the results from a protocol buffer MultiResponse
   *
   * @param request the protocol buffer MultiResponse to convert
   * @param cells Cells to go with the passed in <code>proto</code>.  Can be null.
   * @return the results that were in the MultiResponse (a Result or an Exception).
   * @throws IOException
   */
  public static org.apache.hadoop.hbase.client.MultiResponse getResults(final MultiRequest request,
      final MultiResponse response, final CellScanner cells)
  throws IOException {
    int requestRegionActionCount = request.getRegionActionCount();
    int responseRegionActionResultCount = response.getRegionActionResultCount();
    if (requestRegionActionCount != responseRegionActionResultCount) {
      throw new IllegalStateException("Request mutation count=" + requestRegionActionCount +
          " does not match response mutation result count=" + responseRegionActionResultCount);
    }

    org.apache.hadoop.hbase.client.MultiResponse results =
      new org.apache.hadoop.hbase.client.MultiResponse();

    for (int i = 0; i < responseRegionActionResultCount; i++) {
      RegionAction actions = request.getRegionAction(i);
      RegionActionResult actionResult = response.getRegionActionResult(i);
      HBaseProtos.RegionSpecifier rs = actions.getRegion();
      if (rs.hasType() &&
          (rs.getType() != HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)){
        throw new IllegalArgumentException(
            "We support only encoded types for protobuf multi response.");
      }
      byte[] regionName = rs.getValue().toByteArray();

      if (actionResult.hasException()) {
        Throwable regionException =  ProtobufUtil.toException(actionResult.getException());
        results.addException(regionName, regionException);
        continue;
      }

      if (actions.getActionCount() != actionResult.getResultOrExceptionCount()) {
        throw new IllegalStateException("actions.getActionCount=" + actions.getActionCount() +
            ", actionResult.getResultOrExceptionCount=" +
            actionResult.getResultOrExceptionCount() + " for region " + actions.getRegion());
      }

      for (ResultOrException roe : actionResult.getResultOrExceptionList()) {
        Object responseValue;
        if (roe.hasException()) {
          responseValue = ProtobufUtil.toException(roe.getException());
        } else if (roe.hasResult()) {
          responseValue = ProtobufUtil.toResult(roe.getResult(), cells);
        } else if (roe.hasServiceResult()) {
          responseValue = roe.getServiceResult();
        } else{
          // Sometimes, the response is just "it was processed". Generally, this occurs for things
          // like mutateRows where either we get back 'processed' (or not) and optionally some
          // statistics about the regions we touched.
          responseValue = response.getProcessed() ?
                          ProtobufUtil.EMPTY_RESULT_EXISTS_TRUE :
                          ProtobufUtil.EMPTY_RESULT_EXISTS_FALSE;
        }
        results.add(regionName, roe.getIndex(), responseValue);
      }
    }

    if (response.hasRegionStatistics()) {
      ClientProtos.MultiRegionLoadStats stats = response.getRegionStatistics();
      for (int i = 0; i < stats.getRegionCount(); i++) {
        results.addStatistic(stats.getRegion(i).getValue().toByteArray(), stats.getStat(i));
      }
    }

    return results;
  }

  /**
   * Wrap a throwable to an action result.
   *
   * @param t
   * @return an action result builder
   */
  public static ResultOrException.Builder buildActionResult(final Throwable t) {
    ResultOrException.Builder builder = ResultOrException.newBuilder();
    if (t != null) builder.setException(buildException(t));
    return builder;
  }

  /**
   * Wrap a throwable to an action result.
   *
   * @param r
   * @return an action result builder
   */
  public static ResultOrException.Builder buildActionResult(final ClientProtos.Result r) {
    ResultOrException.Builder builder = ResultOrException.newBuilder();
    if (r != null) builder.setResult(r);
    return builder;
  }

  /**
   * @param t
   * @return NameValuePair of the exception name to stringified version os exception.
   */
  public static NameBytesPair buildException(final Throwable t) {
    NameBytesPair.Builder parameterBuilder = NameBytesPair.newBuilder();
    parameterBuilder.setName(t.getClass().getName());
    parameterBuilder.setValue(
      ByteString.copyFromUtf8(StringUtils.stringifyException(t)));
    return parameterBuilder.build();
  }

// End utilities for Client
// Start utilities for Admin

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
   * @return A RunCatalogScanResponse
   */
  public static RunCatalogScanResponse buildRunCatalogScanResponse(int numCleaned) {
    return RunCatalogScanResponse.newBuilder().setScanResult(numCleaned).build();
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
      RegionStoreSequenceIds ids) {
    return GetLastFlushedSequenceIdResponse.newBuilder()
        .setLastFlushedSequenceId(ids.getLastFlushedSequenceId())
        .addAllStoreLastFlushedSequenceId(ids.getStoreSequenceIdList()).build();
  }

  /**
   * Stores an exception encountered during RPC invocation so it can be passed back
   * through to the client.
   * @param controller the controller instance provided by the client when calling the service
   * @param ioe the exception encountered
   */
  public static void setControllerException(com.google.protobuf.RpcController controller,
      IOException ioe) {
    if (controller != null) {
      if (controller instanceof ServerRpcController) {
        ((ServerRpcController)controller).setFailedOn(ioe);
      } else {
        controller.setFailed(StringUtils.stringifyException(ioe));
      }
    }
  }

  /**
   * Retreivies exception stored during RPC invocation.
   * @param controller the controller instance provided by the client when calling the service
   * @return exception if any, or null; Will return DoNotRetryIOException for string represented
   * failure causes in controller.
   */
  @Nullable
  public static IOException getControllerException(RpcController controller) throws IOException {
    if (controller != null && controller.failed()) {
      if (controller instanceof ServerRpcController) {
        return ((ServerRpcController)controller).getFailedOn();
      } else {
        return new DoNotRetryIOException(controller.errorText());
      }
    }
    return null;
  }


  /**
   * Create Results from the cells using the cells meta data.
   * @param cellScanner
   * @param response
   * @return results
   */
  public static Result[] getResults(CellScanner cellScanner, ScanResponse response)
      throws IOException {
    if (response == null) return null;
    // If cellscanner, then the number of Results to return is the count of elements in the
    // cellsPerResult list.  Otherwise, it is how many results are embedded inside the response.
    int noOfResults = cellScanner != null?
      response.getCellsPerResultCount(): response.getResultsCount();
    Result[] results = new Result[noOfResults];
    for (int i = 0; i < noOfResults; i++) {
      if (cellScanner != null) {
        // Cells are out in cellblocks.  Group them up again as Results.  How many to read at a
        // time will be found in getCellsLength -- length here is how many Cells in the i'th Result
        int noOfCells = response.getCellsPerResult(i);
        boolean isPartial =
            response.getPartialFlagPerResultCount() > i ?
                response.getPartialFlagPerResult(i) : false;
        List<Cell> cells = new ArrayList<Cell>(noOfCells);
        for (int j = 0; j < noOfCells; j++) {
          try {
            if (cellScanner.advance() == false) {
              // We are not able to retrieve the exact number of cells which ResultCellMeta says us.
              // We have to scan for the same results again. Throwing DNRIOE as a client retry on the
              // same scanner will result in OutOfOrderScannerNextException
              String msg = "Results sent from server=" + noOfResults + ". But only got " + i
                + " results completely at client. Resetting the scanner to scan again.";
              LOG.error(msg);
              throw new DoNotRetryIOException(msg);
            }
          } catch (IOException ioe) {
            // We are getting IOE while retrieving the cells for Results.
            // We have to scan for the same results again. Throwing DNRIOE as a client retry on the
            // same scanner will result in OutOfOrderScannerNextException
            LOG.error("Exception while reading cells from result."
              + "Resetting the scanner to scan again.", ioe);
            throw new DoNotRetryIOException("Resetting the scanner.", ioe);
          }
          cells.add(cellScanner.current());
        }
        results[i] = Result.create(cells, null, response.getStale(), isPartial);
      } else {
        // Result is pure pb.
        results[i] = ProtobufUtil.toResult(response.getResults(i));
      }
    }
    return results;
  }

  public static Map<String, Long> getScanMetrics(ScanResponse response) {
    Map<String, Long> metricMap = new HashMap<String, Long>();
    if (response == null || !response.hasScanMetrics() || response.getScanMetrics() == null) {
      return metricMap;
    }

    ScanMetrics metrics = response.getScanMetrics();
    int numberOfMetrics = metrics.getMetricsCount();
    for (int i = 0; i < numberOfMetrics; i++) {
      NameInt64Pair metricPair = metrics.getMetrics(i);
      if (metricPair != null) {
        String name = metricPair.getName();
        Long value = metricPair.getValue();
        if (name != null && value != null) {
          metricMap.put(name, value);
        }
      }
    }

    return metricMap;
  }
}
