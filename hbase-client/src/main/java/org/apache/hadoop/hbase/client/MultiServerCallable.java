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
package org.apache.hadoop.hbase.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Callable that handles the <code>multi</code> method call going against a single
 * regionserver; i.e. A {@link RegionServerCallable} for the multi call (It is not a
 * {@link RegionServerCallable} that goes against multiple regions.
 * @param <R>
 */
class MultiServerCallable<R> extends PayloadCarryingServerCallable<MultiResponse> {
  private final MultiAction<R> multiAction;
  private final boolean cellBlock;
  private final RetryingTimeTracker tracker;
  private final int rpcTimeout;

  MultiServerCallable(final ClusterConnection connection, final TableName tableName,
      final ServerName location, RpcControllerFactory rpcFactory, final MultiAction<R> multi,
      int rpcTimeout, RetryingTimeTracker tracker, int priority) {
    super(connection, tableName, null, rpcFactory, priority);
    this.multiAction = multi;
    // RegionServerCallable has HRegionLocation field, but this is a multi-region request.
    // Using region info from parent HRegionLocation would be a mistake for this class; so
    // we will store the server here, and throw if someone tries to obtain location/regioninfo.
    this.location = new HRegionLocation(null, location);
    this.cellBlock = isCellBlock();
    this.tracker = tracker;
    this.rpcTimeout = rpcTimeout;
  }

  @Override
  protected HRegionLocation getLocation() {
    throw new RuntimeException("Cannot get region location for multi-region request");
  }

  @Override
  public HRegionInfo getHRegionInfo() {
    throw new RuntimeException("Cannot get region info for multi-region request");
  }

  MultiAction<R> getMulti() {
    return this.multiAction;
  }

  @Override
  public MultiResponse call(int operationTimeout) throws IOException {
    //System.out.println("in multiservercallable.java call");
    int remainingTime = tracker.getRemainingTime(operationTimeout);
    if (remainingTime <= 1) {
      // "1" is a special return value in RetryingTimeTracker, see its implementation.
      throw new DoNotRetryIOException("Operation Timeout");
    }
    int callTimeout = Math.min(rpcTimeout, remainingTime);
    int countOfActions = this.multiAction.size();
    if (countOfActions <= 0) throw new DoNotRetryIOException("No Actions");
    MultiRequest.Builder multiRequestBuilder = MultiRequest.newBuilder();
    RegionAction.Builder regionActionBuilder = RegionAction.newBuilder();
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    MutationProto.Builder mutationBuilder = MutationProto.newBuilder();

    // Pre-size. Presume at least a KV per Action. There are likely more.
    List<CellScannable> cells =
        (this.cellBlock ? new ArrayList<CellScannable>(countOfActions) : null);

    long nonceGroup = multiAction.getNonceGroup();
    if (nonceGroup != HConstants.NO_NONCE) {
      multiRequestBuilder.setNonceGroup(nonceGroup);
    }
    // Index to track RegionAction within the MultiRequest
    int regionActionIndex = -1;
    // Map from a created RegionAction for a RowMutations to the original index within
    // its original list of actions
    Map<Integer, Integer> rowMutationsIndexMap = new HashMap<>();
    // The multi object is a list of Actions by region.  Iterate by region.
    for (Map.Entry<byte[], List<Action<R>>> e: this.multiAction.actions.entrySet()) {
      final byte [] regionName = e.getKey();
      final List<Action<R>> actions = e.getValue();
      regionActionBuilder.clear();
      regionActionBuilder.setRegion(RequestConverter.buildRegionSpecifier(
        HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName) );

      int rowMutations = 0;
      for (Action<R> action : actions) {
        Row row = action.getAction();
        //Mutation mutation=(Mutation)(row);
        //System.out.println("finally "+mutation.getId());
        // Row Mutations are a set of Puts and/or Deletes all to be applied atomically
        // on the one row. We do separate RegionAction for each RowMutations.
        // We maintain a map to keep track of this RegionAction and the original Action index.
        if (row instanceof RowMutations) {
          RowMutations rms = (RowMutations)row;
          if (this.cellBlock) {
            // Build a multi request absent its Cell payload. Send data in cellblocks.
            regionActionBuilder = RequestConverter.buildNoDataRegionAction(regionName, rms, cells,
              regionActionBuilder, actionBuilder, mutationBuilder);
          } else {
            regionActionBuilder = RequestConverter.buildRegionAction(regionName, rms);
          }
          regionActionBuilder.setAtomic(true);
          multiRequestBuilder.addRegionAction(regionActionBuilder.build());
          regionActionIndex++;
          rowMutationsIndexMap.put(regionActionIndex, action.getOriginalIndex());
          rowMutations++;

          regionActionBuilder.clear();
          regionActionBuilder.setRegion(RequestConverter.buildRegionSpecifier(
            HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName) );
        }
      }

      if (actions.size() > rowMutations) {
        if (this.cellBlock) {
          // Send data in cellblocks. The call to buildNoDataRegionAction will skip RowMutations.
          // They have already been handled above. Guess at count of cells
          regionActionBuilder = RequestConverter.buildNoDataRegionAction(regionName, actions, cells,
            regionActionBuilder, actionBuilder, mutationBuilder);
        } else {
          regionActionBuilder = RequestConverter.buildRegionAction(regionName, actions,
            regionActionBuilder, actionBuilder, mutationBuilder);
        }
        multiRequestBuilder.addRegionAction(regionActionBuilder.build());
        regionActionIndex++;
      }
      /*for(int jj=0;jj<multiRequestBuilder.getRegionActionList().size();jj++) {
        for (int ii = 0; ii < multiRequestBuilder.getRegionActionList().get(jj).getActionList().size(); ii++) {
          //System.out.println("multiservercallable.java get action list " + multiRequestBuilder.getRegionActionList().get(jj).getActionList().get(ii));
          if(multiRequestBuilder.getRegionActionList().get(jj).getActionList().get(ii).getMutation().hasMyTraceId()){
            System.out.println("multiservercallable.java get action list " + multiRequestBuilder.getRegionActionList().get(jj).getActionList().get(ii).getMutation().getMyTraceId());}
        }
      }*/
    }

    // Controller optionally carries cell data over the proxy/service boundary and also
    // optionally ferries cell response data back out again.
    controller.reset();
    if (cells != null) controller.setCellScanner(CellUtil.createCellScanner(cells));
    controller.setPriority(getTableName());
    controller.setPriority(getPriority());
    controller.setCallTimeout(callTimeout);
    ClientProtos.MultiResponse responseProto;
    ClientProtos.MultiRequest requestProto = multiRequestBuilder.build();
    try {
      responseProto = getStub().multi(controller, requestProto);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
    if (responseProto == null) return null; // Occurs on cancel
    return ResponseConverter.getResults(requestProto, rowMutationsIndexMap,
      responseProto, controller.cellScanner());
  }

  /**
   * @return True if we should send data in cellblocks.  This is an expensive call.  Cache the
   * result if you can rather than call each time.
   */
  private boolean isCellBlock() {
    // This is not exact -- the configuration could have changed on us after connection was set up
    // but it will do for now.
    HConnection connection = getConnection();
    if (!(connection instanceof ClusterConnection)) return true; // Default is to do cellblocks.
    return ((ClusterConnection) connection).hasCellBlockSupport();
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    // Use the location we were given in the constructor rather than go look it up.
    setStub(getConnection().getClient(this.location.getServerName()));
  }

  @VisibleForTesting
  ServerName getServerName() {
    return location.getServerName();
  }
}
