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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * Callable that handles the <code>multi</code> method call going against a single
 * regionserver; i.e. A RegionServerCallable for the multi call (It is NOT a
 * RegionServerCallable that goes against multiple regions).
 * @param <R>
 */
@InterfaceAudience.Private
class MultiServerCallable extends CancellableRegionServerCallable<MultiResponse> {
  private MultiAction multiAction;
  private boolean cellBlock;

  MultiServerCallable(final ClusterConnection connection, final TableName tableName,
      final ServerName location, final MultiAction multi, RpcController rpcController,
      int rpcTimeout, RetryingTimeTracker tracker, int priority) {
    super(connection, tableName, null, rpcController, rpcTimeout, tracker, priority);
    this.multiAction = multi;
    // RegionServerCallable has HRegionLocation field, but this is a multi-region request.
    // Using region info from parent HRegionLocation would be a mistake for this class; so
    // we will store the server here, and throw if someone tries to obtain location/regioninfo.
    this.location = new HRegionLocation(null, location);
    this.cellBlock = isCellBlock();
  }

  public void reset(ServerName location, MultiAction multiAction) {
    this.location = new HRegionLocation(null, location);
    this.multiAction = multiAction;
    this.cellBlock = isCellBlock();
  }

  @Override
  protected HRegionLocation getLocation() {
    throw new RuntimeException("Cannot get region location for multi-region request");
  }

  @Override
  public HRegionInfo getHRegionInfo() {
    throw new RuntimeException("Cannot get region info for multi-region request");
  }

  MultiAction getMulti() {
    return this.multiAction;
  }

  @Override
  protected MultiResponse rpcCall() throws Exception {
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
    // Map from a created RegionAction to the original index for a RowMutations within
    // its original list of actions
    Map<Integer, Integer> rowMutationsIndexMap = new HashMap<>();
    // The multi object is a list of Actions by region. Iterate by region.
    for (Map.Entry<byte[], List<Action>> e: this.multiAction.actions.entrySet()) {
      final byte [] regionName = e.getKey();
      final List<Action> actions = e.getValue();
      regionActionBuilder.clear();
      regionActionBuilder.setRegion(RequestConverter.buildRegionSpecifier(
          HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME, regionName));

      int rowMutations = 0;
      for (Action action : actions) {
        Row row = action.getAction();
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
    }

    if (cells != null) {
      setRpcControllerCellScanner(CellUtil.createCellScanner(cells));
    }
    ClientProtos.MultiRequest requestProto = multiRequestBuilder.build();
    ClientProtos.MultiResponse responseProto = getStub().multi(getRpcController(), requestProto);
    if (responseProto == null) return null; // Occurs on cancel
    return ResponseConverter.getResults(requestProto, rowMutationsIndexMap, responseProto,
      getRpcControllerCellScanner());
  }

  /**
   * @return True if we should send data in cellblocks.  This is an expensive call.  Cache the
   * result if you can rather than call each time.
   */
  private boolean isCellBlock() {
    // This is not exact -- the configuration could have changed on us after connection was set up
    // but it will do for now.
    ClusterConnection conn = getConnection();
    return conn.hasCellBlockSupport();
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
