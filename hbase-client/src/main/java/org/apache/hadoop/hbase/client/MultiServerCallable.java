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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;

import com.google.protobuf.ServiceException;

/**
 * Callable that handles the <code>multi</code> method call going against a single
 * regionserver; i.e. A {@link RegionServerCallable} for the multi call (It is not a
 * {@link RegionServerCallable} that goes against multiple regions.
 * @param <R>
 */
class MultiServerCallable<R> extends RegionServerCallable<MultiResponse> {
  private final MultiAction<R> multi;
  private final boolean cellBlock;

  MultiServerCallable(final HConnection connection, final TableName tableName,
      final HRegionLocation location, final MultiAction<R> multi) {
    super(connection, tableName, null);
    this.multi = multi;
    setLocation(location);
    this.cellBlock = isCellBlock();
  }

  MultiAction<R> getMulti() {
    return this.multi;
  }

  @Override
  public MultiResponse call() throws IOException {
    MultiResponse response = new MultiResponse();
    // The multi object is a list of Actions by region.
    for (Map.Entry<byte[], List<Action<R>>> e: this.multi.actions.entrySet()) {
      byte[] regionName = e.getKey();
      int rowMutations = 0;
      List<Action<R>> actions = e.getValue();
      for (Action<R> action : actions) {
        Row row = action.getAction();
        // Row Mutations are a set of Puts and/or Deletes all to be applied atomically
        // on the one row.  We do these a row at a time.
        if (row instanceof RowMutations) {
          RowMutations rms = (RowMutations)row;
          List<CellScannable> cells = null;
          MultiRequest multiRequest;
          try {
            if (this.cellBlock) {
              // Stick all Cells for all RowMutations in here into 'cells'.  Populated when we call
              // buildNoDataMultiRequest in the below.
              cells = new ArrayList<CellScannable>(rms.getMutations().size());
              // Build a multi request absent its Cell payload (this is the 'nodata' in the below).
              multiRequest = RequestConverter.buildNoDataMultiRequest(regionName, rms, cells);
            } else {
              multiRequest = RequestConverter.buildMultiRequest(regionName, rms);
            }
            // Carry the cells if any over the proxy/pb Service interface using the payload
            // carrying rpc controller.
            getStub().multi(new PayloadCarryingRpcController(cells), multiRequest);
            // This multi call does not return results.
            response.add(regionName, action.getOriginalIndex(), Result.EMPTY_RESULT);
          } catch (ServiceException se) {
            response.add(regionName, action.getOriginalIndex(),
              ProtobufUtil.getRemoteException(se));
          }
          rowMutations++;
        }
      }
      // Are there any non-RowMutation actions to send for this region?
      if (actions.size() > rowMutations) {
        Exception ex = null;
        List<Object> results = null;
        List<CellScannable> cells = null;
        MultiRequest multiRequest;
        try {
          if (isCellBlock()) {
            // Send data in cellblocks. The call to buildNoDataMultiRequest will skip RowMutations.
            // They have already been handled above.
            cells = new ArrayList<CellScannable>(actions.size() - rowMutations);
            multiRequest = RequestConverter.buildNoDataMultiRequest(regionName, actions, cells);
          } else {
            multiRequest = RequestConverter.buildMultiRequest(regionName, actions);
          }
          // Controller optionally carries cell data over the proxy/service boundary and also
          // optionally ferries cell response data back out again.
          PayloadCarryingRpcController controller = new PayloadCarryingRpcController(cells);
          ClientProtos.MultiResponse responseProto = getStub().multi(controller, multiRequest);
          results = ResponseConverter.getResults(responseProto, controller.cellScanner());
        } catch (ServiceException se) {
          ex = ProtobufUtil.getRemoteException(se);
        }
        for (int i = 0, n = actions.size(); i < n; i++) {
          int originalIndex = actions.get(i).getOriginalIndex();
          response.add(regionName, originalIndex, results == null ? ex : results.get(i));
        }
      }
    }
    return response;
  }


  /**
   * @return True if we should send data in cellblocks.  This is an expensive call.  Cache the
   * result if you can rather than call each time.
   */
  private boolean isCellBlock() {
    // This is not exact -- the configuration could have changed on us after connection was set up
    // but it will do for now.
    HConnection connection = getConnection();
    if (connection == null) return true; // Default is to do cellblocks.
    Configuration configuration = connection.getConfiguration();
    if (configuration == null) return true;
    String codec = configuration.get("hbase.client.rpc.codec", "");
    return codec != null && codec.length() > 0;
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    // Use the location we were given in the constructor rather than go look it up.
    setStub(getConnection().getClient(getLocation().getServerName()));
  }
}