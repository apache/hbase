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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.mortbay.log.Log;

import com.google.protobuf.ServiceException;

/**
 * A Callable for flushRegion() RPC.
 */
@InterfaceAudience.Private
public class FlushRegionCallable extends RegionAdminServiceCallable<FlushRegionResponse> {

  private final byte[] regionName;
  private final boolean writeFlushWalMarker;
  private boolean reload;

  public FlushRegionCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, TableName tableName, byte[] regionName,
      byte[] regionStartKey, boolean writeFlushWalMarker) {
    super(connection, rpcControllerFactory, tableName, regionStartKey);
    this.regionName = regionName;
    this.writeFlushWalMarker = writeFlushWalMarker;
  }

  public FlushRegionCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, HRegionInfo regionInfo,
      boolean writeFlushWalMarker) {
    this(connection, rpcControllerFactory, regionInfo.getTable(), regionInfo.getRegionName(),
      regionInfo.getStartKey(), writeFlushWalMarker);
  }

  @Override
  public FlushRegionResponse call(int callTimeout) throws Exception {
    return flushRegion();
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    super.prepare(reload);
    this.reload = reload;
  }

  private FlushRegionResponse flushRegion() throws IOException {
    // check whether we should still do the flush to this region. If the regions are changed due
    // to splits or merges, etc return success
    if (!Bytes.equals(location.getRegionInfo().getRegionName(), regionName)) {
      if (!reload) {
        throw new IOException("Cached location seems to be different than requested region.");
      }
      Log.info("Skipping flush region, because the located region "
          + Bytes.toStringBinary(location.getRegionInfo().getRegionName()) + " is different than "
          + " requested region " + Bytes.toStringBinary(regionName));
      return FlushRegionResponse.newBuilder()
          .setLastFlushTime(EnvironmentEdgeManager.currentTime())
          .setFlushed(false)
          .setWroteFlushWalMarker(false)
          .build();
    }

    FlushRegionRequest request =
        RequestConverter.buildFlushRegionRequest(regionName, writeFlushWalMarker);

    try {
      PayloadCarryingRpcController controller = rpcControllerFactory.newController();
      controller.setPriority(tableName);
      return stub.flushRegion(controller, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }
}
