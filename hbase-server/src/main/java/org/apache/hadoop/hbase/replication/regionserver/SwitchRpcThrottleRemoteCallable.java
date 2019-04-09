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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SwitchRpcThrottleRemoteStateData;

/**
 * The callable executed at RS side to switch rpc throttle state. <br/>
 */
@InterfaceAudience.Private
public class SwitchRpcThrottleRemoteCallable implements RSProcedureCallable {
  private HRegionServer rs;
  private boolean rpcThrottleEnabled;
  private Exception initError;

  @Override
  public Void call() throws Exception {
    if (initError != null) {
      throw initError;
    }
    rs.getRegionServerRpcQuotaManager().switchRpcThrottle(rpcThrottleEnabled);
    return null;
  }

  @Override
  public void init(byte[] parameter, HRegionServer rs) {
    this.rs = rs;
    try {
      SwitchRpcThrottleRemoteStateData param =
          SwitchRpcThrottleRemoteStateData.parseFrom(parameter);
      rpcThrottleEnabled = param.getRpcThrottleEnabled();
    } catch (InvalidProtocolBufferException e) {
      initError = e;
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.M_RS_SWITCH_RPC_THROTTLE;
  }
}
