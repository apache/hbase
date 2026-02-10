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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ClaimReplicationQueueRemoteParameter;

@InterfaceAudience.Private
public class ClaimReplicationQueueCallable extends BaseRSProcedureCallable {

  private ReplicationQueueId queueId;

  @Override
  public EventType getEventType() {
    return EventType.RS_CLAIM_REPLICATION_QUEUE;
  }

  @Override
  protected byte[] doCall() throws Exception {
    PeerProcedureHandler handler = rs.getReplicationSourceService().getPeerProcedureHandler();
    handler.claimReplicationQueue(queueId);
    return null;
  }

  @Override
  protected void initParameter(byte[] parameter) throws InvalidProtocolBufferException {
    ClaimReplicationQueueRemoteParameter param =
      ClaimReplicationQueueRemoteParameter.parseFrom(parameter);
    ServerName crashedServer = ProtobufUtil.toServerName(param.getCrashedServer());
    String queue = param.getQueue();
    if (param.hasSourceServer()) {
      ServerName sourceServer = ProtobufUtil.toServerName(param.getSourceServer());
      queueId = new ReplicationQueueId(crashedServer, queue, sourceServer);
    } else {
      queueId = new ReplicationQueueId(crashedServer, queue);
    }
  }
}
