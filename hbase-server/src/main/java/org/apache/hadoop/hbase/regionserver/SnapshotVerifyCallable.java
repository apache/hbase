/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotVerifyParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class SnapshotVerifyCallable extends BaseRSProcedureCallable {
  private SnapshotDescription snapshot;
  private RegionInfo region;

  @Override
  protected void doCall() throws Exception {
    rs.getRsSnapshotVerifier().verifyRegion(snapshot, region);
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    SnapshotVerifyParameter param = SnapshotVerifyParameter.parseFrom(parameter);
    this.snapshot = param.getSnapshot();
    this.region = ProtobufUtil.toRegionInfo(param.getRegion());
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_VERIFY_SNAPSHOT;
  }
}
