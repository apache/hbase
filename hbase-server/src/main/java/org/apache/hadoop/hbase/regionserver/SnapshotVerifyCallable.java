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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotVerifyUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotVerifyParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class SnapshotVerifyCallable extends BaseRSProcedureCallable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotVerifyCallable.class);

  private SnapshotDescription snapshot;
  private List<RegionInfo> regions;

  @Override
  protected void doCall() throws Exception {
    Configuration conf = rs.getConfiguration();
    TableName tableName = TableName.valueOf(snapshot.getTable());
    SnapshotVerifyUtil.verifySnapshot(conf, snapshot, tableName, regions, false, true);
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    SnapshotVerifyParameter param = SnapshotVerifyParameter.parseFrom(parameter);
    this.snapshot = param.getSnapshot();
    this.regions = param.getRegionList().stream()
      .map(ProtobufUtil::toRegionInfo).collect(Collectors.toList());
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_VERIFY_SNAPSHOT;
  }
}
