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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotRegionParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class SnapshotRegionCallable extends BaseRSProcedureCallable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegionCallable.class);

  private SnapshotDescription snapshot;
  private RegionInfo regionInfo;
  private ForeignExceptionDispatcher monitor;

  @Override
  protected void doCall() throws Exception {
    HRegion region = rs.getRegion(regionInfo.getEncodedName());
    if (region == null) {
      throw new NotServingRegionException(
        "snapshot=" + snapshot.getName() + ", region=" + regionInfo.getRegionNameAsString());
    }
    LOG.debug("Starting snapshot operation on {}", region);
    region.startRegionOperation(Region.Operation.SNAPSHOT);
    try {
      if (snapshot.getType() == SnapshotDescription.Type.FLUSH) {
        boolean succeeded = false;
        long readPt = region.getReadPoint(IsolationLevel.READ_COMMITTED);
        int retryTimes = rs.getConfiguration().getInt("hbase.snapshot.flush.retryTimes", 3);
        for (int i = 0; i < retryTimes; i++) {
          HRegion.FlushResult res = region.flush(true);
          if (res.getResult() == HRegion.FlushResult.Result.CANNOT_FLUSH) {
            region.waitForFlushes();
            if (region.getMaxFlushedSeqId() >= readPt) {
              succeeded = true;
              break;
            }
          } else {
            succeeded = true;
            break;
          }
        }
        if (!succeeded) {
          throw new IOException(
            "Unable to complete flush " + regionInfo.getRegionNameAsString() +
              " after " + retryTimes + " attempts");
        }
      }
      LOG.debug("Snapshotting region {} for {} completed.", region, snapshot.getName());
      region.addRegionToSnapshot(snapshot, monitor);
    } finally {
      LOG.debug("Closing snapshot operation on {}", region);
      region.closeRegionOperation(Region.Operation.SNAPSHOT);
    }
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    SnapshotRegionParameter param = SnapshotRegionParameter.parseFrom(parameter);
    this.snapshot = param.getSnapshot();
    this.regionInfo = ProtobufUtil.toRegionInfo(param.getRegion());
    this.monitor = new ForeignExceptionDispatcher(snapshot.getName());
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_SNAPSHOT_REGIONS;
  }
}
