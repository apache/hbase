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

package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;

import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class SnapshotRegionHandler extends EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegionHandler.class);

  private final RegionInfo regionInfo;
  private final SnapshotDescription snapshot;
  private final long procId;
  private final ForeignExceptionDispatcher monitor;
  private final int retryTimes;

  public SnapshotRegionHandler(RegionServerServices server, RegionInfo region,
                               SnapshotDescription snapshot, long procId) {
    super(server, EventType.RS_SNAPSHOT_REGIONS);
    this.regionInfo = region;
    this.snapshot = snapshot;
    this.procId = procId;
    this.monitor = new ForeignExceptionDispatcher(snapshot.getName());
    this.retryTimes = server.getConfiguration().getInt("hbase.snapshot.flush.retrytimes", 3);
  }

  @Override
  public void process() throws IOException {
    Throwable error = null;
    try {
      HRegion region = ((HRegionServer) server).getRegion(regionInfo.getEncodedName());
      if (region == null) {
        throw new IOException("" + regionInfo.getEncodedName()
          + " but currently not serving");
      }
      LOG.debug("Starting snapshot operation on " + region);
      region.startRegionOperation(Operation.SNAPSHOT);
      try {
        if (snapshot.getType() == SnapshotDescription.Type.FLUSH) {
          boolean succeeded = false;
          long readPt = region.getReadPoint(IsolationLevel.READ_COMMITTED);
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
            throw new IOException("Unable to complete flush " + regionInfo.getEncodedName()
              + " after " + retryTimes + " attempts");
          }
          LOG.debug("Snapshotting region " + region.toString() + " completed.");
          region.addRegionToSnapshot(snapshot, monitor);
        }
      } finally {
        LOG.debug("Closing snapshot operation on " + region);
        region.closeRegionOperation(Operation.SNAPSHOT);
      }
    } catch (Throwable e) {
      error = e;
      LOG.warn("Received an error during running SnapshotRegionProcedure id={}, region={}",
        procId, regionInfo.getRegionNameAsString(), e);
    } finally {
      ((HRegionServer) server).remoteProcedureComplete(procId, error);
    }
  }
}
