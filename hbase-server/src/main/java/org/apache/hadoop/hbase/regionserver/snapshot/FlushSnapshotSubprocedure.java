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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager.SnapshotSubprocedurePool;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;

/**
 * This online snapshot implementation uses the distributed procedure framework to force a
 * store flush and then records the hfiles.  Its enter stage does nothing.  Its leave stage then
 * flushes the memstore, builds the region server's snapshot manifest from its hfiles list, and
 * copies .regioninfos into the snapshot working directory.  At the master side, there is an atomic
 * rename of the working dir into the proper snapshot directory.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FlushSnapshotSubprocedure extends Subprocedure {
  private static final Logger LOG = LoggerFactory.getLogger(FlushSnapshotSubprocedure.class);

  private final List<HRegion> regions;
  private final SnapshotDescription snapshot;
  private final SnapshotSubprocedurePool taskManager;
  private boolean snapshotSkipFlush = false;

  // the maximum number of attempts we flush
  final static int MAX_RETRIES = 3;

  public FlushSnapshotSubprocedure(ProcedureMember member,
      ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
      List<HRegion> regions, SnapshotDescription snapshot,
      SnapshotSubprocedurePool taskManager) {
    super(member, snapshot.getName(), errorListener, wakeFrequency, timeout);
    this.snapshot = snapshot;

    if (this.snapshot.getType() == SnapshotDescription.Type.SKIPFLUSH) {
      snapshotSkipFlush = true;
    }
    this.regions = regions;
    this.taskManager = taskManager;
  }

  /**
   * Callable for adding files to snapshot manifest working dir.  Ready for multithreading.
   */
  public static class RegionSnapshotTask implements Callable<Void> {
    private HRegion region;
    private boolean skipFlush;
    private ForeignExceptionDispatcher monitor;
    private SnapshotDescription snapshotDesc;

    public RegionSnapshotTask(HRegion region, SnapshotDescription snapshotDesc,
        boolean skipFlush, ForeignExceptionDispatcher monitor) {
      this.region = region;
      this.skipFlush = skipFlush;
      this.monitor = monitor;
      this.snapshotDesc = snapshotDesc;
    }

    @Override
    public Void call() throws Exception {
      // Taking the region read lock prevents the individual region from being closed while a
      // snapshot is in progress.  This is helpful but not sufficient for preventing races with
      // snapshots that involve multiple regions and regionservers.  It is still possible to have
      // an interleaving such that globally regions are missing, so we still need the verification
      // step.
      LOG.debug("Starting snapshot operation on " + region);
      region.startRegionOperation(Operation.SNAPSHOT);
      try {
        if (skipFlush) {
        /*
         * This is to take an online-snapshot without force a coordinated flush to prevent pause
         * The snapshot type is defined inside the snapshot description. FlushSnapshotSubprocedure
         * should be renamed to distributedSnapshotSubprocedure, and the flush() behavior can be
         * turned on/off based on the flush type.
         * To minimized the code change, class name is not changed.
         */
          LOG.debug("take snapshot without flush memstore first");
        } else {
          LOG.debug("Flush Snapshotting region " + region.toString() + " started...");
          boolean succeeded = false;
          long readPt = region.getReadPoint(IsolationLevel.READ_COMMITTED);
          for (int i = 0; i < MAX_RETRIES; i++) {
            FlushResult res = region.flush(true);
            if (res.getResult() == FlushResult.Result.CANNOT_FLUSH) {
              // CANNOT_FLUSH may mean that a flush is already on-going
              // we need to wait for that flush to complete
              region.waitForFlushes();
              if (region.getMaxFlushedSeqId() >= readPt) {
                // writes at the start of the snapshot have been persisted
                succeeded = true;
                break;
              }
            } else {
              succeeded = true;
              break;
            }
          }
          if (!succeeded) {
            throw new IOException("Unable to complete flush after " + MAX_RETRIES + " attempts");
          }
        }
        region.addRegionToSnapshot(snapshotDesc, monitor);
        if (skipFlush) {
          LOG.debug("... SkipFlush Snapshotting region " + region.toString() + " completed.");
        } else {
          LOG.debug("... Flush Snapshotting region " + region.toString() + " completed.");
        }
      } finally {
        LOG.debug("Closing snapshot operation on " + region);
        region.closeRegionOperation(Operation.SNAPSHOT);
      }
      return null;
    }
  }

  private void flushSnapshot() throws ForeignException {
    if (regions.isEmpty()) {
      // No regions on this RS, we are basically done.
      return;
    }

    monitor.rethrowException();

    // assert that the taskManager is empty.
    if (taskManager.hasTasks()) {
      throw new IllegalStateException("Attempting to take snapshot "
          + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " but we currently have outstanding tasks");
    }

    // Add all hfiles already existing in region.
    for (HRegion region : regions) {
      // submit one task per region for parallelize by region.
      taskManager.submitTask(new RegionSnapshotTask(region, snapshot, snapshotSkipFlush, monitor));
      monitor.rethrowException();
    }

    // wait for everything to complete.
    LOG.debug("Flush Snapshot Tasks submitted for " + regions.size() + " regions");
    try {
      taskManager.waitForOutstandingTasks();
    } catch (InterruptedException e) {
      LOG.error("got interrupted exception for " + getMemberName());
      throw new ForeignException(getMemberName(), e);
    }
  }

  /**
   * do nothing, core of snapshot is executed in {@link #insideBarrier} step.
   */
  @Override
  public void acquireBarrier() throws ForeignException {
    // NO OP
  }

  /**
   * do a flush snapshot of every region on this rs from the target table.
   */
  @Override
  public byte[] insideBarrier() throws ForeignException {
    flushSnapshot();
    return new byte[0];
  }

  /**
   * Cancel threads if they haven't finished.
   */
  @Override
  public void cleanup(Exception e) {
    LOG.info("Aborting all online FLUSH snapshot subprocedure task threads for '"
        + snapshot.getName() + "' due to error", e);
    try {
      taskManager.cancelTasks();
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Hooray!
   */
  public void releaseBarrier() {
    // NO OP
  }

}
