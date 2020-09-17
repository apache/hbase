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
package org.apache.hadoop.hbase.procedure.flush;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.flush.RegionServerFlushTableProcedureManager.FlushTableSubprocedurePool;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This flush region implementation uses the distributed procedure framework to flush
 * table regions.
 * Its acquireBarrier stage does nothing.  Its insideBarrier stage flushes the regions.
 */
@InterfaceAudience.Private
public class FlushTableSubprocedure extends Subprocedure {
  private static final Logger LOG = LoggerFactory.getLogger(FlushTableSubprocedure.class);

  private final String table;
  private final String family;
  private final List<HRegion> regions;
  private final FlushTableSubprocedurePool taskManager;

  public FlushTableSubprocedure(ProcedureMember member,
      ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
      List<HRegion> regions, String table, String family,
      FlushTableSubprocedurePool taskManager) {
    super(member, table, errorListener, wakeFrequency, timeout);
    this.table = table;
    this.family = family;
    this.regions = regions;
    this.taskManager = taskManager;
  }

  private static class RegionFlushTask implements Callable<Void> {
    HRegion region;
    List<byte[]> families;
    RegionFlushTask(HRegion region, List<byte[]> families) {
      this.region = region;
      this.families = families;
    }

    @Override
    public Void call() throws Exception {
      LOG.debug("Starting region operation on " + region);
      region.startRegionOperation();
      try {
        LOG.debug("Flush region " + region.toString() + " started...");
        if (families == null) {
          region.flush(true);
        } else {
          region.flushcache(families, false, FlushLifeCycleTracker.DUMMY);
        }
        // TODO: flush result is not checked?
      } finally {
        LOG.debug("Closing region operation on " + region);
        region.closeRegionOperation();
      }
      return null;
    }
  }

  private void flushRegions() throws ForeignException {
    if (regions.isEmpty()) {
      // No regions on this RS, we are basically done.
      return;
    }

    monitor.rethrowException();

    // assert that the taskManager is empty.
    if (taskManager.hasTasks()) {
      throw new IllegalStateException("Attempting to flush "
          + table + " but we currently have outstanding tasks");
    }
    List<byte[]> families = null;
    if (family != null) {
      LOG.debug("About to flush family {} on all regions for table {}", family, table);
      families = Collections.singletonList(Bytes.toBytes(family));
    }
    // Add all hfiles already existing in region.
    for (HRegion region : regions) {
      // submit one task per region for parallelize by region.
      taskManager.submitTask(new RegionFlushTask(region, families));
      monitor.rethrowException();
    }

    // wait for everything to complete.
    LOG.debug("Flush region tasks submitted for " + regions.size() + " regions");
    try {
      taskManager.waitForOutstandingTasks();
    } catch (InterruptedException e) {
      throw new ForeignException(getMemberName(), e);
    }
  }

  /**
   * Flush the online regions on this rs for the target table.
   */
  @Override
  public void acquireBarrier() throws ForeignException {
    flushRegions();
  }

  @Override
  public byte[] insideBarrier() throws ForeignException {
    // No-Op
    return new byte[0];
  }

  /**
   * Cancel threads if they haven't finished.
   */
  @Override
  public void cleanup(Exception e) {
    LOG.info("Aborting all flush region subprocedure task threads for '"
        + table + "' due to error", e);
    try {
      taskManager.cancelTasks();
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
    }
  }

  public void releaseBarrier() {
    // NO OP
  }

}
