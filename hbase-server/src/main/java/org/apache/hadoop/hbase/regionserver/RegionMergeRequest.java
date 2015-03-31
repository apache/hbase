/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Handles processing region merges. Put in a queue, owned by HRegionServer.
 */
@InterfaceAudience.Private
class RegionMergeRequest implements Runnable {
  static final Log LOG = LogFactory.getLog(RegionMergeRequest.class);
  private final HRegion region_a;
  private final HRegion region_b;
  private final HRegionServer server;
  private final boolean forcible;
  private TableLock tableLock;

  RegionMergeRequest(Region a, Region b, HRegionServer hrs, boolean forcible) {
    Preconditions.checkNotNull(hrs);
    this.region_a = (HRegion)a;
    this.region_b = (HRegion)b;
    this.server = hrs;
    this.forcible = forcible;
  }

  @Override
  public String toString() {
    return "MergeRequest,regions:" + region_a + ", " + region_b + ", forcible="
        + forcible;
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping merge because server is stopping="
          + this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }
    try {
      final long startTime = EnvironmentEdgeManager.currentTime();
      RegionMergeTransactionImpl mt = new RegionMergeTransactionImpl(region_a,
          region_b, forcible);

      //acquire a shared read lock on the table, so that table schema modifications
      //do not happen concurrently
      tableLock = server.getTableLockManager().readLock(region_a.getTableDesc().getTableName()
          , "MERGE_REGIONS:" + region_a.getRegionInfo().getRegionNameAsString() + ", " +
              region_b.getRegionInfo().getRegionNameAsString());
      try {
        tableLock.acquire();
      } catch (IOException ex) {
        tableLock = null;
        throw ex;
      }

      // If prepare does not return true, for some reason -- logged inside in
      // the prepare call -- we are not ready to merge just now. Just return.
      if (!mt.prepare(this.server)) return;
      try {
        mt.execute(this.server, this.server);
      } catch (Exception e) {
        if (this.server.isStopping() || this.server.isStopped()) {
          LOG.info(
              "Skip rollback/cleanup of failed merge of " + region_a + " and "
                  + region_b + " because server is"
                  + (this.server.isStopping() ? " stopping" : " stopped"), e);
          return;
        }
        try {
          LOG.warn("Running rollback/cleanup of failed merge of "
                  + region_a +" and "+ region_b + "; " + e.getMessage(), e);
          if (mt.rollback(this.server, this.server)) {
            LOG.info("Successful rollback of failed merge of "
                + region_a +" and "+ region_b);
          } else {
            this.server.abort("Abort; we got an error after point-of-no-return"
                + "when merging " + region_a + " and " + region_b);
          }
        } catch (RuntimeException ee) {
          String msg = "Failed rollback of failed merge of "
              + region_a +" and "+ region_b + " -- aborting server";
          // If failed rollback, kill this server to avoid having a hole in
          // table.
          LOG.info(msg, ee);
          this.server.abort(msg);
        }
        return;
      }
      LOG.info("Regions merged, hbase:meta updated, and report to master. region_a="
          + region_a + ", region_b=" + region_b + ",merged region="
          + mt.getMergedRegionInfo().getRegionNameAsString()
          + ". Region merge took "
          + StringUtils.formatTimeDiff(EnvironmentEdgeManager.currentTime(), startTime));
    } catch (IOException ex) {
      LOG.error("Merge failed " + this,
          RemoteExceptionHandler.checkIOException(ex));
      server.checkFileSystem();
    } finally {
      releaseTableLock();
    }
  }

  protected void releaseTableLock() {
    if (this.tableLock != null) {
      try {
        this.tableLock.release();
      } catch (IOException ex) {
        LOG.error("Could not release the table lock (something is really wrong). " 
           + "Aborting this server to avoid holding the lock forever.");
        this.server.abort("Abort; we got an error when releasing the table lock "
                         + "on " + region_a.getRegionInfo().getRegionNameAsString());
      }
    }
  }
}
