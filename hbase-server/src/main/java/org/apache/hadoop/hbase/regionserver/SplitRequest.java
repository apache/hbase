/**
 *
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
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Handles processing region splits. Put in a queue, owned by HRegionServer.
 */
@InterfaceAudience.Private
class SplitRequest implements Runnable {
  static final Log LOG = LogFactory.getLog(SplitRequest.class);
  private final HRegion parent;
  private final byte[] midKey;
  private final HRegionServer server;
  private final User user;
  private TableLock tableLock;

  SplitRequest(HRegion region, byte[] midKey, HRegionServer hrs, User user) {
    Preconditions.checkNotNull(hrs);
    this.parent = region;
    this.midKey = midKey;
    this.server = hrs;
    this.user = user;
  }

  @Override
  public String toString() {
    return "regionName=" + parent + ", midKey=" + Bytes.toStringBinary(midKey);
  }

  private void doSplitting() {
    boolean success = false;
    server.metricsRegionServer.incrSplitRequest();
    long startTime = EnvironmentEdgeManager.currentTime();
    SplitTransaction st = new SplitTransaction(parent, midKey);
    try {
      //acquire a shared read lock on the table, so that table schema modifications
      //do not happen concurrently
      tableLock = server.getTableLockManager().readLock(parent.getTableDesc().getTableName()
          , "SPLIT_REGION:" + parent.getRegionNameAsString());
      try {
        tableLock.acquire();
      } catch (IOException ex) {
        tableLock = null;
        throw ex;
      }

      // If prepare does not return true, for some reason -- logged inside in
      // the prepare call -- we are not ready to split just now. Just return.
      if (!st.prepare()) return;
      try {
        st.execute(this.server, this.server);
        success = true;
      } catch (Exception e) {
        if (this.server.isStopping() || this.server.isStopped()) {
          LOG.info(
              "Skip rollback/cleanup of failed split of "
                  + parent.getRegionNameAsString() + " because server is"
                  + (this.server.isStopping() ? " stopping" : " stopped"), e);
          return;
        }
        if (e instanceof DroppedSnapshotException) {
          server.abort("Replay of WAL required. Forcing server shutdown", e);
          return;
        }
        try {
          LOG.info("Running rollback/cleanup of failed split of " +
            parent.getRegionNameAsString() + "; " + e.getMessage(), e);
          if (st.rollback(this.server, this.server)) {
            LOG.info("Successful rollback of failed split of " +
              parent.getRegionNameAsString());
          } else {
            this.server.abort("Abort; we got an error after point-of-no-return");
          }
        } catch (RuntimeException ee) {
          String msg = "Failed rollback of failed split of " +
            parent.getRegionNameAsString() + " -- aborting server";
          // If failed rollback, kill this server to avoid having a hole in table.
          LOG.info(msg, ee);
          this.server.abort(msg + " -- Cause: " + ee.getMessage());
        }
        return;
      }
    } catch (IOException ex) {
      LOG.error("Split failed " + this, RemoteExceptionHandler.checkIOException(ex));
      server.checkFileSystem();
    } finally {
      if (this.parent.getCoprocessorHost() != null) {
        try {
          this.parent.getCoprocessorHost().postCompleteSplit();
        } catch (IOException io) {
          LOG.error("Split failed " + this,
              RemoteExceptionHandler.checkIOException(io));
        }
      }
      if (parent.shouldForceSplit()) {
        parent.clearSplit();
      }
      releaseTableLock();
      long endTime = EnvironmentEdgeManager.currentTime();
      // Update regionserver metrics with the split transaction total running time
      server.metricsRegionServer.updateSplitTime(endTime - startTime);
      if (success) {
        server.metricsRegionServer.incrSplitSuccess();
        // Log success
        LOG.info("Region split, hbase:meta updated, and report to master. Parent="
            + parent.getRegionNameAsString() + ", new regions: "
            + st.getFirstDaughter().getRegionNameAsString() + ", "
            + st.getSecondDaughter().getRegionNameAsString() + ". Split took "
            + StringUtils.formatTimeDiff(EnvironmentEdgeManager.currentTime(), startTime));
      }
      // Always log the split transaction journal
      LOG.info("Split transaction journal:\n\t" + StringUtils.join("\n\t", st.getJournal()));
    }
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping split because server is stopping=" +
        this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }
    if (this.user == null) doSplitting();
    else {
      try {
        user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            doSplitting();
            return null;
          }
        });
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } catch (IOException ioe) {
        LOG.error("Encountered exception while splitting", ioe);
      }
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
                         + "on " + parent.getRegionNameAsString());
      }
    }
  }
}
