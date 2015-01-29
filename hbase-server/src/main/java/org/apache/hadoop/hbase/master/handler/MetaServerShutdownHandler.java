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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shutdown handler for the server hosting <code>hbase:meta</code>
 */
@InterfaceAudience.Private
public class MetaServerShutdownHandler extends ServerShutdownHandler {
  private static final Log LOG = LogFactory.getLog(MetaServerShutdownHandler.class);
  private AtomicInteger eventExceptionCount = new AtomicInteger(0);
  @VisibleForTesting
  static final int SHOW_STRACKTRACE_FREQUENCY = 100;

  public MetaServerShutdownHandler(final Server server,
      final MasterServices services,
      final DeadServer deadServers, final ServerName serverName) {
    super(server, services, deadServers, serverName,
      EventType.M_META_SERVER_SHUTDOWN, true);
  }

  @Override
  public void process() throws IOException {
    boolean gotException = true; 
    try {
      AssignmentManager am = this.services.getAssignmentManager();
      this.services.getMasterFileSystem().setLogRecoveryMode();
      boolean distributedLogReplay = 
        (this.services.getMasterFileSystem().getLogRecoveryMode() == RecoveryMode.LOG_REPLAY);
      try {
        if (this.shouldSplitWal) {
          LOG.info("Splitting hbase:meta logs for " + serverName);
          if (distributedLogReplay) {
            Set<HRegionInfo> regions = new HashSet<HRegionInfo>();
            regions.add(HRegionInfo.FIRST_META_REGIONINFO);
            this.services.getMasterFileSystem().prepareLogReplay(serverName, regions);
          } else {
            this.services.getMasterFileSystem().splitMetaLog(serverName);
          }
          am.getRegionStates().logSplit(HRegionInfo.FIRST_META_REGIONINFO);
        }
      } catch (IOException ioe) {
        this.services.getExecutorService().submit(this);
        this.deadServers.add(serverName);
        throw new IOException("failed log splitting for " + serverName + ", will retry", ioe);
      }
  
      // Assign meta if we were carrying it.
      // Check again: region may be assigned to other where because of RIT
      // timeout
      if (am.isCarryingMeta(serverName)) {
        LOG.info("Server " + serverName + " was carrying META. Trying to assign.");
        am.regionOffline(HRegionInfo.FIRST_META_REGIONINFO);
        verifyAndAssignMetaWithRetries();
      } else if (!server.getMetaTableLocator().isLocationAvailable(this.server.getZooKeeper())) {
        // the meta location as per master is null. This could happen in case when meta assignment
        // in previous run failed, while meta znode has been updated to null. We should try to
        // assign the meta again.
        verifyAndAssignMetaWithRetries();
      } else {
        LOG.info("META has been assigned to otherwhere, skip assigning.");
      }

      try {
        if (this.shouldSplitWal && distributedLogReplay) {
          if (!am.waitOnRegionToClearRegionsInTransition(HRegionInfo.FIRST_META_REGIONINFO,
            regionAssignmentWaitTimeout)) {
            // Wait here is to avoid log replay hits current dead server and incur a RPC timeout
            // when replay happens before region assignment completes.
            LOG.warn("Region " + HRegionInfo.FIRST_META_REGIONINFO.getEncodedName()
                + " didn't complete assignment in time");
          }
          this.services.getMasterFileSystem().splitMetaLog(serverName);
        }
      } catch (Exception ex) {
        if (ex instanceof IOException) {
          this.services.getExecutorService().submit(this);
          this.deadServers.add(serverName);
          throw new IOException("failed log splitting for " + serverName + ", will retry", ex);
        } else {
          throw new IOException(ex);
        }
      }

      gotException = false;
    } finally {
      if (gotException){
        // If we had an exception, this.deadServers.finish will be skipped in super.process()
        this.deadServers.finish(serverName);
      }     
    }

    super.process();
    // Clear this counter on successful handling.
    this.eventExceptionCount.set(0);
  }

  @Override
  boolean isCarryingMeta() {
    return true;
  }

  /**
   * Before assign the hbase:meta region, ensure it haven't
   *  been assigned by other place
   * <p>
   * Under some scenarios, the hbase:meta region can be opened twice, so it seemed online
   * in two regionserver at the same time.
   * If the hbase:meta region has been assigned, so the operation can be canceled.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private void verifyAndAssignMeta()
      throws InterruptedException, IOException, KeeperException {
    long timeout = this.server.getConfiguration().
        getLong("hbase.catalog.verification.timeout", 1000);
    if (!server.getMetaTableLocator().verifyMetaRegionLocation(server.getConnection(),
      this.server.getZooKeeper(), timeout)) {
      this.services.getAssignmentManager().assignMeta(HRegionInfo.FIRST_META_REGIONINFO);
    } else if (serverName.equals(server.getMetaTableLocator().getMetaRegionLocation(
      this.server.getZooKeeper()))) {
      throw new IOException("hbase:meta is onlined on the dead server "
          + serverName);
    } else {
      LOG.info("Skip assigning hbase:meta, because it is online on the "
          + server.getMetaTableLocator().getMetaRegionLocation(this.server.getZooKeeper()));
    }
  }

  /**
   * Failed many times, shutdown processing
   * @throws IOException
   */
  private void verifyAndAssignMetaWithRetries() throws IOException {
    int iTimes = this.server.getConfiguration().getInt(
        "hbase.catalog.verification.retries", 10);

    long waitTime = this.server.getConfiguration().getLong(
        "hbase.catalog.verification.timeout", 1000);

    int iFlag = 0;
    while (true) {
      try {
        verifyAndAssignMeta();
        break;
      } catch (KeeperException e) {
        this.server.abort("In server shutdown processing, assigning meta", e);
        throw new IOException("Aborting", e);
      } catch (Exception e) {
        if (iFlag >= iTimes) {
          this.server.abort("verifyAndAssignMeta failed after" + iTimes
              + " times retries, aborting", e);
          throw new IOException("Aborting", e);
        }
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
          LOG.warn("Interrupted when is the thread sleep", e1);
          Thread.currentThread().interrupt();
          throw (InterruptedIOException)new InterruptedIOException().initCause(e1);
        }
        iFlag++;
      }
    }
  }

  @Override
  protected void handleException(Throwable t) {
    int count = eventExceptionCount.getAndIncrement();
    if (count < 0) count = eventExceptionCount.getAndSet(0);
    if (count > SHOW_STRACKTRACE_FREQUENCY) { // Too frequent, let's slow reporting
      Threads.sleep(1000);
    }
    if (count % SHOW_STRACKTRACE_FREQUENCY == 0) {
      LOG.error("Caught " + eventType + ", count=" + this.eventExceptionCount, t); 
    } else {
      LOG.error("Caught " + eventType + ", count=" + this.eventExceptionCount +
        "; " + t.getMessage() + "; stack trace shows every " + SHOW_STRACKTRACE_FREQUENCY +
        "th time.");
    }
  }
}
