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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;

import com.google.common.annotations.VisibleForTesting;

/**
 * Runs periodically to determine if the WAL should be rolled.
 *
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 *
 * TODO: change to a pool of threads
 */
@InterfaceAudience.Private
@VisibleForTesting
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="JLM_JSR166_UTILCONCURRENT_MONITORENTER",
  justification="Use of an atomic type both as monitor and condition variable is intended")
public class LogRoller extends HasThread {
  private static final Log LOG = LogFactory.getLog(LogRoller.class);
  private final ReentrantLock rollLock = new ReentrantLock();
  private final AtomicBoolean rollLog = new AtomicBoolean(false);
  private final ConcurrentHashMap<WAL, RollController> wals =
      new ConcurrentHashMap<WAL, RollController>();
  private final Server server;
  protected final RegionServerServices services;
  // Period to roll log.
  private final long rollPeriod;
  private final int threadWakeFrequency;
  // The interval to check low replication on hlog's pipeline
  private final long checkLowReplicationInterval;

  public void addWAL(final WAL wal) {
    if (null == wals.putIfAbsent(wal, new RollController(wal))) {
      wal.registerWALActionsListener(new WALActionsListener.Base() {
        @Override
        public void logRollRequested(WALActionsListener.RollRequestReason reason) {
          // TODO logs will contend with each other here, replace with e.g. DelayedQueue
          synchronized(rollLog) {
            RollController controller = wals.get(wal);
            if (controller == null) {
              controller = new RollController(wal);
              wals.put(wal, controller);
            }
            controller.requestRoll();
            rollLog.set(true);
            rollLog.notifyAll();
          }
        }
      });
    }
  }

  public void requestRollAll() {
    for (RollController controller : wals.values()) {
      controller.requestRoll();
    }
    synchronized(rollLog) {
      rollLog.set(true);
      rollLog.notifyAll();
    }
  }

  /** @param server */
  public LogRoller(final Server server, final RegionServerServices services) {
    super("LogRoller");
    this.server = server;
    this.services = services;
    this.rollPeriod = this.server.getConfiguration().
      getLong("hbase.regionserver.logroll.period", 3600000);
    this.threadWakeFrequency = this.server.getConfiguration().
      getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.checkLowReplicationInterval = this.server.getConfiguration().getLong(
        "hbase.regionserver.hlog.check.lowreplication.interval", 30 * 1000);
  }

  @Override
  public void interrupt() {
    // Wake up if we are waiting on rollLog. For tests.
    synchronized (rollLog) {
      this.rollLog.notify();
    }
    super.interrupt();
  }

  /**
   * we need to check low replication in period, see HBASE-18132
   */
  void checkLowReplication(long now) {
    try {
      for (Entry<WAL, RollController> entry : wals.entrySet()) {
        WAL wal = entry.getKey();
        boolean neeRollAlready = entry.getValue().needsRoll(now);
        if(wal instanceof FSHLog && !neeRollAlready) {
          FSHLog hlog = (FSHLog)wal;
          if ((now - hlog.getLastTimeCheckLowReplication())
              > this.checkLowReplicationInterval) {
            hlog.checkLogRoll();
          }
        }
      }
    } catch (Throwable e) {
      LOG.warn("Failed checking low replication", e);
    }
  }

  @Override
  public void run() {
    while (!server.isStopped()) {
      long now = System.currentTimeMillis();
      checkLowReplication(now);
      if (!rollLog.get()) {
        boolean periodic = false;
        for (RollController controller : wals.values()) {
          if (controller.needsPeriodicRoll(now)) {
            periodic = true;
            break;
          }
        }
        if (!periodic) {
          synchronized (rollLog) {
            try {
              if (!rollLog.get()) {
                rollLog.wait(this.threadWakeFrequency);
              }
            } catch (InterruptedException e) {
              // Fall through
            }
          }
          continue;
        }
      }
      rollLock.lock(); // FindBugs UL_UNRELEASED_LOCK_EXCEPTION_PATH
      try {
        for (Entry<WAL, RollController> entry : wals.entrySet()) {
          final WAL wal = entry.getKey();
          RollController controller = entry.getValue();
          if (controller.isRollRequested()) {
            // WAL roll requested, fall through
            LOG.debug("WAL " + wal + " roll requested");
          } else if (controller.needsPeriodicRoll(now)) {
            // Time for periodic roll, fall through
            LOG.debug("WAL " + wal + " roll period " + this.rollPeriod + "ms elapsed");
          } else {
            continue;
          }
          // Force the roll if the logroll.period is elapsed or if a roll was requested.
          // The returned value is an array of actual region names.
          final byte [][] regionsToFlush = controller.rollWal(now);
          if (regionsToFlush != null) {
            for (byte [] r: regionsToFlush) scheduleFlush(r);
          }
        }
      } catch (FailedLogCloseException e) {
        server.abort("Failed log close in log roller", e);
      } catch (java.net.ConnectException e) {
        server.abort("Failed log close in log roller", e);
      } catch (IOException ex) {
        LOG.fatal("Aborting", ex);
        // Abort if we get here.  We probably won't recover an IOE. HBASE-1132
        server.abort("IOE in log roller",
          RemoteExceptionHandler.checkIOException(ex));
      } catch (Exception ex) {
        final String msg = "Failed rolling WAL; aborting to recover edits!";
        LOG.error(msg, ex);
        server.abort(msg, ex);
      } finally {
        try {
          rollLog.set(false);
        } finally {
          rollLock.unlock();
        }
      }
    }
    LOG.info("LogRoller exiting.");
  }

  /**
   * @param encodedRegionName Encoded name of region to flush.
   */
  private void scheduleFlush(final byte [] encodedRegionName) {
    boolean scheduled = false;
    Region r = this.services.getFromOnlineRegions(Bytes.toString(encodedRegionName));
    FlushRequester requester = null;
    if (r != null) {
      requester = this.services.getFlushRequester();
      if (requester != null) {
        // force flushing all stores to clean old logs
        requester.requestFlush(r, true);
        scheduled = true;
      }
    }
    if (!scheduled) {
      LOG.warn("Failed to schedule flush of " +
        Bytes.toString(encodedRegionName) + ", region=" + r + ", requester=" +
        requester);
    }
  }

  /**
   * For testing only
   * @return true if all WAL roll finished
   */
  @VisibleForTesting
  public boolean walRollFinished() {
    long now = System.currentTimeMillis();
    for (RollController controller : wals.values()) {
      if (controller.needsRoll(now)) {
        return false;
      }
    }
    return true;
  }


  /**
   * Independently control the roll of each wal. When use multiwal,
   * can avoid all wal roll together. see HBASE-24665 for detail
   */
  protected class RollController {
    private final WAL wal;
    private final AtomicBoolean rollRequest;
    private long lastRollTime;

    RollController(WAL wal) {
      this.wal = wal;
      this.rollRequest = new AtomicBoolean(false);
      this.lastRollTime = System.currentTimeMillis();
    }

    public void requestRoll() {
      this.rollRequest.set(true);
    }

    public byte[][] rollWal(long now) throws IOException {
      this.lastRollTime = now;
      byte[][] regionsToFlush = wal.rollWriter(true);
      this.rollRequest.set(false);
      return regionsToFlush;
    }

    public boolean isRollRequested() {
      return rollRequest.get();
    }

    public boolean needsPeriodicRoll(long now) {
      return (now - this.lastRollTime) > rollPeriod;
    }

    public boolean needsRoll(long now) {
      return isRollRequested() || needsPeriodicRoll(now);
    }
  }
}
