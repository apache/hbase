/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.wal.LogRollListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.util.HasThread;

/**
 * Runs periodically to determine if the HLog should be rolled.
 *
 * NOTE: This class extends HasThread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 */
public class LogRoller extends HasThread implements LogRollListener {
  static final Log LOG = LogFactory.getLog(LogRoller.class);

  /**
   * The minimum time in between requested log rolls.
   * In MS.
   */
  public static final String LOG_ROLL_REQUEST_PERIOD_KEY =
      "hbase.regionserver.logroll.request.period";
  public static final long LOG_ROLL_REQUEST_PERIOD_DEFAULT = TimeUnit.SECONDS.toMillis(60);

  private final ReentrantLock rollLock = new ReentrantLock();
  private final AtomicBoolean rollLog = new AtomicBoolean(false);
  private final HRegionServer server;
  private volatile long lastrolltime = System.currentTimeMillis();
  private volatile long lastrollRequestTime = System.currentTimeMillis();
  // Period to roll log.
  private final long rollperiod;
  private final int hlogIndexID;
  private final String logRollerName;
  private final long timeBetweenRequest;

  /** @param server */
  public LogRoller(final HRegionServer server, int hlogIndexID) {
    super();
    this.server = server;
    this.rollperiod =
      this.server.conf.getLong("hbase.regionserver.logroll.period", 3600000);
    this.timeBetweenRequest = this.server.conf.getLong(LOG_ROLL_REQUEST_PERIOD_KEY,
        LOG_ROLL_REQUEST_PERIOD_DEFAULT);
    this.hlogIndexID = hlogIndexID;
    this.logRollerName = "HLogRoller-" + hlogIndexID + " ";
  }

  @Override
  public void run() {
    MonitoredTask status = null;
    int retried = -1;
    while (!server.isStopRequestedAtStageTwo()) {
      long now = System.currentTimeMillis();
      boolean periodic = false;
      long modifiedRollPeriod;
      if (!rollLog.get()) {
        modifiedRollPeriod = this.rollperiod;
        periodic = (now - this.lastrolltime) > modifiedRollPeriod;
        if (!periodic) {
          synchronized (rollLog) {
            try {
              // default 10s
              rollLog.wait(server.threadWakeFrequency);
            } catch (InterruptedException e) {
              // Fall through
            }
          }
          continue;
        }
        // Time for periodic roll
        if (LOG.isDebugEnabled()) {
          if (modifiedRollPeriod == this.rollperiod) {
            LOG.debug(logRollerName + "roll period " + this.rollperiod + "ms elapsed");
          }
        }
      }
      rollLock.lock(); // FindBugs UL_UNRELEASED_LOCK_EXCEPTION_PATH
      try {
        byte [][] regionsToFlush = server.getLog(this.hlogIndexID).rollWriter();
        if (status != null) {
          status.markComplete(logRollerName + "Log rolling succeeded after " + retried +
              "retires.");
          retried = -1;
          status = null;
        }
        if (regionsToFlush != null) {
          for (byte [] r: regionsToFlush) scheduleFlush(r);
        }
      } catch (IOException ex) {
        retried++;
        String msg = logRollerName + "log roll failed." +
            " retried=" + retried + ", " + StringUtils.stringifyException(ex);
        if (status == null) {
          LOG.warn(logRollerName + "Log rolling failed with ioe. Will retry." +
              " Will update status with exceptionif retry fails " +
              RemoteExceptionHandler.checkIOException(ex));
          status = TaskMonitor.get().createStatus(msg);
        } else {
          status.setStatus(msg);
        }
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.fatal(logRollerName + "Log rolling failed, unexpected exception. Force Aborting",
            ex);
        server.forceAbort();
      } finally {
        this.lastrolltime = System.currentTimeMillis();
        rollLog.set(false);
        rollLock.unlock();
      }
    }
    if (status != null) {
      status.abort(logRollerName + "LogRoller exiting while log was unstable and roll pending");
    }
    LOG.info(logRollerName + "exiting.");
  }

  private void scheduleFlush(final byte [] region) {
    boolean scheduled = false;
    HRegion r = this.server.getOnlineRegion(region);
    FlushRequester requester = null;
    if (r != null) {
      requester = this.server.getFlushRequester();
      if (requester != null) {
        // If we do a selective flush, some column families might remain in
        // the memstore for a long time, and might cause old logs to
        // accumulate. Hence, we would not request for a selective flush.
        requester.request(r, false);
        scheduled = true;
      }
    }
    if (!scheduled) {
    LOG.warn(logRollerName + "Failed to schedule flush of " +
      Bytes.toString(region) + "r=" + r + ", requester=" + requester);
    }
  }

  public void logRollRequested() {
    long currentTime = System.currentTimeMillis();
    synchronized (rollLog) {
      if ((currentTime - lastrollRequestTime) > timeBetweenRequest) {
        lastrollRequestTime = currentTime;
        rollLog.set(true);
        rollLog.notifyAll();
      }
    }
  }

  /**
   * Called by region server to wake up this thread if it sleeping.
   * It is sleeping if rollLock is not held.
   */
  public void interruptIfNecessary() {
    try {
      rollLock.lock();
      this.interrupt();
    } finally {
      rollLock.unlock();
    }
  }
}
