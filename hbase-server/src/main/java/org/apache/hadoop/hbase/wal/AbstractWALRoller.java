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
package org.apache.hadoop.hbase.wal;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs periodically to determine if the WAL should be rolled.
 * <p/>
 * NOTE: This class extends Thread rather than Chore because the sleep time can be interrupted when
 * there is something to do, rather than the Chore sleep time which is invariant.
 * <p/>
 * The {@link #scheduleFlush(String, List)} is abstract here,
 * as sometimes we hold a region without a region server but we still want to roll its WAL.
 * <p/>
 * TODO: change to a pool of threads
 */
@InterfaceAudience.Private
public abstract class AbstractWALRoller<T extends Abortable> extends Thread
  implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWALRoller.class);

  protected static final String WAL_ROLL_PERIOD_KEY = "hbase.regionserver.logroll.period";

  /**
   * Configure for the timeout of log rolling retry.
   */
  public static final String WAL_ROLL_WAIT_TIMEOUT = "hbase.regionserver.logroll.wait.timeout.ms";
  public static final long DEFAULT_WAL_ROLL_WAIT_TIMEOUT = 30000;

  /**
   * Configure for the max count of log rolling retry.
   * The real retry count is also limited by the timeout of log rolling
   * via {@link #WAL_ROLL_WAIT_TIMEOUT}
   */
  protected static final String WAL_ROLL_RETRIES = "hbase.regionserver.logroll.retries";

  protected final ConcurrentMap<WAL, RollController> wals = new ConcurrentHashMap<>();
  protected final T abortable;
  // Period to roll log.
  private final long rollPeriod;
  private final int threadWakeFrequency;
  // The interval to check low replication on hlog's pipeline
  private final long checkLowReplicationInterval;
  // Wait period for roll log
  private final long rollWaitTimeout;
  // Max retry for roll log
  private final int maxRollRetry;

  private volatile boolean running = true;

  public void addWAL(WAL wal) {
    // check without lock first
    if (wals.containsKey(wal)) {
      return;
    }
    // this is to avoid race between addWAL and requestRollAll.
    synchronized (this) {
      if (wals.putIfAbsent(wal, new RollController(wal)) == null) {
        wal.registerWALActionsListener(new WALActionsListener() {
          @Override
          public void logRollRequested(WALActionsListener.RollRequestReason reason) {
            // TODO logs will contend with each other here, replace with e.g. DelayedQueue
            synchronized (AbstractWALRoller.this) {
              RollController controller = wals.computeIfAbsent(wal, rc -> new RollController(wal));
              controller.requestRoll();
              AbstractWALRoller.this.notifyAll();
            }
          }

          @Override
          public void postLogArchive(Path oldPath, Path newPath) throws IOException {
            afterWALArchive(oldPath, newPath);
          }
        });
      }
    }
  }

  public void requestRollAll() {
    synchronized (this) {
      for (RollController controller : wals.values()) {
        controller.requestRoll();
      }
      notifyAll();
    }
  }

  protected AbstractWALRoller(String name, Configuration conf, T abortable) {
    super(name);
    this.abortable = abortable;
    this.rollPeriod = conf.getLong(WAL_ROLL_PERIOD_KEY, 3600000);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.checkLowReplicationInterval =
      conf.getLong("hbase.regionserver.hlog.check.lowreplication.interval", 30 * 1000);
    this.rollWaitTimeout = conf.getLong(WAL_ROLL_WAIT_TIMEOUT, DEFAULT_WAL_ROLL_WAIT_TIMEOUT);
    // retry rolling does not have to be the default behavior, so the default value is 0 here
    this.maxRollRetry = conf.getInt(WAL_ROLL_RETRIES, 0);
  }

  /**
   * we need to check low replication in period, see HBASE-18132
   */
  private void checkLowReplication(long now) {
    try {
      for (Entry<WAL, RollController> entry : wals.entrySet()) {
        WAL wal = entry.getKey();
        boolean needRollAlready = entry.getValue().needsRoll(now);
        if (needRollAlready || !(wal instanceof AbstractFSWAL)) {
          continue;
        }
        ((AbstractFSWAL<?>) wal).checkLogLowReplication(checkLowReplicationInterval);
      }
    } catch (Throwable e) {
      LOG.warn("Failed checking low replication", e);
    }
  }

  private void abort(String reason, Throwable cause) {
    // close all WALs before calling abort on RS.
    // This is because AsyncFSWAL replies on us for rolling a new writer to make progress, and if we
    // failed, AsyncFSWAL may be stuck, so we need to close it to let the upper layer know that it
    // is already broken.
    for (WAL wal : wals.keySet()) {
      // shutdown rather than close here since we are going to abort the RS and the wals need to be
      // split when recovery
      try {
        wal.shutdown();
      } catch (IOException e) {
        LOG.warn("Failed to shutdown wal", e);
      }
    }
    abortable.abort(reason, cause);
  }

  @Override
  public void run() {
    while (running) {
      long now = System.currentTimeMillis();
      checkLowReplication(now);
      synchronized (this) {
        if (wals.values().stream().noneMatch(rc -> rc.needsRoll(now))) {
          try {
            wait(this.threadWakeFrequency);
          } catch (InterruptedException e) {
            // restore the interrupt state
            Thread.currentThread().interrupt();
          }
          // goto the beginning to check whether again whether we should fall through to roll
          // several WALs, and also check whether we should quit.
          continue;
        }
      }
      try {
        for (Iterator<Entry<WAL, RollController>> iter = wals.entrySet().iterator();
             iter.hasNext();) {
          Entry<WAL, RollController> entry = iter.next();
          WAL wal = entry.getKey();
          RollController controller = entry.getValue();
          if (controller.isRollRequested()) {
            // WAL roll requested, fall through
            LOG.debug("WAL {} roll requested", wal);
          } else if (controller.needsPeriodicRoll(now)) {
            // Time for periodic roll, fall through
            LOG.debug("WAL {} roll period {} ms elapsed", wal, this.rollPeriod);
          } else {
            continue;
          }
          Map<byte[], List<byte[]>> regionsToFlush = null;
          int nAttempts = 0;
          long startWaiting = System.currentTimeMillis();
          do {
            try {
              // Force the roll if the logroll.period is elapsed or if a roll was requested.
              // The returned value is an collection of actual region and family names.
              regionsToFlush = controller.rollWal(System.currentTimeMillis());
              break;
            } catch (IOException ioe) {
              long waitingTime = System.currentTimeMillis() - startWaiting;
              if (waitingTime < rollWaitTimeout && nAttempts < maxRollRetry) {
                nAttempts++;
                LOG.warn("Retry to roll log, nAttempts={}, waiting time={}ms, sleeping 1s to retry,"
                    + " last exception", nAttempts, waitingTime, ioe);
                sleep(1000);
              } else {
                LOG.error("Roll wal failed and waiting timeout, will not retry", ioe);
                throw ioe;
              }
            }
          } while (EnvironmentEdgeManager.currentTime() - startWaiting < rollWaitTimeout);
          if (regionsToFlush != null) {
            for (Map.Entry<byte[], List<byte[]>> r : regionsToFlush.entrySet()) {
              scheduleFlush(Bytes.toString(r.getKey()), r.getValue());
            }
          }
        }
      } catch (FailedLogCloseException | ConnectException e) {
        abort("Failed log close in log roller", e);
      } catch (IOException ex) {
        // Abort if we get here. We probably won't recover an IOE. HBASE-1132
        abort("IOE in log roller",
          ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex);
      } catch (Exception ex) {
        LOG.error("Log rolling failed", ex);
        abort("Log rolling failed", ex);
      }
    }
    LOG.info("LogRoller exiting.");
  }

  protected void afterWALArchive(Path oldPath, Path newPath) {
  }

  /**
   * @param encodedRegionName Encoded name of region to flush.
   * @param families stores of region to flush.
   */
  protected abstract void scheduleFlush(String encodedRegionName, List<byte[]> families);

  private boolean isWaiting() {
    Thread.State state = getState();
    return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
  }

  /**
   * @return true if all WAL roll finished
   */
  public boolean walRollFinished() {
    return wals.values().stream().noneMatch(rc -> rc.needsRoll(System.currentTimeMillis()))
      && isWaiting();
  }

  /**
   * Wait until all wals have been rolled after calling {@link #requestRollAll()}.
   */
  public void waitUntilWalRollFinished() throws InterruptedException {
    while (!walRollFinished()) {
      Thread.sleep(100);
    }
  }

  @Override
  public void close() {
    running = false;
    interrupt();
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

    public Map<byte[], List<byte[]>> rollWal(long now) throws IOException {
      this.lastRollTime = now;
      // reset the flag in front to avoid missing roll request before we return from rollWriter.
      this.rollRequest.set(false);
      return wal.rollWriter(true);
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
