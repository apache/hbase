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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of nonce manager that stores nonces in a hash map and cleans them up after
 * some time; if nonce group/client ID is supplied, nonces are stored by client ID.
 */
@InterfaceAudience.Private
public class ServerNonceManager {
  public static final String HASH_NONCE_GRACE_PERIOD_KEY = "hbase.server.hashNonce.gracePeriod";
  private static final Log LOG = LogFactory.getLog(ServerNonceManager.class);

  /** The time to wait in an extremely unlikely case of a conflict with a running op.
   * Only here so that tests could override it and not wait. */
  private int conflictWaitIterationMs = 30000;

  private static final SimpleDateFormat tsFormat = new SimpleDateFormat("HH:mm:ss.SSS");

  // This object is used to synchronize on in case of collisions, and for cleanup.
  private static class OperationContext {
    static final int DONT_PROCEED = 0;
    static final int PROCEED = 1;
    static final int WAIT = 2;

    // 0..1 - state, 2..2 - whether anyone is waiting, 3.. - ts of last activity
    private long data = 0;
    private static final long STATE_BITS = 3;
    private static final long WAITING_BIT = 4;
    private static final long ALL_FLAG_BITS = WAITING_BIT | STATE_BITS;

    private volatile long mvcc;

    @Override
    public String toString() {
      return "[state " + getState() + ", hasWait " + hasWait() + ", activity "
          + tsFormat.format(new Date(getActivityTime())) + "]";
    }

    public OperationContext() {
      setState(WAIT);
      reportActivity();
    }

    public void setState(int state) {
      this.data = (this.data & ~STATE_BITS) | state;
    }

    public int getState() {
      return (int)(this.data & STATE_BITS);
    }

    public void setHasWait() {
      this.data = this.data | WAITING_BIT;
    }

    public boolean hasWait() {
      return (this.data & WAITING_BIT) == WAITING_BIT;
    }

    public void reportActivity() {
      long now = EnvironmentEdgeManager.currentTime();
      this.data = (this.data & ALL_FLAG_BITS) | (now << 3);
    }

    public boolean isExpired(long minRelevantTime) {
      return getActivityTime() < (minRelevantTime & (~0l >>> 3));
    }

    private long getActivityTime() {
      return this.data >>> 3;
    }

    public void setMvcc(long mvcc) {
      this.mvcc = mvcc;
    }

    public long getMvcc() {
      return this.mvcc;
    }
  }

  /**
   * Nonces.
   * Approximate overhead per nonce: 64 bytes from hashmap, 32 from two objects (k/v),
   * NK: 16 bytes (2 longs), OC: 8 bytes (1 long) - so, 120 bytes.
   * With 30min expiration time, 5k increments/appends per sec., we'd use approximately 1Gb,
   * which is a realistic worst case. If it's much worse, we could use some sort of memory
   * limit and cleanup.
   */
  private ConcurrentHashMap<NonceKey, OperationContext> nonces =
      new ConcurrentHashMap<NonceKey, OperationContext>();

  private int deleteNonceGracePeriod;

  public ServerNonceManager(Configuration conf) {
    // Default - 30 minutes.
    deleteNonceGracePeriod = conf.getInt(HASH_NONCE_GRACE_PERIOD_KEY, 30 * 60 * 1000);
    if (deleteNonceGracePeriod < 60 * 1000) {
      LOG.warn("Nonce grace period " + deleteNonceGracePeriod
          + " is less than a minute; might be too small to be useful");
    }
  }

  @VisibleForTesting
  public void setConflictWaitIterationMs(int conflictWaitIterationMs) {
    this.conflictWaitIterationMs = conflictWaitIterationMs;
  }

  /**
   * Starts the operation if operation with such nonce has not already succeeded. If the
   * operation is in progress, waits for it to end and checks whether it has succeeded.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @param stoppable Stoppable that terminates waiting (if any) when the server is stopped.
   * @return true if the operation has not already succeeded and can proceed; false otherwise.
   */
  public boolean startOperation(long group, long nonce, Stoppable stoppable)
      throws InterruptedException {
    if (nonce == HConstants.NO_NONCE) return true;
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext ctx = new OperationContext();
    while (true) {
      OperationContext oldResult = nonces.putIfAbsent(nk, ctx);
      if (oldResult == null) return true;

      // Collision with some operation - should be extremely rare.
      synchronized (oldResult) {
        int oldState = oldResult.getState();
        LOG.debug("Conflict detected by nonce: " + nk + ", " + oldResult);
        if (oldState != OperationContext.WAIT) {
          return oldState == OperationContext.PROCEED; // operation ended
        }
        oldResult.setHasWait();
        oldResult.wait(this.conflictWaitIterationMs); // operation is still active... wait and loop
        if (stoppable.isStopped()) {
          throw new InterruptedException("Server stopped");
        }
      }
    }
  }

  /**
   * Ends the operation started by startOperation.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @param success Whether the operation has succeeded.
   */
  public void endOperation(long group, long nonce, boolean success) {
    if (nonce == HConstants.NO_NONCE) return;
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext newResult = nonces.get(nk);
    assert newResult != null;
    synchronized (newResult) {
      assert newResult.getState() == OperationContext.WAIT;
      // If we failed, other retries can proceed.
      newResult.setState(success ? OperationContext.DONT_PROCEED : OperationContext.PROCEED);
      if (success) {
        newResult.reportActivity(); // Set time to use for cleanup.
      } else {
        OperationContext val = nonces.remove(nk);
        assert val == newResult;
      }
      if (newResult.hasWait()) {
        LOG.debug("Conflict with running op ended: " + nk + ", " + newResult);
        newResult.notifyAll();
      }
    }
  }

  /**
   * Store the write point in OperationContext when the operation succeed.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @param mvcc Write point of the succeed operation.
   */
  public void addMvccToOperationContext(long group, long nonce, long mvcc) {
    if (nonce == HConstants.NO_NONCE) {
      return;
    }
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext result = nonces.get(nk);
    assert result != null;
    synchronized (result) {
      result.setMvcc(mvcc);
    }
  }

  /**
   * Return the write point of the previous succeed operation.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @return write point of the previous succeed operation.
   */
  public long getMvccFromOperationContext(long group, long nonce) {
    if (nonce == HConstants.NO_NONCE) {
      return Long.MAX_VALUE;
    }
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext result = nonces.get(nk);
    return result == null ? Long.MAX_VALUE : result.getMvcc();
  }

  /**
   * Reports the operation from WAL during replay.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @param writeTime Entry write time, used to ignore entries that are too old.
   */
  public void reportOperationFromWal(long group, long nonce, long writeTime) {
    if (nonce == HConstants.NO_NONCE) return;
    // Give the write time some slack in case the clocks are not synchronized.
    long now = EnvironmentEdgeManager.currentTime();
    if (now > writeTime + (deleteNonceGracePeriod * 1.5)) return;
    OperationContext newResult = new OperationContext();
    newResult.setState(OperationContext.DONT_PROCEED);
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext oldResult = nonces.putIfAbsent(nk, newResult);
    if (oldResult != null) {
      // Some schemes can have collisions (for example, expiring hashes), so just log it.
      // We have no idea about the semantics here, so this is the least of many evils.
      LOG.warn("Nonce collision during WAL recovery: " + nk
          + ", " + oldResult + " with " + newResult);
    }
  }

  /**
   * Creates a scheduled chore that is used to clean up old nonces.
   * @param stoppable Stoppable for the chore.
   * @return ScheduledChore; the scheduled chore is not started.
   */
  public ScheduledChore createCleanupScheduledChore(Stoppable stoppable) {
    // By default, it will run every 6 minutes (30 / 5).
    return new ScheduledChore("nonceCleaner", stoppable, deleteNonceGracePeriod / 5) {
      @Override
      protected void chore() {
        cleanUpOldNonces();
      }
    };
  }

  private void cleanUpOldNonces() {
    long cutoff = EnvironmentEdgeManager.currentTime() - deleteNonceGracePeriod;
    for (Map.Entry<NonceKey, OperationContext> entry : nonces.entrySet()) {
      OperationContext oc = entry.getValue();
      if (!oc.isExpired(cutoff)) continue;
      synchronized (oc) {
        if (oc.getState() == OperationContext.WAIT || !oc.isExpired(cutoff)) continue;
        nonces.remove(entry.getKey());
      }
    }
  }
}
