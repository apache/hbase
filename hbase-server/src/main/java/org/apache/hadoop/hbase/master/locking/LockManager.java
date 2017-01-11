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
package org.apache.hadoop.hbase.master.locking;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.NonceKey;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Functions to acquire lock on table/namespace/regions.
 */
@InterfaceAudience.Private
public final class LockManager {
  private static final Log LOG = LogFactory.getLog(LockManager.class);
  private final HMaster master;
  private final RemoteLocks remoteLocks;

  public LockManager(HMaster master) {
    this.master = master;
    this.remoteLocks = new RemoteLocks();
  }

  public RemoteLocks remoteLocks() {
    return remoteLocks;
  }

  public MasterLock createMasterLock(final String namespace,
      final LockProcedure.LockType type, final String description) {
    return new MasterLock(namespace, type, description);
  }

  public MasterLock createMasterLock(final TableName tableName,
      final LockProcedure.LockType type, final String description) {
    return new MasterLock(tableName, type, description);
  }

  public MasterLock createMasterLock(final HRegionInfo[] regionInfos, final String description) {
    return new MasterLock(regionInfos, description);
  }

  private void submitProcedure(final LockProcedure proc, final long nonceGroup, final long nonce) {
    proc.setOwner(master.getMasterProcedureExecutor().getEnvironment().getRequestUser());
    final NonceKey nonceKey = master.getMasterProcedureExecutor().createNonceKey(nonceGroup, nonce);
    master.getMasterProcedureExecutor().submitProcedure(proc, nonceKey);
  }

  /**
   * Locks on namespace/table/regions.
   * Underneath, uses procedure framework and queues a {@link LockProcedure} which waits in a
   * queue until scheduled.
   * Use this lock instead LockManager.remoteLocks() for MASTER ONLY operations for two advantages:
   * - no need of polling on LockProcedure to check if lock was acquired.
   * - Generous timeout for lock preemption (default 10 min), no need to spawn thread for heartbeats.
   * (timeout configuration {@link LockProcedure#DEFAULT_LOCAL_MASTER_LOCKS_TIMEOUT_MS}).
   */
  public class MasterLock {
    private final String namespace;
    private final TableName tableName;
    private final HRegionInfo[] regionInfos;
    private final LockProcedure.LockType type;
    private final String description;

    private LockProcedure proc = null;

    public MasterLock(final String namespace,
        final LockProcedure.LockType type, final String description) {
      this.namespace = namespace;
      this.tableName = null;
      this.regionInfos = null;
      this.type = type;
      this.description = description;
    }

    public MasterLock(final TableName tableName,
        final LockProcedure.LockType type, final String description) {
      this.namespace = null;
      this.tableName = tableName;
      this.regionInfos = null;
      this.type = type;
      this.description = description;
    }

    public MasterLock(final HRegionInfo[] regionInfos, final String description) {
      this.namespace = null;
      this.tableName = null;
      this.regionInfos = regionInfos;
      this.type = LockProcedure.LockType.EXCLUSIVE;
      this.description = description;
    }

    /**
     * Acquire the lock, waiting indefinitely until the lock is released or
     * the thread is interrupted.
     * @throws InterruptedException If current thread is interrupted while
     *                              waiting for the lock
     */
    public boolean acquire() throws InterruptedException {
      return tryAcquire(0);
    }

    /**
     * Acquire the lock within a wait time.
     * @param timeoutMs The maximum time (in milliseconds) to wait for the lock,
     *                  0 to wait indefinitely
     * @return True if the lock was acquired, false if waiting time elapsed
     *         before the lock was acquired
     * @throws InterruptedException If the thread is interrupted while waiting to
     *                              acquire the lock
     */
    public boolean tryAcquire(final long timeoutMs) throws InterruptedException {
      if (proc != null && proc.isLocked()) {
        return true;
      }
      // Use new condition and procedure every time lock is requested.
      final CountDownLatch lockAcquireLatch = new CountDownLatch(1);
      if (regionInfos != null) {
        proc = new LockProcedure(master.getConfiguration(), regionInfos, type,
            description, lockAcquireLatch);
      } else if (tableName != null) {
        proc = new LockProcedure(master.getConfiguration(), tableName, type,
            description, lockAcquireLatch);
      } else if (namespace != null) {
        proc = new LockProcedure(master.getConfiguration(), namespace, type,
            description, lockAcquireLatch);
      } else {
        throw new UnsupportedOperationException("no namespace/table/region provided");
      }

      // The user of a MasterLock should be 'hbase', the only case where this is not true
      // is if from inside a coprocessor we try to take a master lock (which should be avoided)
      proc.setOwner(master.getMasterProcedureExecutor().getEnvironment().getRequestUser());
      master.getMasterProcedureExecutor().submitProcedure(proc);

      long deadline = (timeoutMs > 0) ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
      while (deadline >= System.currentTimeMillis() && !proc.isLocked()) {
        try {
          lockAcquireLatch.await(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.info("InterruptedException when waiting for lock: " + proc.toString());
          // kind of weird, releasing a lock which is not locked. This is to make the procedure
          // finish immediately whenever it gets scheduled so that it doesn't hold the lock.
          release();
          throw e;
        }
      }
      if (!proc.isLocked()) {
        LOG.info("Timed out waiting to acquire procedure lock: " + proc.toString());
        release();
        return false;
      }
      return true;
    }

    /**
     * Release the lock.
     * No-op if the lock was never acquired.
     */
    public void release() {
      if (proc != null) {
        proc.unlock(master.getMasterProcedureExecutor().getEnvironment());
      }
      proc = null;
    }

    @Override
    public String toString() {
      return "MasterLock: proc = " + proc.toString();
    }

    @VisibleForTesting
    LockProcedure getProc() {
      return proc;
    }
  }

  /**
   * Locks on namespace/table/regions for remote operations.
   * Since remote operations are unreliable and the client/RS may die anytime and never release
   * locks, regular heartbeats are required to keep the lock held.
   */
  public class RemoteLocks {
    public long requestNamespaceLock(final String namespace, final LockProcedure.LockType type,
        final String description, final long nonceGroup, final long nonce)
        throws IllegalArgumentException, IOException {
      master.getMasterCoprocessorHost().preRequestLock(namespace, null, null, type, description);

      final LockProcedure proc = new LockProcedure(master.getConfiguration(), namespace,
          type, description, null);
      submitProcedure(proc, nonceGroup, nonce);

      master.getMasterCoprocessorHost().postRequestLock(namespace, null, null, type, description);
      return proc.getProcId();
    }

    public long requestTableLock(final TableName tableName, final LockProcedure.LockType type,
        final String description, final long nonceGroup, final long nonce)
        throws IllegalArgumentException, IOException {
      master.getMasterCoprocessorHost().preRequestLock(null, tableName, null, type, description);

      final LockProcedure proc = new LockProcedure(master.getConfiguration(), tableName,
          type, description, null);
      submitProcedure(proc, nonceGroup, nonce);

      master.getMasterCoprocessorHost().postRequestLock(null, tableName, null, type, description);
      return proc.getProcId();
    }

    /**
     * @throws IllegalArgumentException if all regions are not from same table.
     */
    public long requestRegionsLock(final HRegionInfo[] regionInfos, final String description,
        final long nonceGroup, final long nonce) throws IllegalArgumentException, IOException {
      master.getMasterCoprocessorHost().preRequestLock(null, null, regionInfos,
            LockProcedure.LockType.EXCLUSIVE, description);

      final LockProcedure proc = new LockProcedure(master.getConfiguration(), regionInfos,
          LockProcedure.LockType.EXCLUSIVE, description, null);
      submitProcedure(proc, nonceGroup, nonce);

      master.getMasterCoprocessorHost().postRequestLock(null, null, regionInfos,
            LockProcedure.LockType.EXCLUSIVE, description);
      return proc.getProcId();
    }

    /**
     * @param keepAlive if false, release the lock.
     * @return true, if procedure is found and it has the lock; else false.
     */
    public boolean lockHeartbeat(final long procId, final boolean keepAlive) throws IOException {
      final LockProcedure proc = master.getMasterProcedureExecutor()
        .getProcedure(LockProcedure.class, procId);
      if (proc == null) return false;

      master.getMasterCoprocessorHost().preLockHeartbeat(proc, keepAlive);

      proc.updateHeartBeat();
      if (!keepAlive) {
        proc.unlock(master.getMasterProcedureExecutor().getEnvironment());
      }

      master.getMasterCoprocessorHost().postLockHeartbeat(proc, keepAlive);

      return proc.isLocked();
    }
  }
}
