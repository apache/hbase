/*
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

package org.apache.hadoop.hbase.client.locking;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.annotations.VisibleForTesting;

/**
 * Lock for HBase Entity either a Table, a Namespace, or Regions.
 *
 * These are remote locks which live on master, and need periodic heartbeats to keep them alive.
 * (Once we request the lock, internally an heartbeat thread will be started on the client).
 * If master does not receive the heartbeat in time, it'll release the lock and make it available
 * to other users.
 *
 * <p>Use {@link LockServiceClient} to build instances. Then call {@link #requestLock()}.
 * {@link #requestLock} will contact master to queue the lock and start the heartbeat thread
 * which will check lock's status periodically and once the lock is acquired, it will send the
 * heartbeats to the master.
 *
 * <p>Use {@link #await} or {@link #await(long, TimeUnit)} to wait for the lock to be acquired.
 * Always call {@link #unlock()} irrespective of whether lock was acquired or not. If the lock
 * was acquired, it'll be released. If it was not acquired, it is possible that master grants the
 * lock in future and the heartbeat thread keeps it alive forever by sending heartbeats.
 * Calling {@link #unlock()} will stop the heartbeat thread and cancel the lock queued on master.
 *
 * <p>There are 4 ways in which these remote locks may be released/can be lost:
 * <ul><li>Call {@link #unlock}.</li>
 * <li>Lock times out on master: Can happen because of network issues, GC pauses, etc.
 *     Worker thread will call the given abortable as soon as it detects such a situation.</li>
 * <li>Fail to contact master: If worker thread can not contact mater and thus fails to send
 *     heartbeat before the timeout expires, it assumes that lock is lost and calls the
 *     abortable.</li>
 * <li>Worker thread is interrupted.</li>
 * </ul>
 *
 * Use example:
 * <code>
 * EntityLock lock = lockServiceClient.*Lock(...., "exampled lock", abortable);
 * lock.requestLock();
 * ....
 * ....can do other initializations here since lock is 'asynchronous'...
 * ....
 * if (lock.await(timeout)) {
 *   ....logic requiring mutual exclusion
 * }
 * lock.unlock();
 * </code>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EntityLock {
  private static final Log LOG = LogFactory.getLog(EntityLock.class);

  public static final String HEARTBEAT_TIME_BUFFER =
      "hbase.client.locks.heartbeat.time.buffer.ms";

  private final AtomicBoolean locked = new AtomicBoolean(false);
  private final CountDownLatch latch = new CountDownLatch(1);

  private final LockService.BlockingInterface stub;
  private final LockHeartbeatWorker worker;
  private final LockRequest lockRequest;
  private final Abortable abort;

  // Buffer for unexpected delays (GC, network delay, etc) in heartbeat rpc.
  private final int heartbeatTimeBuffer;

  // set to a non-zero value for tweaking sleep time during testing so that worker doesn't wait
  // for long time periods between heartbeats.
  private long testingSleepTime = 0;

  private Long procId = null;

  /**
   * Abortable.abort() is called when the lease of the lock will expire.
   * It's up to the user decide if simply abort the process or handle the loss of the lock
   * by aborting the operation that was supposed to be under lock.
   */
  EntityLock(Configuration conf, LockService.BlockingInterface stub,
      LockRequest request, Abortable abort) {
    this.stub = stub;
    this.lockRequest = request;
    this.abort = abort;

    this.heartbeatTimeBuffer = conf.getInt(HEARTBEAT_TIME_BUFFER, 10000);
    this.worker = new LockHeartbeatWorker(lockRequest.getDescription());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("EntityLock locked=");
    sb.append(locked.get());
    sb.append(", procId=");
    sb.append(procId);
    sb.append(", type=");
    sb.append(lockRequest.getLockType());
    if (lockRequest.getRegionInfoCount() > 0) {
      sb.append(", regions=");
      for (int i = 0; i < lockRequest.getRegionInfoCount(); ++i) {
        if (i > 0) sb.append(", ");
        sb.append(lockRequest.getRegionInfo(i));
      }
    } else if (lockRequest.hasTableName()) {
      sb.append(", table=");
      sb.append(lockRequest.getTableName());
    } else if (lockRequest.hasNamespace()) {
      sb.append(", namespace=");
      sb.append(lockRequest.getNamespace());
    }
    sb.append(", description=");
    sb.append(lockRequest.getDescription());
    return sb.toString();
  }

  @VisibleForTesting
  void setTestingSleepTime(long timeInMillis) {
    testingSleepTime = timeInMillis;
  }

  @VisibleForTesting
  LockHeartbeatWorker getWorker() {
    return worker;
  }

  public boolean isLocked() {
    return locked.get();
  }

  /**
   * Sends rpc to the master to request lock.
   * The lock request is queued with other lock requests.
   */
  public void requestLock() throws IOException {
    if (procId == null) {
      try {
        procId = stub.requestLock(null, lockRequest).getProcId();
      } catch (Exception e) {
        throw ProtobufUtil.handleRemoteException(e);
      }
      worker.start();
    } else {
      LOG.info("Lock already queued : " + toString());
    }
  }

  /**
   * @param timeout in milliseconds. If set to 0, waits indefinitely.
   * @return true if lock was acquired; and false if waiting time elapsed before lock could be
   * acquired.
   */
  public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
    final boolean result = latch.await(timeout, timeUnit);
    String lockRequestStr = lockRequest.toString().replace("\n", ", ");
    if (result) {
      LOG.info("Acquired " + lockRequestStr);
    } else {
      LOG.info(String.format("Failed acquire in %s %s of %s", timeout, timeUnit.toString(),
          lockRequestStr));
    }
    return result;
  }

  public void await() throws InterruptedException {
    latch.await();
  }

  public void unlock() throws IOException {
    locked.set(false);
    worker.interrupt();
    Threads.shutdown(worker);
    try {
      stub.lockHeartbeat(null,
        LockHeartbeatRequest.newBuilder().setProcId(procId).setKeepAlive(false).build());
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  protected class LockHeartbeatWorker extends Thread {
    public LockHeartbeatWorker(final String desc) {
      super("LockHeartbeatWorker(" + desc + ")");
    }

    public void run() {
      final LockHeartbeatRequest lockHeartbeatRequest =
          LockHeartbeatRequest.newBuilder().setProcId(procId).build();

      LockHeartbeatResponse response;
      while (true) {
        try {
          response = stub.lockHeartbeat(null, lockHeartbeatRequest);
        } catch (Exception e) {
          e = ProtobufUtil.handleRemoteException(e);
          locked.set(false);
          LOG.error("Heartbeat failed, releasing " + EntityLock.this, e);
          abort.abort("Heartbeat failed", e);
          return;
        }
        if (!isLocked() && response.getLockStatus() == LockHeartbeatResponse.LockStatus.LOCKED) {
          locked.set(true);
          latch.countDown();
        } else if (isLocked() && response.getLockStatus() == LockHeartbeatResponse.LockStatus.UNLOCKED) {
          // Lock timed out.
          locked.set(false);
          abort.abort("Lock timed out.", null);
          return;
        }

        try {
          // If lock not acquired yet, poll faster so we can notify faster.
          long sleepTime = 1000;
          if (isLocked()) {
            // If lock acquired, then use lock timeout to determine heartbeat rate.
            // If timeout is <heartbeatTimeBuffer, send back to back heartbeats.
            sleepTime = Math.max(response.getTimeoutMs() - heartbeatTimeBuffer, 1);
          }
          if (testingSleepTime != 0) {
            sleepTime = testingSleepTime;
          }
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Since there won't be any more heartbeats, assume lock will be lost.
          locked.set(false);
          LOG.error("Interrupted, releasing " + EntityLock.this, e);
          abort.abort("Worker thread interrupted", e);
          return;
        }
      }
    }
  }
}
