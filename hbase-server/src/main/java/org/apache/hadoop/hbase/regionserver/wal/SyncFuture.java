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
package org.apache.hadoop.hbase.regionserver.wal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Future on a filesystem sync call. It given to a client or 'Handler' for it to wait on till the
 * sync completes.
 * <p>
 * Handlers coming in call append, append, append, and then do a flush/sync of the edits they have
 * appended the WAL before returning. Since sync takes a while to complete, we give the Handlers
 * back this sync future to wait on until the actual HDFS sync completes. Meantime this sync future
 * goes across a queue and is handled by a background thread; when it completes, it finishes up the
 * future, the handler get or failed check completes and the Handler can then progress.
 * <p>
 * This is just a partial implementation of Future; we just implement get and failure.
 * <p>
 * There is not a one-to-one correlation between dfs sync invocations and instances of this class. A
 * single dfs sync call may complete and mark many SyncFutures as done; i.e. we batch up sync calls
 * rather than do a dfs sync call every time a Handler asks for it.
 * <p>
 * SyncFutures are immutable but recycled. Call #reset(long, Span) before use even if it the first
 * time, start the sync, then park the 'hitched' thread on a call to #get().
 */
@InterfaceAudience.Private
class SyncFuture {

  private static final long NOT_DONE = -1L;
  private Thread t;

  /**
   * Lock protecting the thread-safe fields.
   */
  private final ReentrantLock doneLock;

  /**
   * Condition to wait on for client threads.
   */
  private final Condition doneCondition;

  /*
   * Fields below are protected by {@link SyncFuture#doneLock}.
   */

  /**
   * The transaction id that was set in here when we were marked done. Should be equal or > txnId.
   * Put this data member into the NOT_DONE state while this class is in use.
   */
  private long doneTxid;

  /**
   * If error, the associated throwable. Set when the future is 'done'.
   */
  private Throwable throwable;

  /*
   * Fields below are created once at reset() and accessed without any lock. Should be ok as they
   * are immutable for this instance of sync future until it is reset.
   */

  /**
   * The transaction id of this operation, monotonically increases.
   */
  private long txid;

  private boolean forceSync;

  SyncFuture() {
    this.doneLock = new ReentrantLock();
    this.doneCondition = doneLock.newCondition();
  }

  /**
   * Call this method to clear old usage and get it ready for new deploy.
   *
   * @param txid the new transaction id
   * @return this
   */
  SyncFuture reset(long txid, boolean forceSync) {
    if (t != null && t != Thread.currentThread()) {
      throw new IllegalStateException();
    }
    t = Thread.currentThread();
    if (!isDone()) {
      throw new IllegalStateException("" + txid + " " + Thread.currentThread());
    }
    this.doneTxid = NOT_DONE;
    this.forceSync = forceSync;
    this.txid = txid;
    this.throwable = null;
    return this;
  }

  @Override
  public String toString() {
    return "done=" + isDone() + ", txid=" + this.txid + " threadID=" + t.getId() +
        " threadName=" + t.getName();
  }

  long getTxid() {
    return this.txid;
  }

  boolean isForceSync() {
    return forceSync;
  }

  /**
   * Returns the thread that owned this sync future, use with caution as we return the reference to
   * the actual thread object.
   * @return the associated thread instance.
   */
  Thread getThread() {
    return t;
  }

  /**
   * @param txid the transaction id at which this future 'completed'.
   * @param t Can be null. Set if we are 'completing' on error (and this 't' is the error).
   * @return True if we successfully marked this outstanding future as completed/done. Returns false
   *         if this future is already 'done' when this method called.
   */
  boolean done(final long txid, final Throwable t) {
    doneLock.lock();
    try {
      if (doneTxid != NOT_DONE) {
        return false;
      }
      this.throwable = t;
      if (txid < this.txid) {
        // Something badly wrong.
        if (throwable == null) {
          this.throwable =
              new IllegalStateException("done txid=" + txid + ", my txid=" + this.txid);
        }
      }
      // Mark done.
      this.doneTxid = txid;
      doneCondition.signalAll();
      return true;
    } finally {
      doneLock.unlock();
    }
  }

  long get(long timeoutNs) throws InterruptedException,
      ExecutionException, TimeoutIOException {
    doneLock.lock();
    try {
      while (doneTxid == NOT_DONE) {
        if (!doneCondition.await(timeoutNs, TimeUnit.NANOSECONDS)) {
          throw new TimeoutIOException("Failed to get sync result after "
              + TimeUnit.NANOSECONDS.toMillis(timeoutNs) + " ms for txid=" + this.txid
              + ", WAL system stuck?");
        }
      }
      if (this.throwable != null) {
        throw new ExecutionException(this.throwable);
      }
      return this.doneTxid;
    } finally {
      doneLock.unlock();
    }
  }

  boolean isDone() {
    doneLock.lock();
    try {
      return this.doneTxid != NOT_DONE;
    } finally {
      doneLock.unlock();
    }
  }

  Throwable getThrowable() {
    doneLock.lock();
    try {
      if (doneTxid == NOT_DONE) {
        return null;
      }
      return this.throwable;
    } finally {
      doneLock.unlock();
    }
  }
}
