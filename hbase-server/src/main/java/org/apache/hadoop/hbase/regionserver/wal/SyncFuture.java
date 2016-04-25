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
package org.apache.hadoop.hbase.regionserver.wal;

import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.htrace.Span;

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
  // Implementation notes: I tried using a cyclicbarrier in here for handler and sync threads
  // to coordinate on but it did not give any obvious advantage and some issues with order in which
  // events happen.
  private static final long NOT_DONE = -1L;

  /**
   * The transaction id of this operation, monotonically increases.
   */
  private long txid;

  /**
   * The transaction id that was set in here when we were marked done. Should be equal or > txnId.
   * Put this data member into the NOT_DONE state while this class is in use.
   */
  private long doneTxid;

  /**
   * If error, the associated throwable. Set when the future is 'done'.
   */
  private Throwable throwable;

  private Thread t;

  /**
   * Optionally carry a disconnected scope to the SyncRunner.
   */
  private Span span;

  SyncFuture(long txid, Span span) {
    this.t = Thread.currentThread();
    this.txid = txid;
    this.span = span;
    this.doneTxid = NOT_DONE;
  }

  /**
   * Call this method to clear old usage and get it ready for new deploy.
   * @param txid the new transaction id
   * @param span current span, detached from caller. Don't forget to attach it when resuming after a
   *          call to {@link #get()}.
   * @return this
   */
  synchronized SyncFuture reset(final long txid, Span span) {
    if (t != null && t != Thread.currentThread()) {
      throw new IllegalStateException();
    }
    t = Thread.currentThread();
    if (!isDone()) {
      throw new IllegalStateException("" + txid + " " + Thread.currentThread());
    }
    this.doneTxid = NOT_DONE;
    this.txid = txid;
    this.span = span;
    return this;
  }

  @Override
  public synchronized String toString() {
    return "done=" + isDone() + ", txid=" + this.txid;
  }

  synchronized long getTxid() {
    return this.txid;
  }

  /**
   * Retrieve the {@code span} instance from this Future. EventHandler calls this method to continue
   * the span. Thread waiting on this Future musn't call this method until AFTER calling
   * {@link #get()} and the future has been released back to the originating thread.
   */
  synchronized Span getSpan() {
    return this.span;
  }

  /**
   * Used to re-attach a {@code span} to the Future. Called by the EventHandler after a it has
   * completed processing and detached the span from its scope.
   */
  synchronized void setSpan(Span span) {
    this.span = span;
  }

  /**
   * @param txid the transaction id at which this future 'completed'.
   * @param t Can be null. Set if we are 'completing' on error (and this 't' is the error).
   * @return True if we successfully marked this outstanding future as completed/done. Returns false
   *         if this future is already 'done' when this method called.
   */
  synchronized boolean done(final long txid, final Throwable t) {
    if (isDone()) {
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
    // Wake up waiting threads.
    notify();
    return true;
  }

  boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException();
  }

  synchronized long get() throws InterruptedException, ExecutionException {
    while (!isDone()) {
      wait(1000);
    }
    if (this.throwable != null) {
      throw new ExecutionException(this.throwable);
    }
    return this.doneTxid;
  }

  synchronized boolean isDone() {
    return this.doneTxid != NOT_DONE;
  }

  synchronized boolean isThrowable() {
    return isDone() && getThrowable() != null;
  }

  synchronized Throwable getThrowable() {
    return this.throwable;
  }
}
