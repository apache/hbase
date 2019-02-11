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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ScannerCallable.MoreResults;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This class has the logic for handling scanners for regions with and without replicas.
 * 1. A scan is attempted on the default (primary) region
 * 2. The scanner sends all the RPCs to the default region until it is done, or, there
 * is a timeout on the default (a timeout of zero is disallowed).
 * 3. If there is a timeout in (2) above, scanner(s) is opened on the non-default replica(s)
 * 4. The results from the first successful scanner are taken, and it is stored which server
 * returned the results.
 * 5. The next RPCs are done on the above stored server until it is done or there is a timeout,
 * in which case, the other replicas are queried (as in (3) above).
 *
 */
@InterfaceAudience.Private
class ScannerCallableWithReplicas implements RetryingCallable<Result[]> {
  private static final Logger LOG = LoggerFactory.getLogger(ScannerCallableWithReplicas.class);
  volatile ScannerCallable currentScannerCallable;
  AtomicBoolean replicaSwitched = new AtomicBoolean(false);
  final ConnectionImplementation cConnection;
  protected final ExecutorService pool;
  protected final int timeBeforeReplicas;
  private final Scan scan;
  private final int retries;
  private Result lastResult;
  private final RpcRetryingCaller<Result[]> caller;
  private final TableName tableName;
  private Configuration conf;
  private int scannerTimeout;
  private Set<ScannerCallable> outstandingCallables = new HashSet<>();
  private boolean someRPCcancelled = false; //required for testing purposes only
  private int regionReplication = 0;

  public ScannerCallableWithReplicas(TableName tableName, ConnectionImplementation cConnection,
      ScannerCallable baseCallable, ExecutorService pool, int timeBeforeReplicas, Scan scan,
      int retries, int scannerTimeout, int caching, Configuration conf,
      RpcRetryingCaller<Result[]> caller) {
    this.currentScannerCallable = baseCallable;
    this.cConnection = cConnection;
    this.pool = pool;
    if (timeBeforeReplicas < 0) {
      throw new IllegalArgumentException("Invalid value of operation timeout on the primary");
    }
    this.timeBeforeReplicas = timeBeforeReplicas;
    this.scan = scan;
    this.retries = retries;
    this.tableName = tableName;
    this.conf = conf;
    this.scannerTimeout = scannerTimeout;
    this.caller = caller;
  }

  public void setClose() {
    currentScannerCallable.setClose();
  }

  public void setRenew(boolean val) {
    currentScannerCallable.setRenew(val);
  }

  public void setCaching(int caching) {
    currentScannerCallable.setCaching(caching);
  }

  public int getCaching() {
    return currentScannerCallable.getCaching();
  }

  public HRegionInfo getHRegionInfo() {
    return currentScannerCallable.getHRegionInfo();
  }

  public MoreResults moreResultsInRegion() {
    return currentScannerCallable.moreResultsInRegion();
  }

  public MoreResults moreResultsForScan() {
    return currentScannerCallable.moreResultsForScan();
  }

  @Override
  public Result [] call(int timeout) throws IOException {
    // If the active replica callable was closed somewhere, invoke the RPC to
    // really close it. In the case of regular scanners, this applies. We make couple
    // of RPCs to a RegionServer, and when that region is exhausted, we set
    // the closed flag. Then an RPC is required to actually close the scanner.
    if (currentScannerCallable != null && currentScannerCallable.closed) {
      // For closing we target that exact scanner (and not do replica fallback like in
      // the case of normal reads)
      if (LOG.isTraceEnabled()) {
        LOG.trace("Closing scanner id=" + currentScannerCallable.scannerId);
      }
      Result[] r = currentScannerCallable.call(timeout);
      currentScannerCallable = null;
      return r;
    }
    // We need to do the following:
    //1. When a scan goes out to a certain replica (default or not), we need to
    //   continue to hit that until there is a failure. So store the last successfully invoked
    //   replica
    //2. We should close the "losing" scanners (scanners other than the ones we hear back
    //   from first)
    //
    // Since RegionReplication is a table attribute, it wont change as long as table is enabled,
    // it just needs to be set once.

    if (regionReplication <= 0) {
      RegionLocations rl = null;
      try {
        rl = RpcRetryingCallerWithReadReplicas.getRegionLocations(true,
          RegionReplicaUtil.DEFAULT_REPLICA_ID, cConnection, tableName,
          currentScannerCallable.getRow());
      } catch (RetriesExhaustedException | DoNotRetryIOException e) {
        // We cannot get the primary replica region location, it is possible that the region server
        // hosting meta table is down, it needs to proceed to try cached replicas directly.
        rl = cConnection.getCachedLocation(tableName, currentScannerCallable.getRow());
        if (rl == null) {
          throw e;
        }
      }
      regionReplication = rl.size();
    }
    // allocate a boundedcompletion pool of some multiple of number of replicas.
    // We want to accomodate some RPCs for redundant replica scans (but are still in progress)
    ResultBoundedCompletionService<Pair<Result[], ScannerCallable>> cs =
        new ResultBoundedCompletionService<>(
            RpcRetryingCallerFactory.instantiate(ScannerCallableWithReplicas.this.conf), pool,
            regionReplication * 5);

    AtomicBoolean done = new AtomicBoolean(false);
    replicaSwitched.set(false);
    // submit call for the primary replica.
    addCallsForCurrentReplica(cs);
    int startIndex = 0;

    try {
      // wait for the timeout to see whether the primary responds back
      Future<Pair<Result[], ScannerCallable>> f = cs.poll(timeBeforeReplicas,
          TimeUnit.MICROSECONDS); // Yes, microseconds
      if (f != null) {
        // After poll, if f is not null, there must be a completed task
        Pair<Result[], ScannerCallable> r = f.get();
        if (r != null && r.getSecond() != null) {
          updateCurrentlyServingReplica(r.getSecond(), r.getFirst(), done, pool);
        }
        return r == null ? null : r.getFirst(); //great we got a response
      }
    } catch (ExecutionException e) {
      // We ignore the ExecutionException and continue with the replicas
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scan with primary region returns " + e.getCause());
      }

      // If rl's size is 1 or scan's consitency is strong, it needs to throw
      // out the exception from the primary replica
      if ((regionReplication == 1) || (scan.getConsistency() == Consistency.STRONG)) {
        // Rethrow the first exception
        RpcRetryingCallerWithReadReplicas.throwEnrichedException(e, retries);
      }

      startIndex = 1;
    } catch (CancellationException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }

    // submit call for the all of the secondaries at once
    int endIndex = regionReplication;
    if (scan.getConsistency() == Consistency.STRONG) {
      // When scan's consistency is strong, do not send to the secondaries
      endIndex = 1;
    } else {
      // TODO: this may be an overkill for large region replication
      addCallsForOtherReplicas(cs, 0, regionReplication - 1);
    }

    try {
      Future<Pair<Result[], ScannerCallable>> f = cs.pollForFirstSuccessfullyCompletedTask(timeout,
          TimeUnit.MILLISECONDS, startIndex, endIndex);

      if (f == null) {
        throw new IOException("Failed to get result within timeout, timeout=" + timeout + "ms");
      }
      Pair<Result[], ScannerCallable> r = f.get();

      if (r != null && r.getSecond() != null) {
        updateCurrentlyServingReplica(r.getSecond(), r.getFirst(), done, pool);
      }
      return r == null ? null : r.getFirst(); // great we got an answer

    } catch (ExecutionException e) {
      RpcRetryingCallerWithReadReplicas.throwEnrichedException(e, retries);
    } catch (CancellationException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } finally {
      // We get there because we were interrupted or because one or more of the
      // calls succeeded or failed. In all case, we stop all our tasks.
      cs.cancelAll();
    }
    LOG.error("Imposible? Arrive at an unreachable line..."); // unreachable
    throw new IOException("Imposible? Arrive at an unreachable line...");
  }

  private void updateCurrentlyServingReplica(ScannerCallable scanner, Result[] result,
      AtomicBoolean done, ExecutorService pool) {
    if (done.compareAndSet(false, true)) {
      if (currentScannerCallable != scanner) replicaSwitched.set(true);
      currentScannerCallable = scanner;
      // store where to start the replica scanner from if we need to.
      if (result != null && result.length != 0) this.lastResult = result[result.length - 1];
      if (LOG.isTraceEnabled()) {
        LOG.trace("Setting current scanner as id=" + currentScannerCallable.scannerId +
            " associated with replica=" + currentScannerCallable.getHRegionInfo().getReplicaId());
      }
      // close all outstanding replica scanners but the one we heard back from
      outstandingCallables.remove(scanner);
      for (ScannerCallable s : outstandingCallables) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Closing scanner id=" + s.scannerId +
            ", replica=" + s.getHRegionInfo().getRegionId() +
            " because slow and replica=" +
            this.currentScannerCallable.getHRegionInfo().getReplicaId() + " succeeded");
        }
        // Submit the "close" to the pool since this might take time, and we don't
        // want to wait for the "close" to happen yet. The "wait" will happen when
        // the table is closed (when the awaitTermination of the underlying pool is called)
        s.setClose();
        final RetryingRPC r = new RetryingRPC(s);
        pool.submit(new Callable<Void>(){
          @Override
          public Void call() throws Exception {
            r.call(scannerTimeout);
            return null;
          }
        });
      }
      // now clear outstandingCallables since we scheduled a close for all the contained scanners
      outstandingCallables.clear();
    }
  }

  /**
   * When a scanner switches in the middle of scanning (the 'next' call fails
   * for example), the upper layer {@link ClientScanner} needs to know
   */
  public boolean switchedToADifferentReplica() {
    return replicaSwitched.get();
  }

  /**
   * @return true when the most recent RPC response indicated that the response was a heartbeat
   *         message. Heartbeat messages are sent back from the server when the processing of the
   *         scan request exceeds a certain time threshold. Heartbeats allow the server to avoid
   *         timeouts during long running scan operations.
   */
  public boolean isHeartbeatMessage() {
    return currentScannerCallable != null && currentScannerCallable.isHeartbeatMessage();
  }

  public Cursor getCursor() {
    return currentScannerCallable != null ? currentScannerCallable.getCursor() : null;
  }

  private void addCallsForCurrentReplica(
      ResultBoundedCompletionService<Pair<Result[], ScannerCallable>> cs) {
    RetryingRPC retryingOnReplica = new RetryingRPC(currentScannerCallable);
    outstandingCallables.add(currentScannerCallable);
    cs.submit(retryingOnReplica, scannerTimeout, currentScannerCallable.id);
  }

  private void addCallsForOtherReplicas(
      ResultBoundedCompletionService<Pair<Result[], ScannerCallable>> cs, int min, int max) {

    for (int id = min; id <= max; id++) {
      if (currentScannerCallable.id == id) {
        continue; //this was already scheduled earlier
      }
      ScannerCallable s = currentScannerCallable.getScannerCallableForReplica(id);
      setStartRowForReplicaCallable(s);
      outstandingCallables.add(s);
      RetryingRPC retryingOnReplica = new RetryingRPC(s);
      cs.submit(retryingOnReplica, scannerTimeout, id);
    }
  }

  /**
   * Set the start row for the replica callable based on the state of the last result received.
   * @param callable The callable to set the start row on
   */
  private void setStartRowForReplicaCallable(ScannerCallable callable) {
    if (this.lastResult == null || callable == null) {
      return;
    }
    // 1. The last result was a partial result which means we have not received all of the cells
    // for this row. Thus, use the last result's row as the start row. If a replica switch
    // occurs, the scanner will ensure that any accumulated partial results are cleared,
    // and the scan can resume from this row.
    // 2. The last result was not a partial result which means it contained all of the cells for
    // that row (we no longer need any information from it). Set the start row to the next
    // closest row that could be seen.
    callable.getScan().withStartRow(this.lastResult.getRow(), this.lastResult.mayHaveMoreCellsInRow());
  }

  @VisibleForTesting
  boolean isAnyRPCcancelled() {
    return someRPCcancelled;
  }

  class RetryingRPC implements RetryingCallable<Pair<Result[], ScannerCallable>>, Cancellable {
    final ScannerCallable callable;
    RpcRetryingCaller<Result[]> caller;
    private volatile boolean cancelled = false;

    RetryingRPC(ScannerCallable callable) {
      this.callable = callable;
      // For the Consistency.STRONG (default case), we reuse the caller
      // to keep compatibility with what is done in the past
      // For the Consistency.TIMELINE case, we can't reuse the caller
      // since we could be making parallel RPCs (caller.callWithRetries is synchronized
      // and we can't invoke it multiple times at the same time)
      this.caller = ScannerCallableWithReplicas.this.caller;
      if (scan.getConsistency() == Consistency.TIMELINE) {
        this.caller = RpcRetryingCallerFactory.instantiate(ScannerCallableWithReplicas.this.conf)
            .<Result[]>newCaller();
      }
    }

    @Override
    public Pair<Result[], ScannerCallable> call(int callTimeout) throws IOException {
      // since the retries is done within the ResultBoundedCompletionService,
      // we don't invoke callWithRetries here
      if (cancelled) {
        return null;
      }
      Result[] res = this.caller.callWithoutRetries(this.callable, callTimeout);
      return new Pair<>(res, this.callable);
    }

    @Override
    public void prepare(boolean reload) throws IOException {
      if (cancelled) return;

      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }

      callable.prepare(reload);
    }

    @Override
    public void throwable(Throwable t, boolean retrying) {
      callable.throwable(t, retrying);
    }

    @Override
    public String getExceptionMessageAdditionalDetail() {
      return callable.getExceptionMessageAdditionalDetail();
    }

    @Override
    public long sleep(long pause, int tries) {
      return callable.sleep(pause, tries);
    }

    @Override
    public void cancel() {
      cancelled = true;
      caller.cancel();
      if (callable.getRpcController() != null) {
        callable.getRpcController().startCancel();
      }
      someRPCcancelled = true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }
  }

  @Override
  public void prepare(boolean reload) throws IOException {
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    currentScannerCallable.throwable(t, retrying);
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return currentScannerCallable.getExceptionMessageAdditionalDetail();
  }

  @Override
  public long sleep(long pause, int tries) {
    return currentScannerCallable.sleep(pause, tries);
  }
}
