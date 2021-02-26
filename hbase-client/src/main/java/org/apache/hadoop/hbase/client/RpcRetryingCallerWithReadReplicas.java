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


import static org.apache.hadoop.hbase.HConstants.PRIORITY_UNSET;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Caller that goes to replica if the primary region does no answer within a configurable
 * timeout. If the timeout is reached, it calls all the secondary replicas, and returns
 * the first answer. If the answer comes from one of the secondary replica, it will
 * be marked as stale.
 */
@InterfaceAudience.Private
public class RpcRetryingCallerWithReadReplicas {
  private static final Logger LOG =
      LoggerFactory.getLogger(RpcRetryingCallerWithReadReplicas.class);

  protected final ExecutorService pool;
  protected final ClusterConnection cConnection;
  protected final Configuration conf;
  protected final Get get;
  protected final TableName tableName;
  protected final int timeBeforeReplicas;
  private final int operationTimeout;
  private final int rpcTimeout;
  private final int retries;
  private final RpcControllerFactory rpcControllerFactory;
  private final RpcRetryingCallerFactory rpcRetryingCallerFactory;

  public RpcRetryingCallerWithReadReplicas(
      RpcControllerFactory rpcControllerFactory, TableName tableName,
      ClusterConnection cConnection, final Get get,
      ExecutorService pool, int retries, int operationTimeout, int rpcTimeout,
      int timeBeforeReplicas) {
    this.rpcControllerFactory = rpcControllerFactory;
    this.tableName = tableName;
    this.cConnection = cConnection;
    this.conf = cConnection.getConfiguration();
    this.get = get;
    this.pool = pool;
    this.retries = retries;
    this.operationTimeout = operationTimeout;
    this.rpcTimeout = rpcTimeout;
    this.timeBeforeReplicas = timeBeforeReplicas;
    this.rpcRetryingCallerFactory = new RpcRetryingCallerFactory(conf);
  }

  /**
   * A RegionServerCallable that takes into account the replicas, i.e.
   * - the call can be on any replica
   * - we need to stop retrying when the call is completed
   * - we can be interrupted
   */
  class ReplicaRegionServerCallable extends CancellableRegionServerCallable<Result> {
    final int id;
    public ReplicaRegionServerCallable(int id, HRegionLocation location) {
      super(RpcRetryingCallerWithReadReplicas.this.cConnection,
          RpcRetryingCallerWithReadReplicas.this.tableName, get.getRow(),
          rpcControllerFactory.newController(), rpcTimeout, new RetryingTimeTracker(), PRIORITY_UNSET);
      this.id = id;
      this.location = location;
    }

    /**
     * Two responsibilities
     * - if the call is already completed (by another replica) stops the retries.
     * - set the location to the right region, depending on the replica.
     */
    @Override
    // TODO: Very like the super class implemenation. Can we shrink this down?
    public void prepare(final boolean reload) throws IOException {
      if (getRpcController().isCanceled()) return;
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }
      if (reload || location == null) {
        RegionLocations rl = getRegionLocations(false, id, cConnection, tableName, get.getRow());
        location = id < rl.size() ? rl.getRegionLocation(id) : null;
      }

      if (location == null || location.getServerName() == null) {
        // With this exception, there will be a retry. The location can be null for a replica
        //  when the table is created or after a split.
        throw new HBaseIOException("There is no location for replica id #" + id);
      }

      setStubByServiceName(this.location.getServerName());
    }

    @Override
    // TODO: Very like the super class implemenation. Can we shrink this down?
    protected Result rpcCall() throws Exception {
      if (getRpcController().isCanceled()) return null;
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }
      byte[] reg = location.getRegionInfo().getRegionName();
      ClientProtos.GetRequest request = RequestConverter.buildGetRequest(reg, get);
      HBaseRpcController hrc = (HBaseRpcController)getRpcController();
      hrc.reset();
      hrc.setCallTimeout(rpcTimeout);
      hrc.setPriority(tableName);
      ClientProtos.GetResponse response = getStub().get(hrc, request);
      if (response == null) {
        return null;
      }
      return ProtobufUtil.toResult(response.getResult(), hrc.cellScanner());
    }
  }

  /**
   * <p>
   * Algo:
   * - we put the query into the execution pool.
   * - after x ms, if we don't have a result, we add the queries for the secondary replicas
   * - we take the first answer
   * - when done, we cancel what's left. Cancelling means:
   * - removing from the pool if the actual call was not started
   * - interrupting the call if it has started
   * Client side, we need to take into account
   * - a call is not executed immediately after being put into the pool
   * - a call is a thread. Let's not multiply the number of thread by the number of replicas.
   * Server side, if we can cancel when it's still in the handler pool, it's much better, as a call
   * can take some i/o.
   * </p>
   * Globally, the number of retries, timeout and so on still applies, but it's per replica,
   * not global. We continue until all retries are done, or all timeouts are exceeded.
   */
  public Result call(int operationTimeout)
      throws DoNotRetryIOException, InterruptedIOException, RetriesExhaustedException {
    boolean isTargetReplicaSpecified = (get.getReplicaId() >= 0);

    RegionLocations rl = null;
    boolean skipPrimary = false;
    try {
      rl = getRegionLocations(true,
        (isTargetReplicaSpecified ? get.getReplicaId() : RegionReplicaUtil.DEFAULT_REPLICA_ID),
        cConnection, tableName, get.getRow());
    } catch (RetriesExhaustedException | DoNotRetryIOException e) {
      // When there is no specific replica id specified. It just needs to load all replicas.
      if (isTargetReplicaSpecified) {
        throw e;
      } else {
        // We cannot get the primary replica location, it is possible that the region
        // server hosting meta is down, it needs to proceed to try cached replicas.
        if (cConnection instanceof ConnectionImplementation) {
          rl = ((ConnectionImplementation)cConnection).getCachedLocation(tableName, get.getRow());
          if (rl == null) {
            // No cached locations
            throw e;
          }

          // Primary replica location is not known, skip primary replica
          skipPrimary = true;
        } else {
          // For completeness
          throw e;
        }
      }
    }

    final ResultBoundedCompletionService<Result> cs =
        new ResultBoundedCompletionService<>(this.rpcRetryingCallerFactory, pool, rl.size());
    int startIndex = 0;
    int endIndex = rl.size();

    if(isTargetReplicaSpecified) {
      addCallsForReplica(cs, rl, get.getReplicaId(), get.getReplicaId());
      endIndex = 1;
    } else {
      if (!skipPrimary) {
        addCallsForReplica(cs, rl, 0, 0);
        try {
          // wait for the timeout to see whether the primary responds back
          Future<Result> f = cs.poll(timeBeforeReplicas, TimeUnit.MICROSECONDS); // Yes, microseconds
          if (f != null) {
            return f.get(); //great we got a response
          }
          if (cConnection.getConnectionMetrics() != null) {
            cConnection.getConnectionMetrics().incrHedgedReadOps();
          }
        } catch (ExecutionException e) {
          // We ignore the ExecutionException and continue with the secondary replicas
          if (LOG.isDebugEnabled()) {
            LOG.debug("Primary replica returns " + e.getCause());
          }

          // Skip the result from the primary as we know that there is something wrong
          startIndex = 1;
        } catch (CancellationException e) {
          throw new InterruptedIOException();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      } else {
        // Since primary replica is skipped, the endIndex needs to be adjusted accordingly
        endIndex --;
      }

      // submit call for the all of the secondaries at once
      addCallsForReplica(cs, rl, 1, rl.size() - 1);
    }
    try {
      ResultBoundedCompletionService<Result>.QueueingFuture<Result> f =
          cs.pollForFirstSuccessfullyCompletedTask(operationTimeout, TimeUnit.MILLISECONDS, startIndex, endIndex);
      if (f == null) {
        throw new RetriesExhaustedException("Timed out after " + operationTimeout +
            "ms. Get is sent to replicas with startIndex: " + startIndex +
            ", endIndex: " + endIndex + ", Locations: " + rl);
      }
      if (cConnection.getConnectionMetrics() != null && !isTargetReplicaSpecified &&
          !skipPrimary && f.getReplicaId() != RegionReplicaUtil.DEFAULT_REPLICA_ID) {
        cConnection.getConnectionMetrics().incrHedgedReadWin();
      }
      return f.get();
    } catch (ExecutionException e) {
      throwEnrichedException(e, retries);
    } catch (CancellationException e) {
      throw new InterruptedIOException();
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    } finally {
      // We get there because we were interrupted or because one or more of the
      // calls succeeded or failed. In all case, we stop all our tasks.
      cs.cancelAll();
    }

    LOG.error("Imposible? Arrive at an unreachable line..."); // unreachable
    return null; // unreachable
  }

  /**
   * Extract the real exception from the ExecutionException, and throws what makes more
   * sense.
   */
  static void throwEnrichedException(ExecutionException e, int retries)
      throws RetriesExhaustedException, DoNotRetryIOException {
    Throwable t = e.getCause();
    assert t != null; // That's what ExecutionException is about: holding an exception
    t.printStackTrace();

    if (t instanceof RetriesExhaustedException) {
      throw (RetriesExhaustedException) t;
    }

    if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t;
    }

    RetriesExhaustedException.ThrowableWithExtraContext qt =
        new RetriesExhaustedException.ThrowableWithExtraContext(t,
            EnvironmentEdgeManager.currentTime(), null);

    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions =
        Collections.singletonList(qt);

    throw new RetriesExhaustedException(retries, exceptions);
  }

  /**
   * Creates the calls and submit them
   *
   * @param cs  - the completion service to use for submitting
   * @param rl  - the region locations
   * @param min - the id of the first replica, inclusive
   * @param max - the id of the last replica, inclusive.
   */
  private void addCallsForReplica(ResultBoundedCompletionService<Result> cs,
                                 RegionLocations rl, int min, int max) {
    for (int id = min; id <= max; id++) {
      HRegionLocation hrl = rl.getRegionLocation(id);
      ReplicaRegionServerCallable callOnReplica = new ReplicaRegionServerCallable(id, hrl);
      cs.submit(callOnReplica, operationTimeout, id);
    }
  }

  static RegionLocations getRegionLocations(boolean useCache, int replicaId,
                 ClusterConnection cConnection, TableName tableName, byte[] row)
      throws RetriesExhaustedException, DoNotRetryIOException, InterruptedIOException {

    RegionLocations rl;
    try {
      if (useCache) {
        rl = cConnection.locateRegion(tableName, row, true, true, replicaId);
      } else {
        rl = cConnection.relocateRegion(tableName, row, replicaId);
      }
    } catch (DoNotRetryIOException | InterruptedIOException | RetriesExhaustedException e) {
      throw e;
    } catch (IOException e) {
      throw new RetriesExhaustedException("Cannot get the location for replica" + replicaId
          + " of region for " + Bytes.toStringBinary(row) + " in " + tableName, e);
    }
    if (rl == null) {
      throw new RetriesExhaustedException("Cannot get the location for replica" + replicaId
          + " of region for " + Bytes.toStringBinary(row) + " in " + tableName);
    }

    return rl;
  }
}
