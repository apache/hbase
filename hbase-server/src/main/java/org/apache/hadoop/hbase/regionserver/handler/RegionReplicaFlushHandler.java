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

package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.FlushRegionCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

/**
 * HBASE-11580: With the async wal approach (HBASE-11568), the edits are not persisted to wal in
 * secondary region replicas. This means that a secondary region replica can serve some edits from
 * it's memstore that that is still not flushed from primary. We do not want to allow secondary
 * region's seqId to go back in time, when this secondary region is opened elsewhere after a
 * crash or region move. We will trigger a flush cache in the primary region replica and wait
 * for observing a complete flush cycle before marking the region readsEnabled. This handler does
 * the flushing of the primary region replica and ensures that regular region opening is not
 * blocked while the secondary replica is blocked on flush.
 */
@InterfaceAudience.Private
public class RegionReplicaFlushHandler extends EventHandler {

  private static final Log LOG = LogFactory.getLog(RegionReplicaFlushHandler.class);

  private final ClusterConnection connection;
  private final RpcRetryingCallerFactory rpcRetryingCallerFactory;
  private final RpcControllerFactory rpcControllerFactory;
  private final int operationTimeout;
  private final HRegion region;

  public RegionReplicaFlushHandler(Server server, ClusterConnection connection,
      RpcRetryingCallerFactory rpcRetryingCallerFactory, RpcControllerFactory rpcControllerFactory,
      int operationTimeout, HRegion region) {
    super(server, EventType.RS_REGION_REPLICA_FLUSH);
    this.connection = connection;
    this.rpcRetryingCallerFactory = rpcRetryingCallerFactory;
    this.rpcControllerFactory = rpcControllerFactory;
    this.operationTimeout = operationTimeout;
    this.region = region;
  }

  @Override
  public void process() throws IOException {
    triggerFlushInPrimaryRegion(region);
  }

  @Override
  protected void handleException(Throwable t) {
    if (t instanceof InterruptedIOException || t instanceof InterruptedException) {
      LOG.error("Caught throwable while processing event " + eventType, t);
    } else if (t instanceof RuntimeException) {
      server.abort("ServerAborting because a runtime exception was thrown", t);
    } else {
      // something fishy since we cannot flush the primary region until all retries (retries from
      // rpc times 35 trigger). We cannot close the region since there is no such mechanism to
      // close a region without master triggering it. We just abort the server for now.
      server.abort("ServerAborting because an exception was thrown", t);
    }
  }

  private int getRetriesCount(Configuration conf) {
    int numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (numRetries > 10) {
      int mult = conf.getInt("hbase.client.serverside.retries.multiplier", 10);
      numRetries = numRetries / mult; // reset if HRS has multiplied this already
    }
    return numRetries;
  }

  void triggerFlushInPrimaryRegion(final HRegion region) throws IOException, RuntimeException {
    long pause = connection.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

    int maxAttempts = getRetriesCount(connection.getConfiguration());
    RetryCounter counter = new RetryCounterFactory(maxAttempts, (int)pause).create();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to do an RPC to the primary region replica " + ServerRegionReplicaUtil
        .getRegionInfoForDefaultReplica(region.getRegionInfo()).getEncodedName() + " of region "
       + region.getRegionInfo().getEncodedName() + " to trigger a flush");
    }
    while (!region.isClosing() && !region.isClosed()
        && !server.isAborted() && !server.isStopped()) {
      FlushRegionCallable flushCallable = new FlushRegionCallable(
        connection, rpcControllerFactory,
        RegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo()), true);

      // TODO: flushRegion() is a blocking call waiting for the flush to complete. Ideally we
      // do not have to wait for the whole flush here, just initiate it.
      FlushRegionResponse response = null;
      try {
         response = rpcRetryingCallerFactory.<FlushRegionResponse>newCaller()
          .callWithRetries(flushCallable, this.operationTimeout);
      } catch (IOException ex) {
        if (ex instanceof TableNotFoundException
            || connection.isTableDisabled(region.getRegionInfo().getTable())) {
          return;
        }
        throw ex;
      }

      if (response.getFlushed()) {
        // then we have to wait for seeing the flush entry. All reads will be rejected until we see
        // a complete flush cycle or replay a region open event
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully triggered a flush of primary region replica "
              + ServerRegionReplicaUtil
                .getRegionInfoForDefaultReplica(region.getRegionInfo()).getEncodedName()
                + " of region " + region.getRegionInfo().getEncodedName()
                + " Now waiting and blocking reads until observing a full flush cycle");
        }
        break;
      } else {
        if (response.hasWroteFlushWalMarker()) {
          if(response.getWroteFlushWalMarker()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Successfully triggered an empty flush marker(memstore empty) of primary "
                  + "region replica " + ServerRegionReplicaUtil
                    .getRegionInfoForDefaultReplica(region.getRegionInfo()).getEncodedName()
                  + " of region " + region.getRegionInfo().getEncodedName() + " Now waiting and "
                  + "blocking reads until observing a flush marker");
            }
            break;
          } else {
            // somehow we were not able to get the primary to write the flush request. It may be
            // closing or already flushing. Retry flush again after some sleep.
            if (!counter.shouldRetry()) {
              throw new IOException("Cannot cause primary to flush or drop a wal marker after " +
                  "retries. Failing opening of this region replica "
                  + region.getRegionInfo().getEncodedName());
            }
          }
        } else {
          // nothing to do. Are we dealing with an old server?
          LOG.warn("Was not able to trigger a flush from primary region due to old server version? "
              + "Continuing to open the secondary region replica: "
              + region.getRegionInfo().getEncodedName());
          region.setReadsEnabled(true);
          break;
        }
      }
      try {
        counter.sleepUntilNextRetry();
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.getMessage());
      }
    }
  }

}
