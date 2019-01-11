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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;

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

  private static final Logger LOG = LoggerFactory.getLogger(RegionReplicaFlushHandler.class);

  private final AsyncClusterConnection connection;

  private final HRegion region;

  public RegionReplicaFlushHandler(Server server, HRegion region) {
    super(server, EventType.RS_REGION_REPLICA_FLUSH);
    this.connection = server.getAsyncClusterConnection();
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
      int mult = conf.getInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER,
        HConstants.DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER);
      numRetries = numRetries / mult; // reset if HRS has multiplied this already
    }
    return numRetries;
  }

  void triggerFlushInPrimaryRegion(final HRegion region) throws IOException {
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
      // TODO: flushRegion() is a blocking call waiting for the flush to complete. Ideally we
      // do not have to wait for the whole flush here, just initiate it.
      FlushRegionResponse response;
      try {
        response = FutureUtils.get(connection.flush(ServerRegionReplicaUtil
          .getRegionInfoForDefaultReplica(region.getRegionInfo()).getRegionName(), true));
      } catch (IOException e) {
        if (e instanceof TableNotFoundException || FutureUtils
          .get(connection.getAdmin().isTableDisabled(region.getRegionInfo().getTable()))) {
          return;
        }
        if (!counter.shouldRetry()) {
          throw e;
        }
        // The reason that why we need to retry here is that, the retry for asynchronous admin
        // request is much simpler than the normal operation, if we failed to locate the region once
        // then we will throw the exception out and will not try to relocate again. So here we need
        // to add some retries by ourselves to prevent shutting down the region server too
        // frequent...
        LOG.debug("Failed to trigger a flush of primary region replica {} of region {}, retry={}",
          ServerRegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo())
            .getRegionNameAsString(),
          region.getRegionInfo().getRegionNameAsString(), counter.getAttemptTimes(), e);
        try {
          counter.sleepUntilNextRetry();
        } catch (InterruptedException e1) {
          throw new InterruptedIOException(e1.getMessage());
        }
        continue;
      }

      if (response.getFlushed()) {
        // then we have to wait for seeing the flush entry. All reads will be rejected until we see
        // a complete flush cycle or replay a region open event
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully triggered a flush of primary region replica " +
            ServerRegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo())
              .getRegionNameAsString() +
            " of region " + region.getRegionInfo().getRegionNameAsString() +
            " Now waiting and blocking reads until observing a full flush cycle");
        }
        region.setReadsEnabled(true);
        break;
      } else {
        if (response.hasWroteFlushWalMarker()) {
          if (response.getWroteFlushWalMarker()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Successfully triggered an empty flush marker(memstore empty) of primary " +
                "region replica " +
                ServerRegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo())
                  .getRegionNameAsString() +
                " of region " + region.getRegionInfo().getRegionNameAsString() +
                " Now waiting and " + "blocking reads until observing a flush marker");
            }
            region.setReadsEnabled(true);
            break;
          } else {
            // somehow we were not able to get the primary to write the flush request. It may be
            // closing or already flushing. Retry flush again after some sleep.
            if (!counter.shouldRetry()) {
              throw new IOException("Cannot cause primary to flush or drop a wal marker after " +
                counter.getAttemptTimes() + " retries. Failing opening of this region replica " +
                region.getRegionInfo().getRegionNameAsString());
            } else {
              LOG.warn(
                "Cannot cause primary replica {} to flush or drop a wal marker " +
                  "for region replica {}, retry={}",
                ServerRegionReplicaUtil.getRegionInfoForDefaultReplica(region.getRegionInfo())
                  .getRegionNameAsString(),
                region.getRegionInfo().getRegionNameAsString(), counter.getAttemptTimes());
            }
          }
        } else {
          // nothing to do. Are we dealing with an old server?
          LOG.warn(
            "Was not able to trigger a flush from primary region due to old server version? " +
              "Continuing to open the secondary region replica: " +
              region.getRegionInfo().getRegionNameAsString());
          break;
        }
      }
      try {
        counter.sleepUntilNextRetry();
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.getMessage());
      }
    }
    region.setReadsEnabled(true);
  }
}
