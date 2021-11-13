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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manager the buffer size for all {@link RegionReplicationSink}.
 * <p/>
 * If the buffer size exceeds the soft limit, we will find out the region with largest pending size
 * and trigger a flush, so it can drop all the pending entries and save memories.
 * <p/>
 * If the buffer size exceeds the hard limit, we will return {@code false} for
 * {@link #increase(long)} and let the {@link RegionReplicationSink} to drop the edits immediately.
 */
@InterfaceAudience.Private
public class RegionReplicationBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(RegionReplicationBufferManager.class);

  /**
   * This is the total size of pending entries for all the sinks.
   */
  public static final String MAX_PENDING_SIZE = "hbase.region.read-replica.sink.max-pending-size";

  public static final long MAX_PENDING_SIZE_DEFAULT = 100L * 1024 * 1024;

  public static final String SOFT_LIMIT_PERCENTAGE =
    "hbase.region.read-replica.sink.max-pending-size.soft-limit-percentage";

  public static final float SOFT_LIMIT_PERCENTAGE_DEFAULT = 0.8f;

  private final RegionServerServices rsServices;

  private final long maxPendingSize;

  private final long softMaxPendingSize;

  private final AtomicLong pendingSize = new AtomicLong();

  private final ThreadPoolExecutor executor;

  public RegionReplicationBufferManager(RegionServerServices rsServices) {
    this.rsServices = rsServices;
    Configuration conf = rsServices.getConfiguration();
    this.maxPendingSize = conf.getLong(MAX_PENDING_SIZE, MAX_PENDING_SIZE_DEFAULT);
    this.softMaxPendingSize =
      (long) (conf.getFloat(SOFT_LIMIT_PERCENTAGE, SOFT_LIMIT_PERCENTAGE_DEFAULT) * maxPendingSize);
    this.executor = new ThreadPoolExecutor(
      1, 1, 1, TimeUnit.SECONDS, new SynchronousQueue<>(), new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("Region-Replication-Flusher-%d").build(),
      (r, e) -> LOG.debug("A flush task is ongoing, drop the new scheduled one"));
    executor.allowCoreThreadTimeOut(true);
  }

  private void flush() {
    long max = Long.MIN_VALUE;
    HRegion toFlush = null;
    for (HRegion region : rsServices.getRegions()) {
      Optional<RegionReplicationSink> sink = region.getRegionReplicationSink();
      if (sink.isPresent()) {
        RegionReplicationSink s = sink.get();
        long p = s.pendingSize();
        if (p > max) {
          max = p;
          toFlush = region;
        }
      }
    }
    if (toFlush != null) {
      // here we need to write flush marker out, so we can drop all the pending edits in the region
      // replication sink.
      try {
        LOG.info("Going to flush {} with {} pending entry size", toFlush.getRegionInfo(),
          StringUtils.TraditionalBinaryPrefix.long2String(max, "", 1));
        FlushResult result = toFlush.flushcache(true, true, FlushLifeCycleTracker.DUMMY);
        if (!result.isFlushSucceeded()) {
          LOG.warn("Failed to flush {}, the result is {}", toFlush.getRegionInfo(),
            result.getResult());
        }
      } catch (IOException e) {
        LOG.warn("Failed to flush {}", toFlush.getRegionInfo(), e);
      }
    } else {
      // usually this should not happen but since the flush operation is async, theoretically it
      // could happen. Let's log it, no real harm.
      LOG.warn("Can not find a region to flush");
    }
  }

  /**
   * Return whether we should just drop all the edits, if we have reached the hard limit of max
   * pending size.
   * @return {@code true} means OK, {@code false} means drop all the edits.
   */
  public boolean increase(long size) {
    long sz = pendingSize.addAndGet(size);
    if (sz > softMaxPendingSize) {
      executor.execute(this::flush);
    }
    return sz <= maxPendingSize;
  }

  /**
   * Called after you ship the edits out.
   */
  public void decrease(long size) {
    pendingSize.addAndGet(-size);
  }

  public void stop() {
    executor.shutdown();
  }
}
