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
package org.apache.hadoop.hbase.client.metrics;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thread-local storage for server-side scan metrics that captures performance data separately for
 * each scan thread. This class works in conjunction with {@link ServerSideScanMetrics} to provide
 * comprehensive scan performance monitoring.
 * <h3>Purpose and Design</h3> {@link ServerSideScanMetrics} captures scan metrics on the server
 * side and passes them back to the client in protocol buffer responses. However, the
 * {@link ServerSideScanMetrics} instance is not readily available at deeper layers in HBase where
 * HFiles are read and individual HFile blocks are accessed. To avoid the complexity of passing
 * {@link ServerSideScanMetrics} instances through method calls across multiple layers, this class
 * provides thread-local storage for metrics collection.
 * <h3>Thread Safety and HBase Architecture</h3> This class leverages a critical aspect of HBase
 * server design: on the server side, the thread that opens a
 * {@link org.apache.hadoop.hbase.regionserver.RegionScanner} and calls
 * {@link org.apache.hadoop.hbase.regionserver.RegionScanner#nextRaw(List, ScannerContext)} is the
 * same thread that reads HFile blocks. This design allows thread-local storage to effectively
 * capture metrics without cross-thread synchronization.
 * <h3>Special Handling for Parallel Operations</h3> The only deviation from the single-thread model
 * occurs when {@link org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler} is used for
 * parallel store file seeking. In this case, special handling ensures that metrics are captured
 * correctly across multiple threads. The {@link ParallelSeekHandler} captures metrics from worker
 * threads and aggregates them back to the main scan thread. Please refer to the javadoc of
 * {@link org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler} for detailed information
 * about this parallel processing mechanism.
 * <h3>Usage Pattern</h3>
 * <ol>
 * <li>Enable metrics collection: {@link #setScanMetricsEnabled(boolean)}</li>
 * <li>Reset counters at scan start: {@link #reset()}</li>
 * <li>Increment counters during I/O operations using the various {@code add*} methods</li>
 * <li>Populate the main metrics object:
 * {@link #populateServerSideScanMetrics(ServerSideScanMetrics)}</li>
 * </ol>
 * <h3>Thread Safety</h3> This class is thread-safe. Each thread maintains its own set of counters
 * through {@link ThreadLocal} storage, ensuring that metrics from different scan operations do not
 * interfere with each other.
 * @see ServerSideScanMetrics
 * @see org.apache.hadoop.hbase.regionserver.RegionScanner
 * @see org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler
 */
@InterfaceAudience.Private
public final class ThreadLocalServerSideScanMetrics {
  private ThreadLocalServerSideScanMetrics() {
  }

  private static final ThreadLocal<Boolean> isScanMetricsEnabled = new ThreadLocal<>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  private static final ThreadLocal<AtomicLong> bytesReadFromFs = new ThreadLocal<>() {
    @Override
    protected AtomicLong initialValue() {
      return new AtomicLong(0);
    }
  };

  private static final ThreadLocal<AtomicLong> bytesReadFromBlockCache = new ThreadLocal<>() {
    @Override
    protected AtomicLong initialValue() {
      return new AtomicLong(0);
    }
  };

  private static final ThreadLocal<AtomicLong> bytesReadFromMemstore = new ThreadLocal<>() {
    @Override
    protected AtomicLong initialValue() {
      return new AtomicLong(0);
    }
  };

  private static final ThreadLocal<AtomicLong> blockReadOpsCount = new ThreadLocal<>() {
    @Override
    protected AtomicLong initialValue() {
      return new AtomicLong(0);
    }
  };

  public static final void setScanMetricsEnabled(boolean enable) {
    isScanMetricsEnabled.set(enable);
  }

  public static final long addBytesReadFromFs(long bytes) {
    return bytesReadFromFs.get().addAndGet(bytes);
  }

  public static final long addBytesReadFromBlockCache(long bytes) {
    return bytesReadFromBlockCache.get().addAndGet(bytes);
  }

  public static final long addBytesReadFromMemstore(long bytes) {
    return bytesReadFromMemstore.get().addAndGet(bytes);
  }

  public static final long addBlockReadOpsCount(long count) {
    return blockReadOpsCount.get().addAndGet(count);
  }

  public static final boolean isScanMetricsEnabled() {
    return isScanMetricsEnabled.get();
  }

  public static final AtomicLong getBytesReadFromFsCounter() {
    return bytesReadFromFs.get();
  }

  public static final AtomicLong getBytesReadFromBlockCacheCounter() {
    return bytesReadFromBlockCache.get();
  }

  public static final AtomicLong getBytesReadFromMemstoreCounter() {
    return bytesReadFromMemstore.get();
  }

  public static final AtomicLong getBlockReadOpsCountCounter() {
    return blockReadOpsCount.get();
  }

  public static final long getBytesReadFromFsAndReset() {
    return getBytesReadFromFsCounter().getAndSet(0);
  }

  public static final long getBytesReadFromBlockCacheAndReset() {
    return getBytesReadFromBlockCacheCounter().getAndSet(0);
  }

  public static final long getBytesReadFromMemstoreAndReset() {
    return getBytesReadFromMemstoreCounter().getAndSet(0);
  }

  public static final long getBlockReadOpsCountAndReset() {
    return getBlockReadOpsCountCounter().getAndSet(0);
  }

  public static final void reset() {
    getBytesReadFromFsAndReset();
    getBytesReadFromBlockCacheAndReset();
    getBytesReadFromMemstoreAndReset();
    getBlockReadOpsCountAndReset();
  }

  public static final void populateServerSideScanMetrics(ServerSideScanMetrics metrics) {
    if (metrics == null) {
      return;
    }
    metrics.addToCounter(ServerSideScanMetrics.BYTES_READ_FROM_FS_METRIC_NAME,
      getBytesReadFromFsCounter().get());
    metrics.addToCounter(ServerSideScanMetrics.BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME,
      getBytesReadFromBlockCacheCounter().get());
    metrics.addToCounter(ServerSideScanMetrics.BYTES_READ_FROM_MEMSTORE_METRIC_NAME,
      getBytesReadFromMemstoreCounter().get());
    metrics.addToCounter(ServerSideScanMetrics.BLOCK_READ_OPS_COUNT_METRIC_NAME,
      getBlockReadOpsCountCounter().get());
  }
}
