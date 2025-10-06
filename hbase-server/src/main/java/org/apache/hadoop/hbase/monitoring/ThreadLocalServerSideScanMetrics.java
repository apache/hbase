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
package org.apache.hadoop.hbase.monitoring;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

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
 * server design: on the server side, the thread that opens a {@link RegionScanner} and calls
 * {@link RegionScanner#nextRaw(java.util.List, ScannerContext)} is the same thread that reads HFile
 * blocks. This design allows thread-local storage to effectively capture metrics without
 * cross-thread synchronization.
 * <h3>Special Handling for Parallel Operations</h3> The only deviation from the single-thread model
 * occurs when {@link org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler} is used for
 * parallel store file seeking. In this case, special handling ensures that metrics are captured
 * correctly across multiple threads. The
 * {@link org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler} captures metrics from
 * worker threads and aggregates them back to the main scan thread. Please refer to the javadoc of
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
 * @see RegionScanner
 * @see org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.PHOENIX)
@InterfaceStability.Evolving
public final class ThreadLocalServerSideScanMetrics {
  private ThreadLocalServerSideScanMetrics() {
  }

  private static final ThreadLocal<Boolean> IS_SCAN_METRICS_ENABLED =
    ThreadLocal.withInitial(() -> false);

  private static final ThreadLocal<AtomicLong> BYTES_READ_FROM_FS =
    ThreadLocal.withInitial(() -> new AtomicLong(0));

  private static final ThreadLocal<AtomicLong> BYTES_READ_FROM_BLOCK_CACHE =
    ThreadLocal.withInitial(() -> new AtomicLong(0));

  private static final ThreadLocal<AtomicLong> BYTES_READ_FROM_MEMSTORE =
    ThreadLocal.withInitial(() -> new AtomicLong(0));

  private static final ThreadLocal<AtomicLong> BLOCK_READ_OPS_COUNT =
    ThreadLocal.withInitial(() -> new AtomicLong(0));

  public static void setScanMetricsEnabled(boolean enable) {
    IS_SCAN_METRICS_ENABLED.set(enable);
  }

  public static long addBytesReadFromFs(long bytes) {
    return BYTES_READ_FROM_FS.get().addAndGet(bytes);
  }

  public static long addBytesReadFromBlockCache(long bytes) {
    return BYTES_READ_FROM_BLOCK_CACHE.get().addAndGet(bytes);
  }

  public static long addBytesReadFromMemstore(long bytes) {
    return BYTES_READ_FROM_MEMSTORE.get().addAndGet(bytes);
  }

  public static long addBlockReadOpsCount(long count) {
    return BLOCK_READ_OPS_COUNT.get().addAndGet(count);
  }

  public static boolean isScanMetricsEnabled() {
    return IS_SCAN_METRICS_ENABLED.get();
  }

  public static AtomicLong getBytesReadFromFsCounter() {
    return BYTES_READ_FROM_FS.get();
  }

  public static AtomicLong getBytesReadFromBlockCacheCounter() {
    return BYTES_READ_FROM_BLOCK_CACHE.get();
  }

  public static AtomicLong getBytesReadFromMemstoreCounter() {
    return BYTES_READ_FROM_MEMSTORE.get();
  }

  public static AtomicLong getBlockReadOpsCountCounter() {
    return BLOCK_READ_OPS_COUNT.get();
  }

  public static long getBytesReadFromFsAndReset() {
    return getBytesReadFromFsCounter().getAndSet(0);
  }

  public static long getBytesReadFromBlockCacheAndReset() {
    return getBytesReadFromBlockCacheCounter().getAndSet(0);
  }

  public static long getBytesReadFromMemstoreAndReset() {
    return getBytesReadFromMemstoreCounter().getAndSet(0);
  }

  public static long getBlockReadOpsCountAndReset() {
    return getBlockReadOpsCountCounter().getAndSet(0);
  }

  public static void reset() {
    getBytesReadFromFsAndReset();
    getBytesReadFromBlockCacheAndReset();
    getBytesReadFromMemstoreAndReset();
    getBlockReadOpsCountAndReset();
  }

  public static void populateServerSideScanMetrics(ServerSideScanMetrics metrics) {
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
