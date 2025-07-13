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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.client.metrics.ThreadLocalServerSideScanMetrics;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to seek storefiles in parallel.
 */
@InterfaceAudience.Private
public class ParallelSeekHandler extends EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ParallelSeekHandler.class);
  private KeyValueScanner scanner;
  private ExtendedCell keyValue;
  private long readPoint;
  private CountDownLatch latch;
  private Throwable err = null;

  // Flag to enable/disable scan metrics collection and thread-local counters for capturing scan
  // performance during parallel store file seeking.
  // These aggregate metrics from worker threads back to the main scan thread.
  private final boolean isScanMetricsEnabled;
  // Thread-local counter for bytes read from FS.
  private final AtomicLong bytesReadFromFs;
  // Thread-local counter for bytes read from BlockCache.
  private final AtomicLong bytesReadFromBlockCache;
  // Thread-local counter for block read operations count.
  private final AtomicLong blockReadOpsCount;

  public ParallelSeekHandler(KeyValueScanner scanner, ExtendedCell keyValue, long readPoint,
    CountDownLatch latch) {
    super(null, EventType.RS_PARALLEL_SEEK);
    this.scanner = scanner;
    this.keyValue = keyValue;
    this.readPoint = readPoint;
    this.latch = latch;
    this.isScanMetricsEnabled = ThreadLocalServerSideScanMetrics.isScanMetricsEnabled();
    this.bytesReadFromFs = ThreadLocalServerSideScanMetrics.getBytesReadFromFsCounter();
    this.bytesReadFromBlockCache =
      ThreadLocalServerSideScanMetrics.getBytesReadFromBlockCacheCounter();
    this.blockReadOpsCount = ThreadLocalServerSideScanMetrics.getBlockReadOpsCountCounter();
  }

  @Override
  public void process() {
    try {
      ThreadLocalServerSideScanMetrics.setScanMetricsEnabled(isScanMetricsEnabled);
      if (isScanMetricsEnabled) {
        ThreadLocalServerSideScanMetrics.reset();
      }
      scanner.seek(keyValue);
      if (isScanMetricsEnabled) {
        long metricValue = ThreadLocalServerSideScanMetrics.getBytesReadFromFsAndReset();
        if (metricValue > 0) {
          bytesReadFromFs.addAndGet(metricValue);
        }
        metricValue = ThreadLocalServerSideScanMetrics.getBytesReadFromBlockCacheAndReset();
        if (metricValue > 0) {
          bytesReadFromBlockCache.addAndGet(metricValue);
        }
        metricValue = ThreadLocalServerSideScanMetrics.getBlockReadOpsCountAndReset();
        if (metricValue > 0) {
          blockReadOpsCount.addAndGet(metricValue);
        }
      }
    } catch (IOException e) {
      LOG.error("", e);
      setErr(e);
    } finally {
      latch.countDown();
    }
  }

  public Throwable getErr() {
    return err;
  }

  public void setErr(Throwable err) {
    this.err = err;
  }
}
