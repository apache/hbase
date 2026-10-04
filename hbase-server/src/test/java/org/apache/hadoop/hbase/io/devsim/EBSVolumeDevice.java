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
package org.apache.hadoop.hbase.io.devsim;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Models a single physical EBS volume with independent BW and IOPS budgets and device-level
 * coalescing. The synchronized methods naturally serialize IOs to the same volume, which is
 * realistic -- a physical disk processes one IO at a time.
 * <p>
 * Read and write coalescing buffers are independent: a write to the same volume does not break read
 * coalescing (they are separate physical operations at the device level). However, reads and writes
 * share the per-volume IOPS budget.
 * <p>
 * Each instance is fully self-contained with its own counters. Aggregation across volumes and
 * DataNodes is handled by {@link EBSDevice}.
 */
public class EBSVolumeDevice {

  final int id;
  final IOBudget bwBudget;
  final IOBudget iopsBudget;
  final int maxIoSizeBytes;
  final int deviceLatencyUs;

  private Object lastReader;
  private long lastReadEnd = -1;
  private long pendingReadBytes;

  private Object lastWriter;
  private long pendingWriteBytes;

  // Per-volume counters
  final AtomicLong volumeDeviceReadOps = new AtomicLong();
  final AtomicLong volumeDeviceWriteOps = new AtomicLong();
  final AtomicLong totalBytesRead = new AtomicLong();
  final AtomicLong totalBytesWritten = new AtomicLong();
  final AtomicLong readOpCount = new AtomicLong();
  final AtomicLong writeOpCount = new AtomicLong();
  final AtomicLong bwReadSleepTimeMs = new AtomicLong();
  final AtomicLong bwReadSleepCount = new AtomicLong();
  final AtomicLong bwWriteSleepTimeMs = new AtomicLong();
  final AtomicLong bwWriteSleepCount = new AtomicLong();
  final AtomicLong iopsReadSleepTimeMs = new AtomicLong();
  final AtomicLong iopsReadSleepCount = new AtomicLong();
  final AtomicLong iopsWriteSleepTimeMs = new AtomicLong();
  final AtomicLong iopsWriteSleepCount = new AtomicLong();
  final AtomicLong latencySleepCount = new AtomicLong();
  final AtomicLong latencySleepTimeUs = new AtomicLong();

  /**
   * @param id              volume index within the DataNode
   * @param bwBudget        per-volume BW token bucket (null = unlimited)
   * @param iopsBudget      per-volume IOPS token bucket (null = unlimited)
   * @param maxIoSizeBytes  max coalesced IO size in bytes (0 = no coalescing)
   * @param deviceLatencyUs per-IO device latency in microseconds (0 = disabled)
   */
  public EBSVolumeDevice(int id, IOBudget bwBudget, IOBudget iopsBudget, int maxIoSizeBytes,
    int deviceLatencyUs) {
    this.id = id;
    this.bwBudget = bwBudget;
    this.iopsBudget = iopsBudget;
    this.maxIoSizeBytes = maxIoSizeBytes;
    this.deviceLatencyUs = deviceLatencyUs;
  }

  public int getId() {
    return id;
  }

  /**
   * Account a read operation against this volume's BW and IOPS budgets. Sequential reads from the
   * same stream are coalesced up to {@code maxIoSizeBytes} before consuming an IOPS token.
   * Interleaving (a different stream, or a non-sequential position) flushes pending coalesced IO.
   */
  public synchronized void accountRead(Object stream, long position, int bytes) {
    readOpCount.incrementAndGet();
    totalBytesRead.addAndGet(bytes);
    if (iopsBudget != null) {
      if (maxIoSizeBytes > 0) {
        if (lastReader != stream || (lastReadEnd >= 0 && position != lastReadEnd)) {
          flushPendingReadIopsInternal();
        }
        lastReader = stream;
        pendingReadBytes += bytes;
        lastReadEnd = position + bytes;
        while (pendingReadBytes >= maxIoSizeBytes) {
          pendingReadBytes -= maxIoSizeBytes;
          consumeReadIops();
        }
      } else {
        consumeReadIops();
      }
    }
    if (bwBudget != null) {
      long slept = bwBudget.consume(bytes);
      if (slept > 0) {
        bwReadSleepTimeMs.addAndGet(slept);
        bwReadSleepCount.incrementAndGet();
      }
    }
  }

  public synchronized void flushPendingReadIops(Object stream) {
    if (lastReader == stream) {
      flushPendingReadIopsInternal();
    }
  }

  private void flushPendingReadIopsInternal() {
    if (pendingReadBytes > 0 && iopsBudget != null) {
      consumeReadIops();
    }
    pendingReadBytes = 0;
    lastReader = null;
    lastReadEnd = -1;
  }

  private void consumeReadIops() {
    volumeDeviceReadOps.incrementAndGet();
    long slept = iopsBudget.consume(1);
    if (slept > 0) {
      iopsReadSleepTimeMs.addAndGet(slept);
      iopsReadSleepCount.incrementAndGet();
    }
    applyDeviceLatency();
  }

  /**
   * Account a write operation against this volume's BW and IOPS budgets. Sequential writes from the
   * same stream are coalesced up to {@code maxIoSizeBytes}.
   */
  public synchronized void accountWrite(Object stream, int bytes) {
    writeOpCount.incrementAndGet();
    totalBytesWritten.addAndGet(bytes);
    if (iopsBudget != null) {
      if (maxIoSizeBytes > 0) {
        if (lastWriter != stream) {
          flushPendingWriteIopsInternal();
        }
        lastWriter = stream;
        pendingWriteBytes += bytes;
        while (pendingWriteBytes >= maxIoSizeBytes) {
          pendingWriteBytes -= maxIoSizeBytes;
          consumeWriteIops();
        }
      } else {
        consumeWriteIops();
      }
    }
    if (bwBudget != null) {
      long slept = bwBudget.consume(bytes);
      if (slept > 0) {
        bwWriteSleepTimeMs.addAndGet(slept);
        bwWriteSleepCount.incrementAndGet();
      }
    }
  }

  /**
   * Charge a bulk write against this volume's budgets. Used for block-level write throttling at
   * finalize time, where the total bytes are known but were not individually intercepted.
   * @param totalBytes total bytes written
   */
  public synchronized void accountBulkWrite(long totalBytes) {
    writeOpCount.incrementAndGet();
    totalBytesWritten.addAndGet(totalBytes);
    if (iopsBudget != null) {
      int opsToCharge =
        maxIoSizeBytes > 0 ? (int) ((totalBytes + maxIoSizeBytes - 1) / maxIoSizeBytes) : 1;
      for (int i = 0; i < opsToCharge; i++) {
        consumeWriteIops();
      }
    }
    if (bwBudget != null) {
      long slept = bwBudget.consume(totalBytes);
      if (slept > 0) {
        bwWriteSleepTimeMs.addAndGet(slept);
        bwWriteSleepCount.incrementAndGet();
      }
    }
  }

  public synchronized void flushPendingWriteIops(Object stream) {
    if (lastWriter == stream) {
      flushPendingWriteIopsInternal();
    }
  }

  private void flushPendingWriteIopsInternal() {
    if (pendingWriteBytes > 0 && iopsBudget != null) {
      consumeWriteIops();
    }
    pendingWriteBytes = 0;
    lastWriter = null;
  }

  private void consumeWriteIops() {
    volumeDeviceWriteOps.incrementAndGet();
    long slept = iopsBudget.consume(1);
    if (slept > 0) {
      iopsWriteSleepTimeMs.addAndGet(slept);
      iopsWriteSleepCount.incrementAndGet();
    }
    applyDeviceLatency();
  }

  private void applyDeviceLatency() {
    if (deviceLatencyUs > 0) {
      latencySleepCount.incrementAndGet();
      long startNs = System.nanoTime();
      try {
        Thread.sleep(deviceLatencyUs / 1000, (deviceLatencyUs % 1000) * 1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      latencySleepTimeUs.addAndGet((System.nanoTime() - startNs) / 1000);
    }
  }

  public synchronized void reset() {
    lastReader = null;
    lastReadEnd = -1;
    pendingReadBytes = 0;
    lastWriter = null;
    pendingWriteBytes = 0;
    volumeDeviceReadOps.set(0);
    volumeDeviceWriteOps.set(0);
    totalBytesRead.set(0);
    totalBytesWritten.set(0);
    readOpCount.set(0);
    writeOpCount.set(0);
    bwReadSleepTimeMs.set(0);
    bwReadSleepCount.set(0);
    bwWriteSleepTimeMs.set(0);
    bwWriteSleepCount.set(0);
    iopsReadSleepTimeMs.set(0);
    iopsReadSleepCount.set(0);
    iopsWriteSleepTimeMs.set(0);
    iopsWriteSleepCount.set(0);
    latencySleepCount.set(0);
    latencySleepTimeUs.set(0);
    if (bwBudget != null) {
      bwBudget.reset();
    }
    if (iopsBudget != null) {
      iopsBudget.reset();
    }
  }
}
