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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class ThreadLocalScanMetrics {
  private ThreadLocalScanMetrics() {
  }

  private static final ThreadLocal<Boolean> isScanMetricsEnabled = new ThreadLocal<>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  private static final ThreadLocal<AtomicInteger> bytesReadFromFs = new ThreadLocal<>() {
    @Override
    protected AtomicInteger initialValue() {
      return new AtomicInteger(0);
    }
  };

  private static final ThreadLocal<AtomicInteger> bytesReadFromBlockCache = new ThreadLocal<>() {
    @Override
    protected AtomicInteger initialValue() {
      return new AtomicInteger(0);
    }
  };

  private static final ThreadLocal<AtomicInteger> bytesReadFromMemstore = new ThreadLocal<>() {
    @Override
    protected AtomicInteger initialValue() {
      return new AtomicInteger(0);
    }
  };

  public static final void setScanMetricsEnabled(boolean enable) {
    isScanMetricsEnabled.set(enable);
  }

  public static final int addBytesReadFromFs(int bytes) {
    return bytesReadFromFs.get().addAndGet(bytes);
  }

  public static final int addBytesReadFromBlockCache(int bytes) {
    return bytesReadFromBlockCache.get().addAndGet(bytes);
  }

  public static final int addBytesReadFromMemstore(int bytes) {
    return bytesReadFromMemstore.get().addAndGet(bytes);
  }

  public static final boolean isScanMetricsEnabled() {
    return isScanMetricsEnabled.get();
  }

  public static final AtomicInteger getBytesReadFromFsCounter() {
    return bytesReadFromFs.get();
  }

  public static final AtomicInteger getBytesReadFromBlockCacheCounter() {
    return bytesReadFromBlockCache.get();
  }

  public static final AtomicInteger getBytesReadFromMemstoreCounter() {
    return bytesReadFromMemstore.get();
  }

  public static final int getBytesReadFromFsAndReset() {
    return getBytesReadFromFsCounter().getAndSet(0);
  }

  public static final int getBytesReadFromBlockCacheAndReset() {
    return getBytesReadFromBlockCacheCounter().getAndSet(0);
  }

  public static final int getBytesReadFromMemstoreAndReset() {
    return getBytesReadFromMemstoreCounter().getAndSet(0);
  }

  public static final void reset() {
    getBytesReadFromFsAndReset();
    getBytesReadFromBlockCacheAndReset();
    getBytesReadFromMemstoreAndReset();
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
  }
}
