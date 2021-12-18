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
package org.apache.hadoop.hbase.regionserver.throttle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StoreHotnessProtector is designed to help limit the concurrency of puts with dense columns, it
 * does best-effort to avoid exhausting all RS's handlers. When a lot of clients write requests with
 * dense (hundreds) columns to a Store at the same time, it will lead to blocking of RS because CSLM
 * degrades when concurrency goes up. It's not a kind of throttling. Throttling is user-oriented,
 * while StoreHotnessProtector is system-oriented, RS-self-protected mechanism.
 * <p>
 * There are three key parameters:
 * <p>
 * 1. parallelPutToStoreThreadLimitCheckMinColumnCount: If the amount of columns exceed this
 * threshold, the HotProtector will work, 100 by default
 * <p>
 * 2. parallelPutToStoreThreadLimit: The amount of concurrency allowed to write puts to a Store at
 * the same time.
 * <p>
 * 3. parallelPreparePutToStoreThreadLimit: The amount of concurrency allowed to
 * prepare writing puts to a Store at the same time.
 * <p>
 * Notice that our writing pipeline includes three key process: MVCC acquire, writing MemStore, and
 * WAL. Only limit the concurrency of writing puts to Store(parallelPutToStoreThreadLimit) is not
 * enough since the actual concurrency of puts may still exceed the limit when MVCC contention or
 * slow WAL sync happens. This is why parallelPreparePutToStoreThreadLimit is needed.
 * <p>
 * This protector is enabled by default and could be turned off by setting
 * hbase.region.store.parallel.put.limit to 0, supporting online configuration change.
 */
@InterfaceAudience.Private
public class StoreHotnessProtector {
  private static final Logger LOG = LoggerFactory.getLogger(StoreHotnessProtector.class);
  private volatile int parallelPutToStoreThreadLimit;

  private volatile int parallelPreparePutToStoreThreadLimit;
  public final static String PARALLEL_PUT_STORE_THREADS_LIMIT =
      "hbase.region.store.parallel.put.limit";
  public final static String PARALLEL_PREPARE_PUT_STORE_MULTIPLIER =
      "hbase.region.store.parallel.prepare.put.multiplier";
  private final static int DEFAULT_PARALLEL_PUT_STORE_THREADS_LIMIT = 10;
  private volatile int parallelPutToStoreThreadLimitCheckMinColumnCount;
  public final static String PARALLEL_PUT_STORE_THREADS_LIMIT_MIN_COLUMN_COUNT =
      "hbase.region.store.parallel.put.limit.min.column.count";
  private final static int DEFAULT_PARALLEL_PUT_STORE_THREADS_LIMIT_MIN_COLUMN_NUM = 100;
  private final static int DEFAULT_PARALLEL_PREPARE_PUT_STORE_MULTIPLIER = 2;

  private final Map<byte[], AtomicInteger> preparePutToStoreMap =
      new ConcurrentSkipListMap<>(Bytes.BYTES_RAWCOMPARATOR);
  private final Region region;

  public StoreHotnessProtector(Region region, Configuration conf) {
    init(conf);
    this.region = region;
  }

  public void init(Configuration conf) {
    this.parallelPutToStoreThreadLimit =
        conf.getInt(PARALLEL_PUT_STORE_THREADS_LIMIT, DEFAULT_PARALLEL_PUT_STORE_THREADS_LIMIT);
    this.parallelPreparePutToStoreThreadLimit = conf.getInt(PARALLEL_PREPARE_PUT_STORE_MULTIPLIER,
        DEFAULT_PARALLEL_PREPARE_PUT_STORE_MULTIPLIER) * parallelPutToStoreThreadLimit;
    this.parallelPutToStoreThreadLimitCheckMinColumnCount =
        conf.getInt(PARALLEL_PUT_STORE_THREADS_LIMIT_MIN_COLUMN_COUNT,
            DEFAULT_PARALLEL_PUT_STORE_THREADS_LIMIT_MIN_COLUMN_NUM);

  }

  public void update(Configuration conf) {
    init(conf);
    preparePutToStoreMap.clear();
    LOG.debug("update config: " + toString());
  }

  public void start(Map<byte[], List<Cell>> familyMaps) throws RegionTooBusyException {
    if (!isEnable()) {
      return;
    }

    String tooBusyStore = null;
    boolean aboveParallelThreadLimit = false;
    boolean aboveParallelPrePutLimit = false;

    for (Map.Entry<byte[], List<Cell>> e : familyMaps.entrySet()) {
      Store store = this.region.getStore(e.getKey());
      if (store == null || e.getValue() == null) {
        continue;
      }

      if (e.getValue().size() > this.parallelPutToStoreThreadLimitCheckMinColumnCount) {

        //we need to try to add #preparePutCount at first because preparePutToStoreMap will be
        //cleared when changing the configuration.
        int preparePutCount = preparePutToStoreMap
            .computeIfAbsent(e.getKey(), key -> new AtomicInteger())
            .incrementAndGet();
        boolean storeAboveThread =
          store.getCurrentParallelPutCount() > this.parallelPutToStoreThreadLimit;
        boolean storeAbovePrePut = preparePutCount > this.parallelPreparePutToStoreThreadLimit;
        if (storeAboveThread || storeAbovePrePut) {
          tooBusyStore = (tooBusyStore == null ?
              store.getColumnFamilyName() :
              tooBusyStore + "," + store.getColumnFamilyName());
        }
        aboveParallelThreadLimit |= storeAboveThread;
        aboveParallelPrePutLimit |= storeAbovePrePut;

        if (LOG.isTraceEnabled()) {
          LOG.trace(store.getColumnFamilyName() + ": preparePutCount=" + preparePutCount
              + "; currentParallelPutCount=" + store.getCurrentParallelPutCount());
        }
      }
    }

    if (aboveParallelThreadLimit || aboveParallelPrePutLimit) {
      String msg =
          "StoreTooBusy," + this.region.getRegionInfo().getRegionNameAsString() + ":" + tooBusyStore
              + " Above "
              + (aboveParallelThreadLimit ? "parallelPutToStoreThreadLimit("
              + this.parallelPutToStoreThreadLimit + ")" : "")
              + (aboveParallelThreadLimit && aboveParallelPrePutLimit ? " or " : "")
              + (aboveParallelPrePutLimit ? "parallelPreparePutToStoreThreadLimit("
              + this.parallelPreparePutToStoreThreadLimit + ")" : "");
      LOG.trace(msg);
      throw new RegionTooBusyException(msg);
    }
  }

  public void finish(Map<byte[], List<Cell>> familyMaps) {
    if (!isEnable()) {
      return;
    }

    for (Map.Entry<byte[], List<Cell>> e : familyMaps.entrySet()) {
      Store store = this.region.getStore(e.getKey());
      if (store == null || e.getValue() == null) {
        continue;
      }
      if (e.getValue().size() > this.parallelPutToStoreThreadLimitCheckMinColumnCount) {
        AtomicInteger counter = preparePutToStoreMap.get(e.getKey());
        // preparePutToStoreMap will be cleared when changing the configuration, so it may turn
        // into a negative value. It will be not accuracy in a short time, it's a trade-off for
        // performance.
        if (counter != null && counter.decrementAndGet() < 0) {
          counter.incrementAndGet();
        }
      }
    }
  }

  public String toString() {
    return "StoreHotnessProtector, parallelPutToStoreThreadLimit="
        + this.parallelPutToStoreThreadLimit + " ; minColumnNum="
        + this.parallelPutToStoreThreadLimitCheckMinColumnCount + " ; preparePutThreadLimit="
        + this.parallelPreparePutToStoreThreadLimit + " ; hotProtect now " + (this.isEnable() ?
        "enable" :
        "disable");
  }

  public boolean isEnable() {
    // feature is enabled when parallelPutToStoreThreadLimit > 0
    return this.parallelPutToStoreThreadLimit > 0;
  }

  Map<byte[], AtomicInteger> getPreparePutToStoreMap() {
    return preparePutToStoreMap;
  }

  public static final long FIXED_SIZE =
      ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.REFERENCE + 3 * Bytes.SIZEOF_INT);
}
