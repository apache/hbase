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
package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsStoreWrapperImpl implements MetricsStoreWrapper, Closeable {

  private final HStore store;
  private static final Logger LOG = LoggerFactory.getLogger(MetricsStoreWrapperImpl.class);

  public static final int PERIOD = 45;
  public static final String UNKNOWN = "unknown";
  private ScheduledExecutorService executor;
  private Runnable runnable;
  // add others also. check if anything is redundant
  private long numStoreFiles;
  private long memstoreSize;
  private long storeFileSize;
  private long getsFromMemstore;
  private long getsOnStore;
  private long getsOnFile;
  private long numReferenceFiles;
  private long minStoreFileAge;
  private long maxStoreFileAge;
  private long avgStoreFileAge;
  private long numHFiles;
  private int storeRefCount;

  private ScheduledFuture<?> storeMetricUpdateTask;

  public MetricsStoreWrapperImpl(HStore store) {
    this.store = store;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new HStoreMetricsWrapperRunnable();
    this.storeMetricUpdateTask =
        this.executor.scheduleWithFixedDelay(this.runnable, PERIOD, PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    storeMetricUpdateTask.cancel(true);
  }

  @Override
  public String getStoreName() {
    return store.getColumnFamilyName();
  }

  @Override
  public String getRegionName() {
    return store.getRegionInfo().getRegionNameAsString();
  }

  @Override
  public String getTableName() {
    return store.getRegionInfo().getTable().getNameAsString();
  }

  @Override
  public String getNamespace() {
    return store.getTableName().getNamespaceAsString();
  }

  @Override
  public long getNumStoreFiles() {
    return numStoreFiles;
  }

  @Override
  public long getMemStoreSize() {
    // todo : change this - we need to expose data, heapsize and offheapdatasize
    return memstoreSize;
  }

  @Override
  public long getStoreFileSize() {
    return storeFileSize;
  }

  @Override
  public long getReadRequestCount() {
    return getsOnStore;
  }

  @Override
  public long getMemstoreReadRequestsCount() {
    return getsFromMemstore;
  }

  @Override
  public long getFileReadRequestCount() {
    return getsOnFile;
  }

  public class HStoreMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      long tempMaxStoreFileAge = 0;
      long tempMinStoreFileAge = Long.MAX_VALUE;
      long avgAgeNumerator = 0;

      numStoreFiles = store.getStorefilesCount();
      storeRefCount = store.getStoreRefCount();
      // showing data size only. Better to show offheap size etc. but for now its ok
      memstoreSize += store.getMemStoreSize().getDataSize();
      storeFileSize += store.getStorefilesSize();
      OptionalLong storeMaxStoreFileAge = store.getMaxStoreFileAge();
      if (storeMaxStoreFileAge.isPresent()
          && storeMaxStoreFileAge.getAsLong() > tempMaxStoreFileAge) {
        maxStoreFileAge = storeMaxStoreFileAge.getAsLong();
      }

      OptionalLong storeMinStoreFileAge = store.getMinStoreFileAge();
      if (storeMinStoreFileAge.isPresent()
          && storeMinStoreFileAge.getAsLong() < tempMinStoreFileAge) {
        tempMinStoreFileAge = storeMinStoreFileAge.getAsLong();
      }

      numHFiles = store.getNumHFiles();
      numReferenceFiles = store.getNumReferenceFiles();

      OptionalDouble storeAvgStoreFileAge = store.getAvgStoreFileAge();
      if (storeAvgStoreFileAge.isPresent()) {
        avgAgeNumerator += (long) storeAvgStoreFileAge.getAsDouble() * numHFiles;
      }
      // this need not be pushed periodically. This can be always available 
      //readsFromMemstore = store.getReadRequestsCountFromMemstore();

      if (tempMinStoreFileAge != Long.MAX_VALUE) {
        minStoreFileAge = tempMinStoreFileAge;
      }

      if (numHFiles != 0) {
        avgStoreFileAge = avgAgeNumerator / numHFiles;
      }
      getsFromMemstore = store.getGetRequestsCountFromMemstore();
      getsOnStore = store.getReadRequestsFromStoreCount();
      getsOnFile = store.getGetRequestsCountFromFile();
    }
  }

  @Override
  public long getMaxStoreFileAge() {
    return this.maxStoreFileAge;
  }

  @Override
  public long getMinStoreFileAge() {
    return this.minStoreFileAge;
  }

  @Override
  public long getAvgStoreFileAge() {
    return this.avgStoreFileAge;
  }

  @Override
  public long getNumReferenceFiles() {
    return this.numReferenceFiles;
  }

  @Override
  public long getStoreRefCount() {
    return this.storeRefCount;
  }
}
