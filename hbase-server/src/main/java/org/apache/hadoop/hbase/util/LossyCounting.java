/*
 *
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

package org.apache.hadoop.hbase.util;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * LossyCounting utility, bounded data structure that maintains approximate high frequency
 * elements in data stream.
 *
 * Bucket size is 1 / error rate.  (Error rate is 0.02 by default)
 * Lemma If element does not appear in set, then is frequency is less than e * N
 *       (N is total element counts until now.)
 * Based on paper:
 * http://www.vldb.org/conf/2002/S10P03.pdf
 */
@InterfaceAudience.Private
public class LossyCounting<T> {
  private static final Logger LOG = LoggerFactory.getLogger(LossyCounting.class);
  private final ExecutorService executor;
  private long bucketSize;
  private int currentTerm;
  private Map<T, Integer> data;
  private long totalDataCount;
  private final String name;
  private LossyCountingListener<T> listener;
  private static AtomicReference<Future<?>> fut = new AtomicReference<>(null);

  public interface LossyCountingListener<T> {
    void sweep(T key);
  }

  LossyCounting(String name, double errorRate) {
    this(name, errorRate, null);
  }

  public LossyCounting(String name, double errorRate, LossyCountingListener<T> listener) {
    this.name = name;
    if (errorRate < 0.0 || errorRate > 1.0) {
      throw new IllegalArgumentException(" Lossy Counting error rate should be within range [0,1]");
    }
    this.bucketSize = (long) Math.ceil(1 / errorRate);
    this.currentTerm = 1;
    this.totalDataCount = 0;
    this.data = new ConcurrentHashMap<>();
    this.listener = listener;
    calculateCurrentTerm();
    executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("lossy-count-%d").build());
  }

  LossyCounting(String name, Configuration conf) {
    this(name, conf, null);
  }

  public LossyCounting(String name, Configuration conf, LossyCountingListener<T> listener) {
    this(name, conf.getDouble(HConstants.DEFAULT_LOSSY_COUNTING_ERROR_RATE, 0.02), listener);
  }

  private void addByOne(T key) {
    //If entry exists, we update the entry by incrementing its frequency by one. Otherwise,
    //we create a new entry starting with currentTerm so that it will not be pruned immediately
    data.put(key, data.getOrDefault(key, currentTerm != 0 ? currentTerm - 1 : 0) + 1);

    //update totalDataCount and term
    totalDataCount++;
    calculateCurrentTerm();
  }

  public void add(T key) {
    addByOne(key);
    if(totalDataCount % bucketSize == 0) {
      //sweep the entries at bucket boundaries
      //run Sweep
      Future<?> future = fut.get();
      if (future != null && !future.isDone()){
        return;
      }
      future = executor.submit(new SweepRunnable());
      fut.set(future);
    }
  }


  /**
   * sweep low frequency data
   */
  public void sweep() {
    for(Map.Entry<T, Integer> entry : data.entrySet()) {
      if(entry.getValue() < currentTerm) {
        T metric = entry.getKey();
        data.remove(metric);
        if (listener != null) {
          listener.sweep(metric);
        }
      }
    }
  }

  /**
   * Calculate and set current term
   */
  private void calculateCurrentTerm() {
    this.currentTerm = (int) Math.ceil(1.0 * totalDataCount / (double) bucketSize);
  }

  public long getBucketSize(){
    return bucketSize;
  }

  public long getDataSize() {
    return data.size();
  }

  public boolean contains(T key) {
    return data.containsKey(key);
  }

  public Set<T> getElements(){
    return data.keySet();
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  class SweepRunnable implements Runnable {
    @Override public void run() {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Starting sweep of lossyCounting-" + name);
      }
      try {
        sweep();
      } catch (Exception exception) {
        LOG.debug("Error while sweeping of lossyCounting-{}", name, exception);
      }
    }
  }

  public Future<?> getSweepFuture() {
    return fut.get();
  }
}

