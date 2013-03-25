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

package org.apache.hadoop.hbase.metrics;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.cliffc.high_scale_lib.Counter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

public class ExactCounterMetric extends MetricsBase {

  private static final int DEFAULT_TOP_N = 5;

  // only publish stats on the topN items (default to DEFAULT_TOP_N)
  private final int topN;
  private final Map<String, Counter> counts;

  // all access to the 'counts' map should use this lock.
  // take a write lock iff you want to guarantee exclusive access
  // (the map stripes locks internally, so it's already thread safe -
  // this lock is just so you can take a consistent snapshot of data)
  private final ReadWriteLock lock;
  
  
  /**
   * Constructor to create a new counter metric
   * @param nam         the name to publish this metric under
   * @param registry    where the metrics object will be registered
   * @param description metrics description
   * @param topN        how many 'keys' to publish metrics on 
   */
  public ExactCounterMetric(final String nam, final MetricsRegistry registry, 
      final String description, int topN) {
    super(nam, description);

    this.counts = new MapMaker().makeComputingMap(
        new Function<String, Counter>() {
          @Override
          public Counter apply(String input) {
            return new Counter();
          }    
        });

    this.lock = new ReentrantReadWriteLock();
    this.topN = topN;

    if (registry != null) {
      registry.add(nam, this);      
    }
  }

  /**
   * Constructor creates a new ExactCounterMetric
   * @param nam       the name of the metrics to be used to publish the metric
   * @param registry  where the metrics object will be registered
   */
  public ExactCounterMetric(final String nam, MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION, DEFAULT_TOP_N);
  }

  
  public void update(String type) {
    this.lock.readLock().lock();
    try {
      this.counts.get(type).increment();
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  public void update(String type, long count) {
    this.lock.readLock().lock();
    try {
      this.counts.get(type).add(count);
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  public List<Pair<String, Long>> getTop(int n) {
    final List<Pair<String, Long>> countsSnapshot = 
        Lists.newArrayListWithCapacity(this.counts.size());
    
    // no updates are allowed while I'm holding this lock, so move fast
    this.lock.writeLock().lock();
    try {
      for(Entry<String, Counter> entry : this.counts.entrySet()) {
        countsSnapshot.add(Pair.newPair(entry.getKey(), 
            entry.getValue().get()));
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    
    Collections.sort(countsSnapshot, new Comparator<Pair<String, Long>>() {
      @Override
      public int compare(Pair<String, Long> a, Pair<String, Long> b) {
        return b.getSecond().compareTo(a.getSecond());
      }      
    });
    
    return countsSnapshot.subList(0, Math.min(n, countsSnapshot.size()));
  }
  
  @Override
  public void pushMetric(MetricsRecord mr) {
    final List<Pair<String, Long>> topKeys = getTop(Integer.MAX_VALUE);
    int sum = 0;
    
    int counter = 0;
    for (Pair<String, Long> keyCount : topKeys) {
      counter++;
      // only push stats on the topN keys
      if (counter <= this.topN) {
        mr.setMetric(getName() + "_" + keyCount.getFirst(), 
            keyCount.getSecond());        
      }
      sum += keyCount.getSecond();
    }
    mr.setMetric(getName() + "_map_size", this.counts.size());
    mr.setMetric(getName() + "_total_count", sum);
  }
  
}
