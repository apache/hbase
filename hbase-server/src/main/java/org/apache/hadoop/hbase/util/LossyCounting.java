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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

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
public class LossyCounting {
  private long bucketSize;
  private int currentTerm;
  private Map<String, Integer> data;
  private long totalDataCount;
  private LossyCountingListener listener;

  public interface LossyCountingListener {
    void sweep(String key);
  }

  LossyCounting(double errorRate) {
    this(errorRate, null);
  }

  public LossyCounting(double errorRate, LossyCountingListener listener) {
    if (errorRate < 0.0 || errorRate > 1.0) {
      throw new IllegalArgumentException(" Lossy Counting error rate should be within range [0,1]");
    }
    this.bucketSize = (long) Math.ceil(1 / errorRate);
    this.currentTerm = 1;
    this.totalDataCount = 0;
    this.data = new ConcurrentHashMap<>();
    this.listener = listener;
    calculateCurrentTerm();
  }

  LossyCounting(Configuration conf) {
    this(conf, null);
  }

  public LossyCounting(Configuration conf, LossyCountingListener listener) {
    this(conf.getDouble(HConstants.DEFAULT_LOSSY_COUNTING_ERROR_RATE, 0.02), listener);
  }

  private void addByOne(String key) {
    //If entry exists, we update the entry by incrementing its frequency by one. Otherwise,
    //we create a new entry starting with currentTerm so that it will not be pruned immediately
    Integer i = data.get(key);
    if (i == null) {
      i = currentTerm != 0 ? currentTerm - 1 : 0;
    }
    data.put(key, i + 1);
    //update totalDataCount and term
    totalDataCount++;
    calculateCurrentTerm();
  }

  public void add(String key) {
    addByOne(key);
    if (totalDataCount % bucketSize == 0) {
      //sweep the entries at bucket boundaries
      sweep();
    }
  }

  /**
   * sweep low frequency data
   * @return Names of elements got swept
   */
  private void sweep() {
    for(Map.Entry<String, Integer> entry : data.entrySet()) {
      if(entry.getValue() < currentTerm) {
        String metric = entry.getKey();
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

  public boolean contains(String key) {
    return data.containsKey(key);
  }

  public Set<String> getElements(){
    return data.keySet();
  }

  public long getCurrentTerm() {
    return currentTerm;
  }
}

