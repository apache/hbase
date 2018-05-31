/**
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



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

@InterfaceAudience.Public
public class LossyCounting {
  private static final Logger LOG = LoggerFactory.getLogger(LossyCounting.class);
  private long bucketSize;
  private long currentTerm;
  private double errorRate;
  private Map<String, Integer> data;
  private long totalDataCount;

  public LossyCounting(double errorRate) {
    this.errorRate = errorRate;
    if (errorRate < 0.0 || errorRate > 1.0) {
      throw new IllegalArgumentException(" Lossy Counting error rate should be within range [0,1]");
    }
    this.bucketSize = (long) Math.ceil(1 / errorRate);
    this.currentTerm = 1;
    this.totalDataCount = 0;
    this.errorRate = errorRate;
    this.data = new ConcurrentHashMap<>();
    calculateCurrentTerm();
  }

  public LossyCounting() {
    Configuration conf = HBaseConfiguration.create();
    this.errorRate = conf.getDouble(HConstants.DEFAULT_LOSSY_COUNTING_ERROR_RATE, 0.02);
    this.bucketSize = (long) Math.ceil(1.0 / errorRate);
    this.currentTerm = 1;
    this.totalDataCount = 0;
    this.data = new ConcurrentHashMap<>();
    calculateCurrentTerm();
  }

  public Set<String> addByOne(String key) {
    if(data.containsKey(key)) {
      data.put(key, data.get(key) +1);
    } else {
      data.put(key, 1);
    }
    totalDataCount++;
    calculateCurrentTerm();
    Set<String> dataToBeSwept = new HashSet<>();
    if(totalDataCount % bucketSize == 0) {
      dataToBeSwept = sweep();
    }
    return dataToBeSwept;
  }

  /**
   * sweep low frequency data
   * @return Names of elements got swept
   */
  private Set<String> sweep() {
    Set<String> dataToBeSwept = new HashSet<>();
    for(Map.Entry<String, Integer> entry : data.entrySet()) {
      if(entry.getValue() + errorRate < currentTerm) {
        dataToBeSwept.add(entry.getKey());
      }
    }
    for(String key : dataToBeSwept) {
      data.remove(key);
    }
    LOG.debug(String.format("Swept %d of elements.", dataToBeSwept.size()));
    return dataToBeSwept;
  }

  /**
   * Calculate and set current term
   */
  private void calculateCurrentTerm() {
    this.currentTerm = (int) Math.ceil(1.0 * totalDataCount / bucketSize);
  }

  public long getBuketSize(){
    return bucketSize;
  }

  public long getDataSize() {
    return data.size();
  }

  public boolean contains(String key) {
    return data.containsKey(key);
  }

  public long getCurrentTerm() {
    return currentTerm;
  }
}

