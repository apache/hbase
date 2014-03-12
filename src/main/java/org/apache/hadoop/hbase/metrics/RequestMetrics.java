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

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * The RequestMetrics is to keep track of total request count metrics
 * as well as the requests per second
 *
 */
public class RequestMetrics {
  private long totalRequestCount = 0;
  private long lastTotalRequestCount = 0;
  private long lastUpdateTimeStamp = 0;
  private int requestPerSecond = 0;
  private static final long INTERVAL = 1000;
  
  public RequestMetrics() {
    this.lastUpdateTimeStamp = EnvironmentEdgeManager.currentTimeMillis();
  }

  public synchronized long getTotalRequestCount() {
    return totalRequestCount;
  }
  
  public synchronized void incrTotalRequestCount(long incr) {
    this.totalRequestCount += incr;
  }
  
  public synchronized void incrTotalRequestCount() {
    this.totalRequestCount ++;
  }

  /**
   * @return requests per second
   */
  public synchronized int getRequestPerSecond() {
    long interval = EnvironmentEdgeManager.currentTimeMillis()
      - lastUpdateTimeStamp;
    if (interval == 0) 
      interval = 1;

    if (interval >= INTERVAL) {
      // update the request per second if the interval is more than one second
      int sec = (int) (interval / INTERVAL);
      long requsts = this.totalRequestCount - this.lastTotalRequestCount;
      requestPerSecond =  (int) (requsts / sec);
      
      //update the last updated time stamp and last total request count
      this.lastTotalRequestCount = this.totalRequestCount;
      this.lastUpdateTimeStamp = EnvironmentEdgeManager.currentTimeMillis();
    } 
    
    return requestPerSecond;
  }

}
