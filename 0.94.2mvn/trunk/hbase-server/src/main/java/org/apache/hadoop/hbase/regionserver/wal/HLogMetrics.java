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

package org.apache.hadoop.hbase.regionserver.wal;

import java.util.concurrent.atomic.AtomicLong;


public class HLogMetrics {

  public static class Metric {
    public long min = Long.MAX_VALUE;
    public long max = 0;
    public long total = 0;
    public int count = 0;

    synchronized void inc(final long val) {
      min = Math.min(min, val);
      max = Math.max(max, val);
      total += val;
      ++count;
    }

    synchronized Metric get() {
      Metric copy = new Metric();
      copy.min = min;
      copy.max = max;
      copy.total = total;
      copy.count = count;
      this.min = Long.MAX_VALUE;
      this.max = 0;
      this.total = 0;
      this.count = 0;
      return copy;
    }
  }
    
    
  // For measuring latency of writes
  static Metric writeTime = new Metric();
  static Metric writeSize = new Metric();
  // For measuring latency of syncs
  static Metric syncTime = new Metric();
  //For measuring slow HLog appends
  static AtomicLong slowHLogAppendCount = new AtomicLong();
  static Metric slowHLogAppendTime = new Metric();

  public static Metric getWriteTime() {
    return writeTime.get();
  }

  public static Metric getWriteSize() {
    return writeSize.get();
  }

  public static Metric getSyncTime() {
    return syncTime.get();
  }

  public static long getSlowAppendCount() {
    return slowHLogAppendCount.get();
  }

  public static Metric getSlowAppendTime() {
    return slowHLogAppendTime.get();
  }
}
