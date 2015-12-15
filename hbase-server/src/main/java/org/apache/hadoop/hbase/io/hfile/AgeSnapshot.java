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
package org.apache.hadoop.hbase.io.hfile;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;

/**
 * Snapshot of block cache age in cache.
 * This object is preferred because we can control how it is serialized out when JSON'ing.
 */
@JsonIgnoreProperties({"ageHistogram", "snapshot"})
public class AgeSnapshot {
  private final Snapshot snapshot;

  AgeSnapshot(final Histogram ageHistogram) {
    this.snapshot = ageHistogram.getSnapshot();
  }

  public double get75thPercentile() {
    return snapshot.get75thPercentile();
  }

  public double get95thPercentile() {
    return snapshot.get95thPercentile();
  }

  public double get98thPercentile() {
    return snapshot.get98thPercentile();
  }

  public double get999thPercentile() {
    return snapshot.get999thPercentile();
  }

  public double get99thPercentile() {
    return snapshot.get99thPercentile();
  }

  public double getMean() {
    return this.snapshot.getMean();
  }

  public double getMax() {
    return snapshot.getMax();
  }

  public double getMin() {
    return snapshot.getMin();
  }

  public double getStdDev() {
    return snapshot.getStdDev();
  }
}
