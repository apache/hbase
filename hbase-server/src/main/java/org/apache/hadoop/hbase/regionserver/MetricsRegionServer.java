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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

/**
 * <p>
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * </p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsRegionServer {
  private MetricsRegionServerSource serverSource;
  private MetricsRegionServerWrapper regionServerWrapper;

  public MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper) {
    this(regionServerWrapper,
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
            .createServer(regionServerWrapper));

  }

  MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper,
                      MetricsRegionServerSource serverSource) {
    this.regionServerWrapper = regionServerWrapper;
    this.serverSource = serverSource;
  }

  @VisibleForTesting
  public MetricsRegionServerSource getMetricsSource() {
    return serverSource;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
    return regionServerWrapper;
  }

  public void updatePut(long t) {
    if (t > 1000) {
      serverSource.incrSlowPut();
    }
    serverSource.updatePut(t);
  }

  public void updateDelete(long t) {
    if (t > 1000) {
      serverSource.incrSlowDelete();
    }
    serverSource.updateDelete(t);
  }

  public void updateGet(long t) {
    if (t > 1000) {
      serverSource.incrSlowGet();
    }
    serverSource.updateGet(t);
  }

  public void updateIncrement(long t) {
    if (t > 1000) {
      serverSource.incrSlowIncrement();
    }
    serverSource.updateIncrement(t);
  }

  public void updateAppend(long t) {
    if (t > 1000) {
      serverSource.incrSlowAppend();
    }
    serverSource.updateAppend(t);
  }

  public void updateReplay(long t){
    serverSource.updateReplay(t);
  }

  public void updateScanSize(long scanSize){
    serverSource.updateScanSize(scanSize);
  }

  public void updateScanTime(long t) {
    serverSource.updateScanTime(t);
  }

  public void updateSplitTime(long t) {
    serverSource.updateSplitTime(t);
  }

  public void incrSplitRequest() {
    serverSource.incrSplitRequest();
  }

  public void incrSplitSuccess() {
    serverSource.incrSplitSuccess();
  }

  public void updateFlush(long t, long memstoreSize, long fileSize) {
    serverSource.updateFlushTime(t);
    serverSource.updateFlushMemstoreSize(memstoreSize);
    serverSource.updateFlushOutputSize(fileSize);
  }

  public void updateCompaction(boolean isMajor, long t, int inputFileCount, int outputFileCount,
      long inputBytes, long outputBytes) {
    serverSource.updateCompactionTime(isMajor, t);
    serverSource.updateCompactionInputFileCount(isMajor, inputFileCount);
    serverSource.updateCompactionOutputFileCount(isMajor, outputFileCount);
    serverSource.updateCompactionInputSize(isMajor, inputBytes);
    serverSource.updateCompactionOutputSize(isMajor, outputBytes);
  }
}
