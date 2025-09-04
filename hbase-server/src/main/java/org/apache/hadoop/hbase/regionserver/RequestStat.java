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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RequestStat implements HeapSize {
  private static final long FIXED_OVERHEAD = ClassSize.estimateBase(RequestStat.class, false);

  private final LongAdder counts = new LongAdder();
  private final LongAdder requestSizeBytes = new LongAdder();
  private final LongAdder responseTimeMs = new LongAdder();
  private final LongAdder responseSizeBytes = new LongAdder();

  public void add(long count, long requestSizeBytes, long responseTimeMs, long responseSizeBytes) {
    this.counts.add(count);
    if (requestSizeBytes > 0) {
      this.requestSizeBytes.add(requestSizeBytes);
    }
    if (responseTimeMs > 0) {
      this.responseTimeMs.add(responseTimeMs);
    }
    if (responseSizeBytes > 0) {
      this.responseSizeBytes.add(responseSizeBytes);
    }
  }

  public long counts() {
    return counts.sum();
  }

  // For Comparator
  public double countsAsDouble() {
    return counts.sum();
  }

  public long requestSizeSumBytes() {
    return requestSizeBytes.sum();
  }

  // For Comparator
  public double requestSizeSumBytesAsDouble() {
    return requestSizeBytes.sum();
  }

  public double requestSizeAvgBytes() {
    double counts = counts();
    return counts == 0 ? 0 : (double) requestSizeSumBytes() / counts;
  }

  public long responseTimeSumMs() {
    return responseTimeMs.sum();
  }

  // For Comparator
  public double responseTimeSumMsAsDouble() {
    return responseTimeMs.sum();
  }

  public double responseTimeAvgMs() {
    double counts = counts();
    return counts == 0 ? 0 : (double) responseTimeSumMs() / counts;
  }

  public long responseSizeSumBytes() {
    return responseSizeBytes.sum();
  }

  // For Comparator
  public double responseSizeSumBytesAsDouble() {
    return responseSizeBytes.sum();
  }

  public double responseSizeAvgBytes() {
    double counts = counts();
    return counts == 0 ? 0 : (double) responseSizeSumBytes() / counts;
  }

  @Override
  public long heapSize() {
    return FIXED_OVERHEAD;
  }
}
