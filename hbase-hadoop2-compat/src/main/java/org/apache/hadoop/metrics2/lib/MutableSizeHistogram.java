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

package org.apache.hadoop.metrics2.lib;

import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Extended histogram implementation with counters for metric size ranges.
 */
@InterfaceAudience.Private
public class MutableSizeHistogram extends MutableRangeHistogram {
  private final String rangeType = "SizeRangeCount";
  private final long[] ranges = {10,100,1000,10000,100000,1000000,10000000,100000000};
  private final AtomicLongArray rangeVals = new AtomicLongArray(getRange().length+1);

  public MutableSizeHistogram(MetricsInfo info) {
    this(info.name(), info.description());
  }

  public MutableSizeHistogram(String name, String description) {
    super(name, description);
  }

  @Override
  public String getRangeType() {
    return rangeType;
  }

  @Override
  public long[] getRange() {
    return ranges;
  }
  
  @Override
  public AtomicLongArray getRangeVals() {
    return rangeVals;
  }  
}
