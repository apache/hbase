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

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extended histogram implementation with counters for metric size ranges.
 */
@InterfaceAudience.Private
public class MutableSizeHistogram extends MutableRangeHistogram {

  private final static String RANGE_TYPE = "SizeRangeCount";
  private final static long[] RANGES = {10,100,1000,10000,100000,1000000,10000000,100000000};

  public MutableSizeHistogram(MetricsInfo info) {
    this(info.name(), info.description());
  }

  public MutableSizeHistogram(String name, String description) {
    super(name, description);
  }

  @Override
  public String getRangeType() {
    return RANGE_TYPE;
  }

  @Override
  public long[] getRanges() {
    return RANGES;
  }

}
