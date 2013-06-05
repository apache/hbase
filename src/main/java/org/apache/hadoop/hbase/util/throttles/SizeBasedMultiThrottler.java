/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.util.throttles;

public class SizeBasedMultiThrottler implements SizeBasedThrottlerInterface {
  final long smallMax;
  final SizeBasedThrottlerInterface smallerThrottler;
  final SizeBasedThrottlerInterface largerThrottler;

  public SizeBasedMultiThrottler(long smallerThreshold, long largerThreshold,
      long smallMax) {
    if (smallerThreshold <= 0 || largerThreshold <= 0) {
      throw new IllegalArgumentException("Threshold must be greater than 0");
    }
    this.smallMax = smallMax;
    this.smallerThrottler = new SizeBasedThrottler(smallerThreshold);
    this.largerThrottler = new SizeBasedThrottler(largerThreshold);
  }

  private SizeBasedThrottlerInterface chooseThrottler(long delta) {
    if (delta <= smallMax) {
      return smallerThrottler;
    } else {
      return largerThrottler;
    }
  }

  @Override
  public long increase(long delta) throws InterruptedException {
    chooseThrottler(delta).increase(delta);
    return getCurrentValue();
  }

  @Override
  public long decrease(long delta) {
    chooseThrottler(delta).decrease(delta);
    return getCurrentValue();
  }

  @Override
  public long getCurrentValue() {
    return smallerThrottler.getCurrentValue()
        + largerThrottler.getCurrentValue();
  }
}
