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
package org.apache.hadoop.metrics2.util;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Specifies a quantile (with error bounds) to be watched by a
 * {@link MetricSampleQuantiles} object.
 */
@InterfaceAudience.Private
public class MetricQuantile {
  /**
   * The quantile to be watched by a {@link MetricSampleQuantiles}.
   */
  final double quantile;
  /**
   * The error bounds for the {@link #quantile}.
   */
  final double error;

  public MetricQuantile(double quantile, double error) {
    this.quantile = quantile;
    this.error = error;
  }

  @Override
  public boolean equals(Object aThat) {
    if (this == aThat) {
      return true;
    }
    if (!(aThat instanceof MetricQuantile)) {
      return false;
    }

    MetricQuantile that = (MetricQuantile) aThat;

    long qbits = Double.doubleToLongBits(quantile);
    long ebits = Double.doubleToLongBits(error);

    return qbits == Double.doubleToLongBits(that.quantile)
        && ebits == Double.doubleToLongBits(that.error);
  }

  @Override
  public int hashCode() {
    return (int) (Double.doubleToLongBits(quantile) ^ Double
        .doubleToLongBits(error));
  }
}
