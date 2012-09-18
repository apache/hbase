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

package org.apache.hadoop.metrics;

/**
 *
 */
public interface MetricHistogram {

  public static final String NUM_OPS_METRIC_NAME = "_num_ops";
  public static final String MIN_METRIC_NAME = "_min";
  public static final String MAX_METRIC_NAME = "_max";
  public static final String MEAN_METRIC_NAME = "_mean";
  public static final String STD_DEV_METRIC_NAME = "_std_dev";
  public static final String MEDIAN_METRIC_NAME = "_median";
  public static final String SEVENTY_FIFTH_PERCENTILE_METRIC_NAME = "_75th_percentile";
  public static final String NINETY_FIFTH_PERCENTILE_METRIC_NAME = "_95th_percentile";
  public static final String NINETY_NINETH_PERCENTILE_METRIC_NAME = "_99th_percentile";

  public void add(long value);

}
