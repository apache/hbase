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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsUserAggregateFactory {
  private MetricsUserAggregateFactory() {

  }

  public static final String METRIC_USER_ENABLED_CONF = "hbase.regionserver.user.metrics.enabled";
  public static final boolean DEFAULT_METRIC_USER_ENABLED_CONF = false;

  public static MetricsUserAggregate getMetricsUserAggregate(Configuration conf) {
    if (conf.getBoolean(METRIC_USER_ENABLED_CONF, DEFAULT_METRIC_USER_ENABLED_CONF)) {
      return new MetricsUserAggregateImpl(conf);
    } else {
      //NoOpMetricUserAggregate
      return new MetricsUserAggregate() {
        @Override public MetricsUserAggregateSource getSource() {
          return null;
        }

        @Override public void updatePut(long t) {

        }

        @Override public void updateDelete(long t) {

        }

        @Override public void updateGet(long t) {

        }

        @Override public void updateIncrement(long t) {

        }

        @Override public void updateAppend(long t) {

        }

        @Override public void updateReplay(long t) {

        }

        @Override public void updateScanTime(long t) {

        }

        @Override public void updateFilteredReadRequests() {

        }

        @Override public void updateReadRequestCount() {

        }
      };
    }
  }

}
