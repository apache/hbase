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
package org.apache.hadoop.hbase.client.backoff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Simple exponential backoff policy on for the client that uses a  percent^4 times the
 * max backoff to generate the backoff time.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ExponentialClientBackoffPolicy implements ClientBackoffPolicy {

  private static final Log LOG = LogFactory.getLog(ExponentialClientBackoffPolicy.class);

  private static final long ONE_MINUTE = 60 * 1000;
  public static final long DEFAULT_MAX_BACKOFF = 5 * ONE_MINUTE;
  public static final String MAX_BACKOFF_KEY = "hbase.client.exponential-backoff.max";
  private long maxBackoff;

  public ExponentialClientBackoffPolicy(Configuration conf) {
    this.maxBackoff = conf.getLong(MAX_BACKOFF_KEY, DEFAULT_MAX_BACKOFF);
  }

  @Override
  public long getBackoffTime(ServerName serverName, byte[] region, ServerStatistics stats) {
    // no stats for the server yet, so don't backoff
    if (stats == null) {
      return 0;
    }

    ServerStatistics.RegionStatistics regionStats = stats.getStatsForRegion(region);
    // no stats for the region yet - don't backoff
    if (regionStats == null) {
      return 0;
    }

    // square the percent as a value less than 1. Closer we move to 100 percent,
    // the percent moves to 1, but squaring causes the exponential curve
    double percent = regionStats.getMemstoreLoadPercent() / 100.0;
    double multiplier = Math.pow(percent, 4.0);
    // shouldn't ever happen, but just incase something changes in the statistic data
    if (multiplier > 1) {
      LOG.warn("Somehow got a backoff multiplier greater than the allowed backoff. Forcing back " +
          "down to the max backoff");
      multiplier = 1;
    }
    return (long) (multiplier * maxBackoff);
  }
}