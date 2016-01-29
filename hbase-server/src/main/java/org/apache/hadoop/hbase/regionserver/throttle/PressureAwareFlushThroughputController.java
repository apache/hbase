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
package org.apache.hadoop.hbase.regionserver.throttle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;

/**
 * A throughput controller which uses the follow schema to limit throughput
 * <ul>
 * <li>If flush pressure is greater than or equal to 1.0, no limitation.</li>
 * <li>In normal case, the max throughput is tuned between
 * {@value #HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND} and
 * {@value #HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND}, using the formula &quot;lower +
 * (upper - lower) * flushPressure&quot;, where flushPressure is in range [0.0, 1.0)</li>
 * </ul>
 * @see org.apache.hadoop.hbase.regionserver.HRegionServer#getFlushPressure()
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class PressureAwareFlushThroughputController extends PressureAwareThroughputController {

  private static final Log LOG = LogFactory.getLog(PressureAwareFlushThroughputController.class);

  public static final String HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND =
      "hbase.hstore.flush.throughput.upper.bound";

  private static final long DEFAULT_HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND =
      200L * 1024 * 1024;

  public static final String HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND =
      "hbase.hstore.flush.throughput.lower.bound";

  private static final long DEFAULT_HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND =
      100L * 1024 * 1024;

  public static final String HBASE_HSTORE_FLUSH_THROUGHPUT_TUNE_PERIOD =
      "hbase.hstore.flush.throughput.tune.period";

  private static final int DEFAULT_HSTORE_FLUSH_THROUGHPUT_TUNE_PERIOD = 20 * 1000;

  // check flush throughput every this size
  public static final String HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL =
      "hbase.hstore.flush.throughput.control.check.interval";

  private static final long DEFAULT_HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL =
      10L * 1024 * 1024;// 10MB

  @Override
  public void setup(final RegionServerServices server) {
    server.getChoreService().scheduleChore(
      new ScheduledChore("FlushThroughputTuner", this, tuningPeriod, this.tuningPeriod) {

        @Override
        protected void chore() {
          tune(server.getFlushPressure());
        }
      });
  }

  private void tune(double flushPressure) {
    double maxThroughputToSet;
    if (flushPressure >= 1.0) {
      // set to unlimited if global memstore size already exceeds lower limit
      maxThroughputToSet = Double.MAX_VALUE;
    } else {
      // flushPressure is between 0.0 and 1.0, we use a simple linear formula to
      // calculate the throughput limitation.
      maxThroughputToSet =
          maxThroughputLowerBound + (maxThroughputUpperBound - maxThroughputLowerBound)
              * flushPressure;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushPressure is " + flushPressure + ", tune flush throughput to "
          + throughputDesc(maxThroughputToSet));
    }
    this.setMaxThroughput(maxThroughputToSet);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }
    this.maxThroughputUpperBound =
        conf.getLong(HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND,
          DEFAULT_HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND);
    this.maxThroughputLowerBound =
        conf.getLong(HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND,
          DEFAULT_HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND);
    this.offPeakHours = OffPeakHours.getInstance(conf);
    this.controlPerSize =
        conf.getLong(HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL,
          DEFAULT_HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL);
    this.setMaxThroughput(this.maxThroughputLowerBound);
    this.tuningPeriod =
        getConf().getInt(HBASE_HSTORE_FLUSH_THROUGHPUT_TUNE_PERIOD,
          DEFAULT_HSTORE_FLUSH_THROUGHPUT_TUNE_PERIOD);
    LOG.info("Flush throughput configurations, upper bound: "
        + throughputDesc(maxThroughputUpperBound) + ", lower bound "
        + throughputDesc(maxThroughputLowerBound) + ", tuning period: " + tuningPeriod + " ms");
  }

  @Override
  public String toString() {
    return "DefaultFlushController [maxThroughput=" + throughputDesc(getMaxThroughput())
        + ", activeFlushNumber=" + activeOperations.size() + "]";
  }

  @Override
  protected boolean skipControl(long deltaSize, long controlSize) {
    // for flush, we control the flow no matter whether the flush size is small
    return false;
  }
}
