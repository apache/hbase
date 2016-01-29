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
 * <li>If compaction pressure is greater than 1.0, no limitation.</li>
 * <li>In off peak hours, use a fixed throughput limitation
 * {@value #HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK}</li>
 * <li>In normal hours, the max throughput is tuned between
 * {@value #HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND} and
 * {@value #HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND}, using the formula &quot;lower +
 * (higer - lower) * compactionPressure&quot;, where compactionPressure is in range [0.0, 1.0]</li>
 * </ul>
 * @see org.apache.hadoop.hbase.regionserver.Store#getCompactionPressure()
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class PressureAwareCompactionThroughputController extends PressureAwareThroughputController {

  private final static Log LOG = LogFactory
      .getLog(PressureAwareCompactionThroughputController.class);

  public static final String HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND =
      "hbase.hstore.compaction.throughput.higher.bound";

  private static final long DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND =
      20L * 1024 * 1024;

  public static final String HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND =
      "hbase.hstore.compaction.throughput.lower.bound";

  private static final long DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND =
      10L * 1024 * 1024;

  public static final String HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK =
      "hbase.hstore.compaction.throughput.offpeak";

  private static final long DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK = Long.MAX_VALUE;

  public static final String HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD =
      "hbase.hstore.compaction.throughput.tune.period";

  private static final int DEFAULT_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD = 60 * 1000;

  // check compaction throughput every this size
  private static final String HBASE_HSTORE_COMPACTION_THROUGHPUT_CONTROL_CHECK_INTERVAL =
    "hbase.hstore.compaction.throughput.control.check.interval";

  private long maxThroughputOffpeak;

  @Override
  public void setup(final RegionServerServices server) {
    server.getChoreService().scheduleChore(
      new ScheduledChore("CompactionThroughputTuner", this, tuningPeriod) {

        @Override
        protected void chore() {
          tune(server.getCompactionPressure());
        }
      });
  }

  private void tune(double compactionPressure) {
    double maxThroughputToSet;
    if (compactionPressure > 1.0) {
      // set to unlimited if some stores already reach the blocking store file count
      maxThroughputToSet = Double.MAX_VALUE;
    } else if (offPeakHours.isOffPeakHour()) {
      maxThroughputToSet = maxThroughputOffpeak;
    } else {
      // compactionPressure is between 0.0 and 1.0, we use a simple linear formula to
      // calculate the throughput limitation.
      maxThroughputToSet =
          maxThroughputLowerBound + (maxThroughputUpperBound - maxThroughputLowerBound)
              * compactionPressure;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("compactionPressure is " + compactionPressure + ", tune compaction throughput to "
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
        conf.getLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
          DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND);
    this.maxThroughputLowerBound =
        conf.getLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
          DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND);
    this.maxThroughputOffpeak =
        conf.getLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK,
          DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK);
    this.offPeakHours = OffPeakHours.getInstance(conf);
    this.controlPerSize =
        conf.getLong(HBASE_HSTORE_COMPACTION_THROUGHPUT_CONTROL_CHECK_INTERVAL,
          this.maxThroughputLowerBound);
    this.setMaxThroughput(this.maxThroughputLowerBound);
    this.tuningPeriod =
        getConf().getInt(HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
          DEFAULT_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD);
    LOG.info("Compaction throughput configurations, higher bound: "
        + throughputDesc(maxThroughputUpperBound) + ", lower bound "
        + throughputDesc(maxThroughputLowerBound) + ", off peak: "
        + throughputDesc(maxThroughputOffpeak) + ", tuning period: " + tuningPeriod + " ms");
  }

  @Override
  public String toString() {
    return "DefaultCompactionThroughputController [maxThroughput="
        + throughputDesc(getMaxThroughput()) + ", activeCompactions=" + activeOperations.size()
        + "]";
  }

  @Override
  protected boolean skipControl(long deltaSize, long controlSize) {
    if (deltaSize < controlSize) {
      return true;
    } else {
      return false;
    }
  }
}
