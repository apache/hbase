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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A throughput controller which uses the follow schema to limit throughput
 * <ul>
 * <li>If compaction pressure is greater than 1.0, no limitation.</li>
 * <li>In off peak hours, use a fixed throughput limitation
 * {@value #HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK}</li>
 * <li>In normal hours, the max throughput is tune between
 * {@value #HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND} and
 * {@value #HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND}, using the formula &quot;lower +
 * (higer - lower) * compactionPressure&quot;, where compactionPressure is in range [0.0, 1.0]</li>
 * </ul>
 * @see org.apache.hadoop.hbase.regionserver.Store#getCompactionPressure()
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class PressureAwareCompactionThroughputController extends Configured implements
    CompactionThroughputController, Stoppable {

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

  /**
   * Stores the information of one controlled compaction.
   */
  private static final class ActiveCompaction {

    private final long startTime;

    private long lastControlTime;

    private long lastControlSize;

    private long totalSize;

    private long numberOfSleeps;

    private long totalSleepTime;

    // prevent too many debug log
    private long lastLogTime;

    ActiveCompaction() {
      long currentTime = EnvironmentEdgeManager.currentTime();
      this.startTime = currentTime;
      this.lastControlTime = currentTime;
      this.lastLogTime = currentTime;
    }
  }

  private long maxThroughputHigherBound;

  private long maxThroughputLowerBound;

  private long maxThroughputOffpeak;

  private OffPeakHours offPeakHours;

  private long controlPerSize;

  private int tuningPeriod;

  volatile double maxThroughput;

  private final ConcurrentMap<String, ActiveCompaction> activeCompactions =
      new ConcurrentHashMap<String, ActiveCompaction>();

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
          maxThroughputLowerBound + (maxThroughputHigherBound - maxThroughputLowerBound)
              * compactionPressure;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("compactionPressure is " + compactionPressure + ", tune compaction throughput to "
          + throughputDesc(maxThroughputToSet));
    }
    this.maxThroughput = maxThroughputToSet;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }
    this.maxThroughputHigherBound =
        conf.getLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
          DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND);
    this.maxThroughputLowerBound =
        conf.getLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
          DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND);
    this.maxThroughputOffpeak =
        conf.getLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK,
          DEFAULT_HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_OFFPEAK);
    this.offPeakHours = OffPeakHours.getInstance(conf);
    this.controlPerSize = this.maxThroughputLowerBound;
    this.maxThroughput = this.maxThroughputLowerBound;
    this.tuningPeriod =
        getConf().getInt(HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
          DEFAULT_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD);
    LOG.info("Compaction throughput configurations, higher bound: "
        + throughputDesc(maxThroughputHigherBound) + ", lower bound "
        + throughputDesc(maxThroughputLowerBound) + ", off peak: "
        + throughputDesc(maxThroughputOffpeak) + ", tuning period: " + tuningPeriod + " ms");
  }

  private String throughputDesc(long deltaSize, long elapsedTime) {
    return throughputDesc((double) deltaSize / elapsedTime * 1000);
  }

  private String throughputDesc(double speed) {
    if (speed >= 1E15) { // large enough to say it is unlimited
      return "unlimited";
    } else {
      return String.format("%.2f MB/sec", speed / 1024 / 1024);
    }
  }

  @Override
  public void start(String compactionName) {
    activeCompactions.put(compactionName, new ActiveCompaction());
  }

  @Override
  public long control(String compactionName, long size) throws InterruptedException {
    ActiveCompaction compaction = activeCompactions.get(compactionName);
    compaction.totalSize += size;
    long deltaSize = compaction.totalSize - compaction.lastControlSize;
    if (deltaSize < controlPerSize) {
      return 0;
    }
    long now = EnvironmentEdgeManager.currentTime();
    double maxThroughputPerCompaction = this.maxThroughput / activeCompactions.size();
    long minTimeAllowed = (long) (deltaSize / maxThroughputPerCompaction * 1000); // ms
    long elapsedTime = now - compaction.lastControlTime;
    compaction.lastControlSize = compaction.totalSize;
    if (elapsedTime >= minTimeAllowed) {
      compaction.lastControlTime = EnvironmentEdgeManager.currentTime();
      return 0;
    }
    // too fast
    long sleepTime = minTimeAllowed - elapsedTime;
    if (LOG.isDebugEnabled()) {
      // do not log too much
      if (now - compaction.lastLogTime > 60L * 1000) {
        LOG.debug(compactionName + " sleep " + sleepTime + " ms because current throughput is "
            + throughputDesc(deltaSize, elapsedTime) + ", max allowed is "
            + throughputDesc(maxThroughputPerCompaction) + ", already slept "
            + compaction.numberOfSleeps + " time(s) and total slept time is "
            + compaction.totalSleepTime + " ms till now.");
        compaction.lastLogTime = now;
      }
    }
    Thread.sleep(sleepTime);
    compaction.numberOfSleeps++;
    compaction.totalSleepTime += sleepTime;
    compaction.lastControlTime = EnvironmentEdgeManager.currentTime();
    return sleepTime;
  }

  @Override
  public void finish(String compactionName) {
    ActiveCompaction compaction = activeCompactions.remove(compactionName);
    long elapsedTime = Math.max(1, EnvironmentEdgeManager.currentTime() - compaction.startTime);
    LOG.info(compactionName + " average throughput is "
        + throughputDesc(compaction.totalSize, elapsedTime) + ", slept "
        + compaction.numberOfSleeps + " time(s) and total slept time is "
        + compaction.totalSleepTime + " ms. " + activeCompactions.size()
        + " active compactions remaining, total limit is " + throughputDesc(maxThroughput));
  }

  private volatile boolean stopped = false;

  @Override
  public void stop(String why) {
    stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public String toString() {
    return "DefaultCompactionThroughputController [maxThroughput=" + throughputDesc(maxThroughput)
        + ", activeCompactions=" + activeCompactions.size() + "]";
  }
}
