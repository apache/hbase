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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class PressureAwareThroughputController extends Configured implements
    ThroughputController, Stoppable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PressureAwareThroughputController.class);

  /**
   * Stores the information of one controlled compaction.
   */
  private static final class ActiveOperation {

    private final long startTime;

    private long lastControlTime;

    private long lastControlSize;

    private long totalSize;

    private long numberOfSleeps;

    private long totalSleepTime;

    // prevent too many debug log
    private long lastLogTime;

    ActiveOperation() {
      long currentTime = EnvironmentEdgeManager.currentTime();
      this.startTime = currentTime;
      this.lastControlTime = currentTime;
      this.lastLogTime = currentTime;
    }
  }

  protected long maxThroughputUpperBound;

  protected long maxThroughputLowerBound;

  protected OffPeakHours offPeakHours;

  protected long controlPerSize;

  protected int tuningPeriod;

  private volatile double maxThroughput;
  private volatile double maxThroughputPerOperation;

  protected final ConcurrentMap<String, ActiveOperation> activeOperations = new ConcurrentHashMap<>();

  @Override
  public abstract void setup(final RegionServerServices server);

  protected String throughputDesc(long deltaSize, long elapsedTime) {
    return throughputDesc((double) deltaSize / elapsedTime * 1000);
  }

  protected String throughputDesc(double speed) {
    if (speed >= 1E15) { // large enough to say it is unlimited
      return "unlimited";
    } else {
      return String.format("%.2f MB/second", speed / 1024 / 1024);
    }
  }

  @Override
  public void start(String opName) {
    activeOperations.put(opName, new ActiveOperation());
    maxThroughputPerOperation = getMaxThroughput() / activeOperations.size();
  }

  @Override
  public long control(String opName, long size) throws InterruptedException {
    ActiveOperation operation = activeOperations.get(opName);
    operation.totalSize += size;
    long deltaSize = operation.totalSize - operation.lastControlSize;
    if (deltaSize < controlPerSize) {
      return 0;
    }
    long now = EnvironmentEdgeManager.currentTime();
    long minTimeAllowed = (long) (deltaSize / maxThroughputPerOperation * 1000); // ms
    long elapsedTime = now - operation.lastControlTime;
    operation.lastControlSize = operation.totalSize;
    if (elapsedTime >= minTimeAllowed) {
      operation.lastControlTime = EnvironmentEdgeManager.currentTime();
      return 0;
    }
    // too fast
    long sleepTime = minTimeAllowed - elapsedTime;
    if (LOG.isDebugEnabled()) {
      // do not log too much
      if (now - operation.lastLogTime > 5L * 1000) {
        LOG.debug("deltaSize: " + deltaSize + " bytes; elapseTime: " + elapsedTime + " ns");
        LOG.debug(opName + " sleep=" + sleepTime + "ms because current throughput is "
            + throughputDesc(deltaSize, elapsedTime) + ", max allowed is "
            + throughputDesc(maxThroughputPerOperation) + ", already slept "
            + operation.numberOfSleeps + " time(s) and total slept time is "
            + operation.totalSleepTime + " ms till now.");
        operation.lastLogTime = now;
      }
    }
    Thread.sleep(sleepTime);
    operation.numberOfSleeps++;
    operation.totalSleepTime += sleepTime;
    operation.lastControlTime = EnvironmentEdgeManager.currentTime();
    return sleepTime;
  }

  @Override
  public void finish(String opName) {
    ActiveOperation operation = activeOperations.remove(opName);
    maxThroughputPerOperation = getMaxThroughput() / activeOperations.size();
    long elapsedTime = EnvironmentEdgeManager.currentTime() - operation.startTime;
    LOG.info(opName + " average throughput is "
        + throughputDesc(operation.totalSize, elapsedTime) + ", slept "
        + operation.numberOfSleeps + " time(s) and total slept time is "
        + operation.totalSleepTime + " ms. " + activeOperations.size()
        + " active operations remaining, total limit is " + throughputDesc(getMaxThroughput()));
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

  public double getMaxThroughput() {
    return maxThroughput;
  }

  public void setMaxThroughput(double maxThroughput) {
    this.maxThroughput = maxThroughput;
    maxThroughputPerOperation = getMaxThroughput() / activeOperations.size();
  }
}
