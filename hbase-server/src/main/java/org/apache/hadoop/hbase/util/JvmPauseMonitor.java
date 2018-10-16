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
package org.apache.hadoop.hbase.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.metrics.JvmPauseMonitorSource;
import org.apache.hadoop.conf.Configuration;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Stopwatch;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a short interval of time. If the sleep takes significantly longer
 * than its target time, it implies that the JVM or host machine has
 * paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message.
 * The original JvmPauseMonitor is:
 * ${hadoop-common-project}/hadoop-common/src/main/java/org/apache/hadoop/util/
 * JvmPauseMonitor.java
 * r1503806 | cmccabe | 2013-07-17 01:48:24 +0800 (Wed, 17 Jul 2013) | 1 line
 * HADOOP-9618.  thread which detects GC pauses(Todd Lipcon)
 */
@InterfaceAudience.Private
public class JvmPauseMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(JvmPauseMonitor.class);

  /** The target sleep time */
  private static final long SLEEP_INTERVAL_MS = 500;
  
  /** log WARN if we detect a pause longer than this threshold */
  private final long warnThresholdMs;
  public static final String WARN_THRESHOLD_KEY =
      "jvm.pause.warn-threshold.ms";
  private static final long WARN_THRESHOLD_DEFAULT = 10000;
  
  /** log INFO if we detect a pause longer than this threshold */
  private final long infoThresholdMs;
  public static final String INFO_THRESHOLD_KEY =
      "jvm.pause.info-threshold.ms";
  private static final long INFO_THRESHOLD_DEFAULT = 1000;

  private Thread monitorThread;
  private volatile boolean shouldRun = true;
  private JvmPauseMonitorSource metricsSource;

  public JvmPauseMonitor(Configuration conf) {
    this(conf, null);
  }

  public JvmPauseMonitor(Configuration conf, JvmPauseMonitorSource metricsSource) {
    this.warnThresholdMs = conf.getLong(WARN_THRESHOLD_KEY, WARN_THRESHOLD_DEFAULT);
    this.infoThresholdMs = conf.getLong(INFO_THRESHOLD_KEY, INFO_THRESHOLD_DEFAULT);
    this.metricsSource = metricsSource;
  }
  
  public void start() {
    Preconditions.checkState(monitorThread == null, "Already started");
    monitorThread = new Thread(new Monitor(), "JvmPauseMonitor");
    monitorThread.setDaemon(true);
    monitorThread.start();
  }

  public void stop() {
    shouldRun = false;
    monitorThread.interrupt();
    try {
      monitorThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  private String formatMessage(long extraSleepTime, List<String> gcDiffs) {
    String ret = "Detected pause in JVM or host machine (eg GC): " + "pause of approximately "
        + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }
  
  private Map<String, GcTimes> getGcTimes() {
    Map<String, GcTimes> map = Maps.newHashMap();
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new GcTimes(gcBean));
    }
    return map;
  }

  private static class GcTimes {
    private GcTimes(GarbageCollectorMXBean gcBean) {
      gcCount = gcBean.getCollectionCount();
      gcTimeMillis = gcBean.getCollectionTime();
    }

    private GcTimes(long count, long time) {
      this.gcCount = count;
      this.gcTimeMillis = time;
    }

    private GcTimes subtract(GcTimes other) {
      return new GcTimes(this.gcCount - other.gcCount, this.gcTimeMillis - other.gcTimeMillis);
    }

    @Override
    public String toString() {
      return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
    }

    private long gcCount;
    private long gcTimeMillis;
  }

  private class Monitor implements Runnable {
    @Override
    public void run() {
      Stopwatch sw = Stopwatch.createUnstarted();
      Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
      while (shouldRun) {
        sw.reset().start();
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (InterruptedException ie) {
          return;
        }

        long extraSleepTime = sw.elapsed(TimeUnit.MILLISECONDS) - SLEEP_INTERVAL_MS;
        Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

        if (extraSleepTime > infoThresholdMs) {
          Set<String> gcBeanNames = Sets.intersection(gcTimesAfterSleep.keySet(),
            gcTimesBeforeSleep.keySet());
          List<String> gcDiffs = Lists.newArrayList();
          for (String name : gcBeanNames) {
            GcTimes diff = gcTimesAfterSleep.get(name).subtract(gcTimesBeforeSleep.get(name));
            if (diff.gcCount != 0) {
              gcDiffs.add("GC pool '" + name + "' had collection(s): " + diff.toString());
            }
          }

          updateMetrics(extraSleepTime, !gcDiffs.isEmpty());

          if (extraSleepTime > warnThresholdMs) {
            LOG.warn(formatMessage(extraSleepTime, gcDiffs));
          } else {
            LOG.info(formatMessage(extraSleepTime, gcDiffs));
          }
        }
        gcTimesBeforeSleep = gcTimesAfterSleep;
      }
    }
  }

  public void updateMetrics(long sleepTime, boolean gcDetected) {
    if (metricsSource != null) {
      if (sleepTime > warnThresholdMs) {
        metricsSource.incWarnThresholdExceeded(1);
      } else {
        metricsSource.incInfoThresholdExceeded(1);
      }
      if (gcDetected) {
        metricsSource.updatePauseTimeWithGc(sleepTime);
      } else {
        metricsSource.updatePauseTimeWithoutGc(sleepTime);
      }
    }
  }

  public JvmPauseMonitorSource getMetricsSource() {
    return metricsSource;
  }

  public void setMetricsSource(JvmPauseMonitorSource metricsSource) {
    this.metricsSource = metricsSource;
  }

  /**
   * Simple 'main' to facilitate manual testing of the pause monitor.
   * 
   * This main function just leaks memory into a list. Running this class
   * with a 1GB heap will very quickly go into "GC hell" and result in
   * log messages about the GC pauses.
   */
  public static void main(String []args) throws Exception {
    new JvmPauseMonitor(new Configuration()).start();
    List<String> list = Lists.newArrayList();
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
    }
  }
}
