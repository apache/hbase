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
package org.apache.hadoop.hbase.io.devsim;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static registry and metrics aggregation for the EBS device layer. Each {@link ThrottledFsDataset}
 * proxy (one per DataNode) registers itself here at creation time. Provides:
 * <ul>
 * <li>Per-DataNode and per-volume metric accessors
 * <li>Aggregate accessors across all DataNodes (for simple single-DN tests)
 * <li>Periodic metrics reporter (same style as the original ThrottledFileSystem reporter)
 * <li>Configuration key constants and a {@link #configure} helper for test setup
 * </ul>
 * <p>
 * <b>Test isolation:</b> This class uses static mutable state. Tests must call {@link #shutdown()}
 * in their {@code @AfterClass} method. Parallel test execution in the same JVM is not supported.
 */
public final class EBSDevice {

  private static final Logger LOG = LoggerFactory.getLogger(EBSDevice.class);

  // ---- Configuration keys ----

  public static final String IO_BUDGET_BYTES_PER_SEC_KEY = "hbase.test.devsim.budget.bytes.per.sec";
  public static final String IO_BUDGET_IOPS_KEY = "hbase.test.devsim.budget.iops";
  public static final String IO_BUDGET_WINDOW_MS_KEY = "hbase.test.devsim.budget.window.ms";
  public static final int DEFAULT_WINDOW_MS = 100;

  public static final String IO_BUDGET_REPORT_INTERVAL_SEC_KEY =
    "hbase.test.devsim.budget.report.interval.sec";
  public static final int DEFAULT_REPORT_INTERVAL_SEC = 10;

  public static final String IO_MAX_IO_SIZE_KB_KEY = "hbase.test.devsim.max.iosize.kb";
  public static final int DEFAULT_MAX_IO_SIZE_KB = 1024;

  public static final String IO_INSTANCE_MBPS_KEY = "hbase.test.devsim.instance.mbps";
  public static final int DEFAULT_INSTANCE_MBPS = 0;

  public static final String IO_DEVICE_LATENCY_US_KEY = "hbase.test.devsim.device.latency.us";
  public static final int DEFAULT_DEVICE_LATENCY_US = 1000;

  // ---- Per-DataNode context ----

  /**
   * Holds the EBS volume devices and instance-level budget for a single DataNode. One instance per
   * {@link ThrottledFsDataset} proxy.
   */
  public static final class DataNodeContext {
    private final String datanodeId;
    private final EBSVolumeDevice[] volumes;
    private final IOBudget instanceBwBudget;
    private final long budgetBytesPerSec;
    private final int budgetIops;
    private final int deviceLatencyUs;

    DataNodeContext(String datanodeId, EBSVolumeDevice[] volumes, IOBudget instanceBwBudget,
      long budgetBytesPerSec, int budgetIops, int deviceLatencyUs) {
      this.datanodeId = datanodeId;
      this.volumes = volumes;
      this.instanceBwBudget = instanceBwBudget;
      this.budgetBytesPerSec = budgetBytesPerSec;
      this.budgetIops = budgetIops;
      this.deviceLatencyUs = deviceLatencyUs;
    }

    public String getDatanodeId() {
      return datanodeId;
    }

    public EBSVolumeDevice[] getVolumes() {
      return volumes;
    }

    public int getNumVolumes() {
      return volumes.length;
    }

    public IOBudget getInstanceBwBudget() {
      return instanceBwBudget;
    }

    public long getBudgetBytesPerSec() {
      return budgetBytesPerSec;
    }

    public int getBudgetIops() {
      return budgetIops;
    }

    public int getDeviceLatencyUs() {
      return deviceLatencyUs;
    }

    /**
     * Charge bytes against the instance-level BW budget (shared across all volumes on this DN).
     * @return ms slept
     */
    public long consumeInstanceBw(long bytes) {
      if (instanceBwBudget != null) {
        return instanceBwBudget.consume(bytes);
      }
      return 0;
    }

    public void reset() {
      for (EBSVolumeDevice vol : volumes) {
        vol.reset();
      }
      if (instanceBwBudget != null) {
        instanceBwBudget.reset();
      }
    }
  }

  // ---- Static registry ----

  private static final List<DataNodeContext> DATANODES = new CopyOnWriteArrayList<>();

  private static volatile ScheduledExecutorService metricsExecutor;
  private static volatile ScheduledFuture<?> metricsFuture;
  private static volatile MetricsReporter metricsReporter;
  private static final AtomicLong readInterceptCount = new AtomicLong();
  private static final AtomicLong writeInterceptCount = new AtomicLong();
  private static final AtomicLong unresolvedVolumeCount = new AtomicLong();

  private EBSDevice() {
  }

  /**
   * Register a DataNode's EBS context. Called by {@link ThrottledFsDataset} at proxy creation.
   */
  public static DataNodeContext register(String datanodeId, EBSVolumeDevice[] volumes,
    IOBudget instanceBwBudget, long budgetBytesPerSec, int budgetIops, int deviceLatencyUs) {
    DataNodeContext ctx = new DataNodeContext(datanodeId, volumes, instanceBwBudget,
      budgetBytesPerSec, budgetIops, deviceLatencyUs);
    DATANODES.add(ctx);
    LOG.info(
      "EBSDevice: registered DN {} with {} volumes "
        + "(BW={} MB/s, IOPS={}, latency={}us per volume)",
      datanodeId, volumes.length,
      budgetBytesPerSec > 0
        ? String.format("%.1f", budgetBytesPerSec / (1024.0 * 1024.0))
        : "unlimited",
      budgetIops > 0 ? budgetIops : "unlimited", deviceLatencyUs > 0 ? deviceLatencyUs : "off");
    return ctx;
  }

  public static List<DataNodeContext> getDataNodes() {
    return DATANODES;
  }

  public static DataNodeContext getDataNodeContext(int index) {
    return DATANODES.get(index);
  }

  public static int getNumDataNodes() {
    return DATANODES.size();
  }

  // ---- Aggregate accessors (sum across all DNs and volumes) ----

  private static long sumVolumes(java.util.function.ToLongFunction<EBSVolumeDevice> fn) {
    long total = 0;
    for (DataNodeContext dn : DATANODES) {
      for (EBSVolumeDevice vol : dn.volumes) {
        total += fn.applyAsLong(vol);
      }
    }
    return total;
  }

  public static long getTotalBytesRead() {
    return sumVolumes(v -> v.totalBytesRead.get());
  }

  public static long getTotalBytesWritten() {
    return sumVolumes(v -> v.totalBytesWritten.get());
  }

  public static long getReadOpCount() {
    return sumVolumes(v -> v.readOpCount.get());
  }

  public static long getWriteOpCount() {
    return sumVolumes(v -> v.writeOpCount.get());
  }

  public static long getDeviceReadOps() {
    return sumVolumes(v -> v.volumeDeviceReadOps.get());
  }

  public static long getDeviceWriteOps() {
    return sumVolumes(v -> v.volumeDeviceWriteOps.get());
  }

  public static long getReadSleepTimeMs() {
    return sumVolumes(v -> v.bwReadSleepTimeMs.get());
  }

  public static long getWriteSleepTimeMs() {
    return sumVolumes(v -> v.bwWriteSleepTimeMs.get());
  }

  public static long getReadSleepCount() {
    return sumVolumes(v -> v.bwReadSleepCount.get());
  }

  public static long getWriteSleepCount() {
    return sumVolumes(v -> v.bwWriteSleepCount.get());
  }

  public static long getIopsReadSleepTimeMs() {
    return sumVolumes(v -> v.iopsReadSleepTimeMs.get());
  }

  public static long getIopsWriteSleepTimeMs() {
    return sumVolumes(v -> v.iopsWriteSleepTimeMs.get());
  }

  public static long getIopsReadSleepCount() {
    return sumVolumes(v -> v.iopsReadSleepCount.get());
  }

  public static long getIopsWriteSleepCount() {
    return sumVolumes(v -> v.iopsWriteSleepCount.get());
  }

  public static long getLatencySleepCount() {
    return sumVolumes(v -> v.latencySleepCount.get());
  }

  public static long getLatencySleepTimeUs() {
    return sumVolumes(v -> v.latencySleepTimeUs.get());
  }

  public static int getDeviceLatencyUs() {
    if (!DATANODES.isEmpty()) {
      return DATANODES.get(0).deviceLatencyUs;
    }
    return 0;
  }

  public static int getNumVolumes() {
    if (!DATANODES.isEmpty()) {
      return DATANODES.get(0).volumes.length;
    }
    return 0;
  }

  public static long getBudgetBytesPerSec() {
    if (!DATANODES.isEmpty()) {
      return DATANODES.get(0).budgetBytesPerSec;
    }
    return 0;
  }

  public static int getBudgetIops() {
    if (!DATANODES.isEmpty()) {
      return DATANODES.get(0).budgetIops;
    }
    return 0;
  }

  public static String getPerVolumeStats() {
    if (DATANODES.isEmpty()) {
      return "N/A (no DataNodes registered)";
    }
    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < DATANODES.size(); d++) {
      DataNodeContext dn = DATANODES.get(d);
      if (DATANODES.size() > 1) {
        if (d > 0) sb.append("; ");
        sb.append("DN").append(d).append("[");
      }
      for (int i = 0; i < dn.volumes.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(String.format("v%d: R=%d W=%d", i, dn.volumes[i].volumeDeviceReadOps.get(),
          dn.volumes[i].volumeDeviceWriteOps.get()));
      }
      if (DATANODES.size() > 1) {
        sb.append("]");
      }
    }
    return sb.toString();
  }

  public static void recordReadIntercept() {
    readInterceptCount.incrementAndGet();
  }

  public static void recordWriteIntercept() {
    writeInterceptCount.incrementAndGet();
  }

  public static void recordUnresolvedVolume() {
    unresolvedVolumeCount.incrementAndGet();
  }

  public static long getReadInterceptCount() {
    return readInterceptCount.get();
  }

  public static long getWriteInterceptCount() {
    return writeInterceptCount.get();
  }

  public static long getUnresolvedVolumeCount() {
    return unresolvedVolumeCount.get();
  }

  // ---- Lifecycle ----

  public static void resetMetrics() {
    MetricsReporter reporter = metricsReporter;
    if (reporter != null) {
      synchronized (reporter) {
        for (DataNodeContext dn : DATANODES) {
          dn.reset();
        }
        reporter.resetBaseline();
      }
    } else {
      for (DataNodeContext dn : DATANODES) {
        dn.reset();
      }
    }
    readInterceptCount.set(0);
    writeInterceptCount.set(0);
    unresolvedVolumeCount.set(0);
  }

  public static synchronized void startMetricsReporter(int intervalSec) {
    if (metricsExecutor != null) {
      return;
    }
    metricsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "EBSDevice-MetricsReporter");
      t.setDaemon(true);
      return t;
    });
    MetricsReporter reporter = new MetricsReporter(intervalSec);
    metricsReporter = reporter;
    metricsFuture =
      metricsExecutor.scheduleAtFixedRate(reporter, intervalSec, intervalSec, TimeUnit.SECONDS);
  }

  public static synchronized void stopMetricsReporter() {
    ScheduledFuture<?> f = metricsFuture;
    if (f != null) {
      f.cancel(false);
      metricsFuture = null;
    }
    ScheduledExecutorService exec = metricsExecutor;
    if (exec != null) {
      exec.shutdownNow();
      metricsExecutor = null;
    }
    metricsReporter = null;
  }

  /**
   * Unregister all DataNodes and stop the metrics reporter. Call from test teardown.
   */
  public static void shutdown() {
    stopMetricsReporter();
    DATANODES.clear();
    readInterceptCount.set(0);
    writeInterceptCount.set(0);
    unresolvedVolumeCount.set(0);
  }

  // ---- Configuration helper ----

  /**
   * Configure a Hadoop {@link Configuration} for the EBS device layer. Sets the
   * {@code dfs.datanode.fsdataset.factory} key and all EBS volume properties.
   * @param conf            Hadoop configuration to modify
   * @param budgetMbps      per-volume BW cap in MB/s (0 = unlimited)
   * @param budgetIops      per-volume IOPS cap (0 = unlimited)
   * @param deviceLatencyUs per-IO device latency in microseconds (0 = disabled)
   */
  public static void configure(Configuration conf, int budgetMbps, int budgetIops,
    int deviceLatencyUs) {
    conf.set("dfs.datanode.fsdataset.factory", ThrottledFsDatasetFactory.class.getName());
    if (budgetMbps > 0) {
      conf.setLong(IO_BUDGET_BYTES_PER_SEC_KEY, (long) budgetMbps * 1024L * 1024L);
    }
    if (budgetIops > 0) {
      conf.setInt(IO_BUDGET_IOPS_KEY, budgetIops);
    }
    if (deviceLatencyUs > 0) {
      conf.setInt(IO_DEVICE_LATENCY_US_KEY, deviceLatencyUs);
    }
  }

  /**
   * Overload with all knobs.
   */
  public static void configure(Configuration conf, int budgetMbps, int budgetIops,
    int deviceLatencyUs, int maxIoSizeKb, int instanceMbps) {
    configure(conf, budgetMbps, budgetIops, deviceLatencyUs);
    conf.setInt(IO_MAX_IO_SIZE_KB_KEY, maxIoSizeKb);
    if (instanceMbps > 0) {
      conf.setInt(IO_INSTANCE_MBPS_KEY, instanceMbps);
    }
  }

  // ---- Periodic reporter ----

  private static class MetricsReporter implements Runnable {
    private final int intervalSec;
    private long prevBytesRead;
    private long prevBytesWritten;
    private long prevBwReadSleepMs;
    private long prevBwWriteSleepMs;
    private long prevBwReadSleepCount;
    private long prevBwWriteSleepCount;
    private long prevReadOps;
    private long prevWriteOps;
    private long prevIopsReadSleeps;
    private long prevIopsWriteSleeps;
    private long prevDeviceReadOps;
    private long prevDeviceWriteOps;
    private final Map<String, Long> prevPerVolDeviceOps = new HashMap<>();

    MetricsReporter(int intervalSec) {
      this.intervalSec = intervalSec;
    }

    synchronized void resetBaseline() {
      prevBytesRead = 0;
      prevBytesWritten = 0;
      prevBwReadSleepMs = 0;
      prevBwWriteSleepMs = 0;
      prevBwReadSleepCount = 0;
      prevBwWriteSleepCount = 0;
      prevReadOps = 0;
      prevWriteOps = 0;
      prevIopsReadSleeps = 0;
      prevIopsWriteSleeps = 0;
      prevDeviceReadOps = 0;
      prevDeviceWriteOps = 0;
      prevPerVolDeviceOps.clear();
    }

    @Override
    public synchronized void run() {
      long curBytesRead = getTotalBytesRead();
      long curBytesWritten = getTotalBytesWritten();
      long curBwReadSleepMs = getReadSleepTimeMs();
      long curBwWriteSleepMs = getWriteSleepTimeMs();
      long curBwReadSleepCount = getReadSleepCount();
      long curBwWriteSleepCount = getWriteSleepCount();
      long curReadOps = getReadOpCount();
      long curWriteOps = getWriteOpCount();
      long curIopsReadSleeps = getIopsReadSleepCount();
      long curIopsWriteSleeps = getIopsWriteSleepCount();
      long curDeviceReadOps = getDeviceReadOps();
      long curDeviceWriteOps = getDeviceWriteOps();

      long dBytesRead = curBytesRead - prevBytesRead;
      long dBytesWritten = curBytesWritten - prevBytesWritten;
      long dBwReadSleepMs = curBwReadSleepMs - prevBwReadSleepMs;
      long dBwWriteSleepMs = curBwWriteSleepMs - prevBwWriteSleepMs;
      long dBwReadSleeps = curBwReadSleepCount - prevBwReadSleepCount;
      long dBwWriteSleeps = curBwWriteSleepCount - prevBwWriteSleepCount;
      long dReadOps = curReadOps - prevReadOps;
      long dWriteOps = curWriteOps - prevWriteOps;
      long dIopsReadSleeps = curIopsReadSleeps - prevIopsReadSleeps;
      long dIopsWriteSleeps = curIopsWriteSleeps - prevIopsWriteSleeps;
      long dDeviceReadOps = curDeviceReadOps - prevDeviceReadOps;
      long dDeviceWriteOps = curDeviceWriteOps - prevDeviceWriteOps;

      double readMbps = dBytesRead / (1024.0 * 1024.0) / intervalSec;
      double writeMbps = dBytesWritten / (1024.0 * 1024.0) / intervalSec;
      double combinedMbps = readMbps + writeMbps;
      double dRIops = (double) dDeviceReadOps / intervalSec;
      double dWIops = (double) dDeviceWriteOps / intervalSec;
      double combinedDeviceIops = dRIops + dWIops;

      int totalVolumes = 0;
      long budgetBytesPerSec = 0;
      int budgetIops = 0;
      int deviceLatencyUs = 0;
      if (!DATANODES.isEmpty()) {
        DataNodeContext first = DATANODES.get(0);
        budgetBytesPerSec = first.budgetBytesPerSec;
        budgetIops = first.budgetIops;
        deviceLatencyUs = first.deviceLatencyUs;
        for (DataNodeContext dn : DATANODES) {
          totalVolumes += dn.volumes.length;
        }
      }

      StringBuilder sb = new StringBuilder();
      sb.append(String.format("EBSDevice [%ds interval, %d DNs, %d vols]: ", intervalSec,
        DATANODES.size(), totalVolumes));

      if (totalVolumes > 0) {
        double perVolBwMbps = budgetBytesPerSec > 0 ? budgetBytesPerSec / (1024.0 * 1024.0) : 0;
        double aggBwMbps = perVolBwMbps * totalVolumes;
        double bwUtil = aggBwMbps > 0 ? (combinedMbps / aggBwMbps) * 100.0 : 0;
        int aggIops = totalVolumes * budgetIops;
        double iopsUtil = aggIops > 0 ? (combinedDeviceIops / aggIops) * 100.0 : 0;

        if (budgetBytesPerSec > 0) {
          sb.append(String.format(
            "BW: %dx%.0f=%.0f MB/s, R=%.1f W=%.1f combined=%.1f (%.0f%% util), "
              + "BW-sleeps: R=%d(%dms) W=%d(%dms); ",
            totalVolumes, perVolBwMbps, aggBwMbps, readMbps, writeMbps, combinedMbps, bwUtil,
            dBwReadSleeps, dBwReadSleepMs, dBwWriteSleeps, dBwWriteSleepMs));
        }
        if (budgetIops > 0) {
          sb.append(String.format(
            "IOPS: %dx%d=%d, app-R=%.0f app-W=%.0f dev-R=%.0f dev-W=%.0f "
              + "combined-dev=%.0f (%.0f%% util), IOPS-sleeps: R=%d W=%d; ",
            totalVolumes, budgetIops, aggIops, (double) dReadOps / intervalSec,
            (double) dWriteOps / intervalSec, dRIops, dWIops, combinedDeviceIops, iopsUtil,
            dIopsReadSleeps, dIopsWriteSleeps));
        }

        if (budgetIops > 0) {
          sb.append("per-vol:[");
          boolean first2 = true;
          for (int d = 0; d < DATANODES.size(); d++) {
            DataNodeContext dn = DATANODES.get(d);
            for (int i = 0; i < dn.volumes.length; i++) {
              if (!first2) sb.append(' ');
              first2 = false;
              long volR = dn.volumes[i].volumeDeviceReadOps.get();
              long volW = dn.volumes[i].volumeDeviceWriteOps.get();
              long curVolOps = volR + volW;
              String volKey = "d" + d + "v" + i;
              long prevVolOps = prevPerVolDeviceOps.getOrDefault(volKey, 0L);
              long dVolOps = curVolOps - prevVolOps;
              prevPerVolDeviceOps.put(volKey, curVolOps);
              double volIops = (double) dVolOps / intervalSec;
              double volUtil = budgetIops > 0 ? (volIops / budgetIops) * 100.0 : 0;
              if (DATANODES.size() > 1) {
                sb.append(String.format("d%dv%d=%.0f%%", d, i, volUtil));
              } else {
                sb.append(String.format("v%d=%.0f%%", i, volUtil));
              }
            }
          }
          sb.append("]; ");
        }
      }

      if (deviceLatencyUs > 0) {
        long curLatencySleeps = getLatencySleepCount();
        long curLatencyUs = getLatencySleepTimeUs();
        sb.append(
          String.format("lat-sleeps=%d(%.1fs); ", curLatencySleeps, curLatencyUs / 1_000_000.0));
      }
      sb.append(String.format("cumulative: R=%s W=%s, R-ops=%d W-ops=%d, dev-R=%d dev-W=%d",
        formatBytes(curBytesRead), formatBytes(curBytesWritten), curReadOps, curWriteOps,
        curDeviceReadOps, curDeviceWriteOps));
      LOG.info(sb.toString());

      prevBytesRead = curBytesRead;
      prevBytesWritten = curBytesWritten;
      prevBwReadSleepMs = curBwReadSleepMs;
      prevBwWriteSleepMs = curBwWriteSleepMs;
      prevBwReadSleepCount = curBwReadSleepCount;
      prevBwWriteSleepCount = curBwWriteSleepCount;
      prevReadOps = curReadOps;
      prevWriteOps = curWriteOps;
      prevIopsReadSleeps = curIopsReadSleeps;
      prevIopsWriteSleeps = curIopsWriteSleeps;
      prevDeviceReadOps = curDeviceReadOps;
      prevDeviceWriteOps = curDeviceWriteOps;
    }
  }

  static String formatBytes(long bytes) {
    if (bytes >= 1024L * 1024L * 1024L) {
      return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    } else if (bytes >= 1024L * 1024L) {
      return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
    } else {
      return String.format("%d bytes", bytes);
    }
  }
}
