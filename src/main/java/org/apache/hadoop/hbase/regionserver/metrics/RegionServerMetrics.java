/**
 *
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
package org.apache.hadoop.hbase.regionserver.metrics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.metrics.HBaseInfo;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.metrics.PersistentMetricsTimeVaryingRate;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;
import com.yammer.metrics.stats.Snapshot;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class RegionServerMetrics implements Updater {
  @SuppressWarnings({"FieldCanBeLocal"})
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final MetricsRecord metricsRecord;
  private long lastUpdate = System.currentTimeMillis();
  private long lastExtUpdate = System.currentTimeMillis();
  private long extendedPeriod = 0;
  private static final int MB = 1024*1024;
  private MetricsRegistry registry = new MetricsRegistry();
  private final RegionServerStatistics statistics;

  public final MetricsTimeVaryingRate atomicIncrementTime =
      new MetricsTimeVaryingRate("atomicIncrementTime", registry);

  /**
   * Count of regions carried by this regionserver
   */
  public final MetricsIntValue regions =
    new MetricsIntValue("regions", registry);

  /**
   * Block cache size.
   */
  public final MetricsLongValue blockCacheSize = 
      new MetricsLongValue("blockCacheSize", registry);

  /**
   * Block cache free size.
   */
  public final MetricsLongValue blockCacheFree = 
      new MetricsLongValue("blockCacheFree", registry);

  /**
   * Block cache item count.
   */
  public final MetricsLongValue blockCacheCount = 
      new MetricsLongValue("blockCacheCount", registry);

  /**
   * Block cache hit count.
   */
  public final MetricsLongValue blockCacheHitCount = 
      new MetricsLongValue("blockCacheHitCount", registry);

  /**
   * Block cache miss count.
   */
  public final MetricsLongValue blockCacheMissCount = 
      new MetricsLongValue("blockCacheMissCount", registry);

  /**
   * Block cache evict count.
   */
  public final MetricsLongValue blockCacheEvictedCount = 
      new MetricsLongValue("blockCacheEvictedCount", registry);

  /**
   * Block hit ratio.
   */
  public final MetricsIntValue blockCacheHitRatio = 
      new MetricsIntValue("blockCacheHitRatio", registry);

  /**
   * Block hit caching ratio.  This only includes the requests to the block
   * cache where caching was turned on.  See HBASE-2253.
   */
  public final MetricsIntValue blockCacheHitCachingRatio = 
      new MetricsIntValue("blockCacheHitCachingRatio", registry);

  /** Block hit ratio for past N periods. */
  public final MetricsIntValue blockCacheHitRatioPastNPeriods = new MetricsIntValue("blockCacheHitRatioPastNPeriods", registry);

  /** Block hit caching ratio for past N periods */
  public final MetricsIntValue blockCacheHitCachingRatioPastNPeriods = new MetricsIntValue("blockCacheHitCachingRatioPastNPeriods", registry);

  /*
   * Count of requests to the regionservers since last call to metrics update
   */
  public final MetricsRate requests = new MetricsRate("requests", registry);

  /**
   * Count of stores open on the regionserver.
   */
  public final MetricsIntValue stores = new MetricsIntValue("stores", registry);

  /**
   * Count of storefiles open on the regionserver.
   */
  public final MetricsIntValue storefiles = 
      new MetricsIntValue("storefiles", registry);

  /**
   * Count of hlogfiles
   */
  public final MetricsIntValue hlogFileCount = 
      new MetricsIntValue("hlogFileCount", registry);
  
  /**
   * the total size of hlog files in MB
   */
  public final MetricsLongValue hlogFileSizeMB = 
      new MetricsLongValue("hlogFileSizeMB", registry);
  
  /**
   * Count of read requests
   */
  public final MetricsLongValue readRequestsCount = 
      new MetricsLongValue("readRequestsCount", registry);

  /**
   * Count of write requests
   */
  public final MetricsLongValue writeRequestsCount = 
      new MetricsLongValue("writeRequestsCount", registry);

  /**
   */
  public final MetricsIntValue storefileIndexSizeMB =
    new MetricsIntValue("storefileIndexSizeMB", registry);

  /** The total size of block index root levels in this regionserver in KB. */
  public final MetricsIntValue rootIndexSizeKB =
    new MetricsIntValue("rootIndexSizeKB", registry);

  /** Total size of all block indexes (not necessarily loaded in memory) */
  public final MetricsIntValue totalStaticIndexSizeKB =
    new MetricsIntValue("totalStaticIndexSizeKB", registry);

  /** Total size of all Bloom filters (not necessarily loaded in memory) */
  public final MetricsIntValue totalStaticBloomSizeKB =
    new MetricsIntValue("totalStaticBloomSizeKB", registry);

  /**
   * HDFS blocks locality index
   */
  public final MetricsIntValue hdfsBlocksLocalityIndex =
    new MetricsIntValue("hdfsBlocksLocalityIndex", registry);
  
  /**
   * Sum of all the memstore sizes in this regionserver in MB
   */
  public final MetricsIntValue memstoreSizeMB =
    new MetricsIntValue("memstoreSizeMB", registry);

  /**
   * Number of put with WAL disabled in this regionserver in MB
   */
  public final MetricsLongValue numPutsWithoutWAL =
    new MetricsLongValue("numPutsWithoutWAL", registry);

  /**
   * Possible data loss sizes (due to put with WAL disabled) in this regionserver in MB
   */
  public final MetricsIntValue mbInMemoryWithoutWAL =
    new MetricsIntValue("mbInMemoryWithoutWAL", registry);

  /**
   * Size of the compaction queue.
   */
  public final MetricsIntValue compactionQueueSize =
    new MetricsIntValue("compactionQueueSize", registry);
  
  /**
   * Size of the flush queue.
   */
  public final MetricsIntValue flushQueueSize =
    new MetricsIntValue("flushQueueSize", registry);

  /**
   * filesystem sequential read latency distribution
   */
  public final MetricsHistogram fsReadLatencyHistogram = 
      new MetricsHistogram("fsReadLatencyHistogram", registry);

  /**
   * filesystem pread latency distribution
   */
  public final MetricsHistogram fsPreadLatencyHistogram = 
      new MetricsHistogram("fsPreadLatencyHistogram", registry);

  /**
   * Metrics on the distribution of filesystem write latencies (improved version of fsWriteLatency)
   */
  public final MetricsHistogram fsWriteLatencyHistogram = 
      new MetricsHistogram("fsWriteLatencyHistogram", registry);

  
  /**
   * filesystem read latency
   */
  public final MetricsTimeVaryingRate fsReadLatency =
    new MetricsTimeVaryingRate("fsReadLatency", registry);

  /**
   * filesystem positional read latency
   */
  public final MetricsTimeVaryingRate fsPreadLatency =
    new MetricsTimeVaryingRate("fsPreadLatency", registry);

  /**
   * filesystem write latency
   */
  public final MetricsTimeVaryingRate fsWriteLatency =
    new MetricsTimeVaryingRate("fsWriteLatency", registry);

  /**
   * size (in bytes) of data in HLog append calls
   */
  public final MetricsTimeVaryingRate fsWriteSize =
    new MetricsTimeVaryingRate("fsWriteSize", registry);

  /**
   * filesystem sync latency
   */
  public final MetricsTimeVaryingRate fsSyncLatency =
    new MetricsTimeVaryingRate("fsSyncLatency", registry);

  
  /**
   * time each scheduled compaction takes
   */
  protected final PersistentMetricsTimeVaryingRate compactionTime =
    new PersistentMetricsTimeVaryingRate("compactionTime", registry);

  protected final PersistentMetricsTimeVaryingRate compactionSize =
    new PersistentMetricsTimeVaryingRate("compactionSize", registry);

  /**
   * time each scheduled flush takes
   */
  protected final PersistentMetricsTimeVaryingRate flushTime =
    new PersistentMetricsTimeVaryingRate("flushTime", registry);

  protected final PersistentMetricsTimeVaryingRate flushSize =
    new PersistentMetricsTimeVaryingRate("flushSize", registry);
  
  public final MetricsLongValue slowHLogAppendCount =
      new MetricsLongValue("slowHLogAppendCount", registry);
  
  public final MetricsTimeVaryingRate slowHLogAppendTime =
      new MetricsTimeVaryingRate("slowHLogAppendTime", registry);
  
  public final MetricsTimeVaryingLong regionSplitSuccessCount =
      new MetricsTimeVaryingLong("regionSplitSuccessCount", registry);
  
  public final MetricsTimeVaryingLong regionSplitFailureCount =
      new MetricsTimeVaryingLong("regionSplitFailureCount", registry);

  /**
   * Number of times checksum verification failed.
   */
  public final MetricsLongValue checksumFailuresCount =
    new MetricsLongValue("checksumFailuresCount", registry);

  /**
   * time blocked on lack of resources
   */
  public final MetricsLongValue updatesBlockedSeconds = new MetricsLongValue(
      "updatesBlockedSeconds", registry);

  /**
   * time blocked on memstoreHW
   */
  public final MetricsLongValue updatesBlockedSecondsHighWater = new MetricsLongValue(
      "updatesBlockedSecondsHighWater",registry);

  public RegionServerMetrics() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "regionserver");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);
    // Add Hbase Info metrics
    HBaseInfo.init();

    // export for JMX
    statistics = new RegionServerStatistics(this.registry, name);

    // get custom attributes
    try {
      Object m = ContextFactory.getFactory().getAttribute("hbase.extendedperiod");
      if (m instanceof String) {
        this.extendedPeriod = Long.parseLong((String) m)*1000;
      }
    } catch (IOException ioe) {
      LOG.info("Couldn't load ContextFactory for Metrics config info");
    }

    LOG.info("Initialized");
  }

  public void shutdown() {
    if (statistics != null)
      statistics.shutdown();
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param caller the metrics context that this responsible for calling us
   */
  public void doUpdates(MetricsContext caller) {
    synchronized (this) {
      this.lastUpdate = System.currentTimeMillis();

      // has the extended period for long-living stats elapsed?
      if (this.extendedPeriod > 0 &&
          this.lastUpdate - this.lastExtUpdate >= this.extendedPeriod) {
        this.lastExtUpdate = this.lastUpdate;
        this.compactionTime.resetMinMaxAvg();
        this.compactionSize.resetMinMaxAvg();
        this.flushTime.resetMinMaxAvg();
        this.flushSize.resetMinMaxAvg();
        this.resetAllMinMax();
      }

      this.stores.pushMetric(this.metricsRecord);
      this.storefiles.pushMetric(this.metricsRecord);
      this.hlogFileCount.pushMetric(this.metricsRecord);
      this.hlogFileSizeMB.pushMetric(this.metricsRecord);
      this.storefileIndexSizeMB.pushMetric(this.metricsRecord);
      this.rootIndexSizeKB.pushMetric(this.metricsRecord);
      this.totalStaticIndexSizeKB.pushMetric(this.metricsRecord);
      this.totalStaticBloomSizeKB.pushMetric(this.metricsRecord);
      this.memstoreSizeMB.pushMetric(this.metricsRecord);
      this.mbInMemoryWithoutWAL.pushMetric(this.metricsRecord);
      this.numPutsWithoutWAL.pushMetric(this.metricsRecord);
      this.readRequestsCount.pushMetric(this.metricsRecord);
      this.writeRequestsCount.pushMetric(this.metricsRecord);
      this.regions.pushMetric(this.metricsRecord);
      this.requests.pushMetric(this.metricsRecord);
      this.compactionQueueSize.pushMetric(this.metricsRecord);
      this.flushQueueSize.pushMetric(this.metricsRecord);
      this.blockCacheSize.pushMetric(this.metricsRecord);
      this.blockCacheFree.pushMetric(this.metricsRecord);
      this.blockCacheCount.pushMetric(this.metricsRecord);
      this.blockCacheHitCount.pushMetric(this.metricsRecord);
      this.blockCacheMissCount.pushMetric(this.metricsRecord);
      this.blockCacheEvictedCount.pushMetric(this.metricsRecord);
      this.blockCacheHitRatio.pushMetric(this.metricsRecord);
      this.blockCacheHitCachingRatio.pushMetric(this.metricsRecord);
      this.hdfsBlocksLocalityIndex.pushMetric(this.metricsRecord);
      this.blockCacheHitRatioPastNPeriods.pushMetric(this.metricsRecord);
      this.blockCacheHitCachingRatioPastNPeriods.pushMetric(this.metricsRecord);

      // Mix in HFile and HLog metrics
      // Be careful. Here is code for MTVR from up in hadoop:
      // public synchronized void inc(final int numOps, final long time) {
      //   currentData.numOperations += numOps;
      //   currentData.time += time;
      //   long timePerOps = time/numOps;
      //    minMax.update(timePerOps);
      // }
      // Means you can't pass a numOps of zero or get a ArithmeticException / by zero.
      // HLog metrics
      addHLogMetric(HLog.getWriteTime(), this.fsWriteLatency);
      addHLogMetric(HLog.getWriteSize(), this.fsWriteSize);
      addHLogMetric(HLog.getSyncTime(), this.fsSyncLatency);
      addHLogMetric(HLog.getSlowAppendTime(), this.slowHLogAppendTime);
      this.slowHLogAppendCount.set(HLog.getSlowAppendCount());
      // HFile metrics, sequential reads
      int ops = HFile.getReadOps(); 
      if (ops != 0) this.fsReadLatency.inc(ops, HFile.getReadTimeMs());
      // HFile metrics, positional reads
      ops = HFile.getPreadOps(); 
      if (ops != 0) this.fsPreadLatency.inc(ops, HFile.getPreadTimeMs());
      this.checksumFailuresCount.set(HFile.getChecksumFailuresCount());

      /* NOTE: removed HFile write latency.  2 reasons:
       * 1) Mixing HLog latencies are far higher priority since they're 
       *      on-demand and HFile is used in background (compact/flush)
       * 2) HFile metrics are being handled at a higher level 
       *      by compaction & flush metrics.
       */

      for(Long latency : HFile.getReadLatenciesNanos()) {
        this.fsReadLatencyHistogram.update(latency);
      }
      for(Long latency : HFile.getPreadLatenciesNanos()) {
        this.fsPreadLatencyHistogram.update(latency);
      }
      for(Long latency : HFile.getWriteLatenciesNanos()) {
        this.fsWriteLatencyHistogram.update(latency);
      }
            

      // push the result
      this.fsPreadLatency.pushMetric(this.metricsRecord);
      this.fsReadLatency.pushMetric(this.metricsRecord);
      this.fsWriteLatency.pushMetric(this.metricsRecord);
      this.fsWriteSize.pushMetric(this.metricsRecord);
      
      this.fsReadLatencyHistogram.pushMetric(this.metricsRecord);
      this.fsWriteLatencyHistogram.pushMetric(this.metricsRecord);
      this.fsPreadLatencyHistogram.pushMetric(this.metricsRecord);

      this.fsSyncLatency.pushMetric(this.metricsRecord);
      this.compactionTime.pushMetric(this.metricsRecord);
      this.compactionSize.pushMetric(this.metricsRecord);
      this.flushTime.pushMetric(this.metricsRecord);
      this.flushSize.pushMetric(this.metricsRecord);
      this.slowHLogAppendCount.pushMetric(this.metricsRecord);
      this.regionSplitSuccessCount.pushMetric(this.metricsRecord);
      this.regionSplitFailureCount.pushMetric(this.metricsRecord);
      this.checksumFailuresCount.pushMetric(this.metricsRecord);
      this.updatesBlockedSeconds.pushMetric(this.metricsRecord);
      this.updatesBlockedSecondsHighWater.pushMetric(this.metricsRecord);
    }
    this.metricsRecord.update();
  }

  private void addHLogMetric(HLog.Metric logMetric,
      MetricsTimeVaryingRate hadoopMetric) {
    if (logMetric.count > 0)
      hadoopMetric.inc(logMetric.min);
    if (logMetric.count > 1)
      hadoopMetric.inc(logMetric.max);
    if (logMetric.count > 2) {
      int ops = logMetric.count - 2;
      hadoopMetric.inc(ops, logMetric.total - logMetric.max - logMetric.min);
    }
  }

  public void resetAllMinMax() {
    this.atomicIncrementTime.resetMinMax();
    this.fsReadLatency.resetMinMax();
    this.fsWriteLatency.resetMinMax();
    this.fsWriteSize.resetMinMax();
    this.fsSyncLatency.resetMinMax();
    this.slowHLogAppendTime.resetMinMax();
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return this.requests.getPreviousIntervalValue();
  }

  /**
   * @param time time that compaction took
   * @param size bytesize of storefiles in the compaction
   */
  public synchronized void addCompaction(long time, long size) {
    this.compactionTime.inc(time);
    this.compactionSize.inc(size);
  }

  /**
   * @param flushes history in <time, size>
   */
  public synchronized void addFlush(final List<Pair<Long,Long>> flushes) {
    for (Pair<Long,Long> f : flushes) {
      this.flushTime.inc(f.getFirst());
      this.flushSize.inc(f.getSecond());
    }
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    this.requests.inc(inc);
  }
  
  public void incrementSplitSuccessCount() {
    this.regionSplitSuccessCount.inc();
  }
  
  public void incrementSplitFailureCount() {
    this.regionSplitFailureCount.inc();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb = Strings.appendKeyValue(sb, "requestsPerSecond", Integer
        .valueOf((int) this.requests.getPreviousIntervalValue()));
    sb = Strings.appendKeyValue(sb, "numberOfOnlineRegions",
      Integer.valueOf(this.regions.get()));
    sb = Strings.appendKeyValue(sb, "numberOfStores",
      Integer.valueOf(this.stores.get()));
    sb = Strings.appendKeyValue(sb, this.hlogFileCount.getName(),
      Integer.valueOf(this.hlogFileCount.get()));
    sb = Strings.appendKeyValue(sb, this.hlogFileSizeMB.getName(),
      Long.valueOf(this.hlogFileSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "numberOfStorefiles",
      Integer.valueOf(this.storefiles.get()));
    sb = Strings.appendKeyValue(sb, this.storefileIndexSizeMB.getName(),
      Integer.valueOf(this.storefileIndexSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "rootIndexSizeKB",
        Integer.valueOf(this.rootIndexSizeKB.get()));
    sb = Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
        Integer.valueOf(this.totalStaticIndexSizeKB.get()));
    sb = Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
        Integer.valueOf(this.totalStaticBloomSizeKB.get()));
    sb = Strings.appendKeyValue(sb, this.memstoreSizeMB.getName(),
      Integer.valueOf(this.memstoreSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "mbInMemoryWithoutWAL",
      Integer.valueOf(this.mbInMemoryWithoutWAL.get()));
    sb = Strings.appendKeyValue(sb, "numberOfPutsWithoutWAL",
      Long.valueOf(this.numPutsWithoutWAL.get()));
    sb = Strings.appendKeyValue(sb, "readRequestsCount",
        Long.valueOf(this.readRequestsCount.get()));
    sb = Strings.appendKeyValue(sb, "writeRequestsCount",
        Long.valueOf(this.writeRequestsCount.get()));
    sb = Strings.appendKeyValue(sb, "compactionQueueSize",
      Integer.valueOf(this.compactionQueueSize.get()));
    sb = Strings.appendKeyValue(sb, "flushQueueSize",
      Integer.valueOf(this.flushQueueSize.get()));
    // Duplicate from jvmmetrics because metrics are private there so
    // inaccessible.
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    sb = Strings.appendKeyValue(sb, "usedHeapMB",
      Long.valueOf(memory.getUsed()/MB));
    sb = Strings.appendKeyValue(sb, "maxHeapMB",
      Long.valueOf(memory.getMax()/MB));
    sb = Strings.appendKeyValue(sb, this.blockCacheSize.getName()+"MB",
    	StringUtils.limitDecimalTo2((float)this.blockCacheSize.get()/MB));
    sb = Strings.appendKeyValue(sb, this.blockCacheFree.getName()+"MB",
    	StringUtils.limitDecimalTo2((float)this.blockCacheFree.get()/MB));
    sb = Strings.appendKeyValue(sb, this.blockCacheCount.getName(),
        Long.valueOf(this.blockCacheCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitCount.getName(),
        Long.valueOf(this.blockCacheHitCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheMissCount.getName(),
        Long.valueOf(this.blockCacheMissCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheEvictedCount.getName(),
        Long.valueOf(this.blockCacheEvictedCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitRatio.getName(),
        Long.valueOf(this.blockCacheHitRatio.get())+"%");
    sb = Strings.appendKeyValue(sb, this.blockCacheHitCachingRatio.getName(),
        Long.valueOf(this.blockCacheHitCachingRatio.get())+"%");
    sb = Strings.appendKeyValue(sb, this.hdfsBlocksLocalityIndex.getName(),
        Long.valueOf(this.hdfsBlocksLocalityIndex.get()));
    sb = Strings.appendKeyValue(sb, "slowHLogAppendCount",
        Long.valueOf(this.slowHLogAppendCount.get()));
    sb = appendHistogram(sb, this.fsReadLatencyHistogram);
    sb = appendHistogram(sb, this.fsPreadLatencyHistogram);
    sb = appendHistogram(sb, this.fsWriteLatencyHistogram);

    return sb.toString();
  }
  
  private StringBuilder appendHistogram(StringBuilder sb, 
      MetricsHistogram histogram) {
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "Mean", 
        StringUtils.limitDecimalTo2(histogram.getMean()));
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "Count", 
        StringUtils.limitDecimalTo2(histogram.getCount()));
    final Snapshot s = histogram.getSnapshot();
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "Median", 
        StringUtils.limitDecimalTo2(s.getMedian()));
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "75th", 
        StringUtils.limitDecimalTo2(s.get75thPercentile()));
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "95th", 
        StringUtils.limitDecimalTo2(s.get95thPercentile()));
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "99th", 
        StringUtils.limitDecimalTo2(s.get99thPercentile()));
    sb = Strings.appendKeyValue(sb, 
        histogram.getName() + "999th", 
        StringUtils.limitDecimalTo2(s.get999thPercentile()));
    return sb;
  }
}
