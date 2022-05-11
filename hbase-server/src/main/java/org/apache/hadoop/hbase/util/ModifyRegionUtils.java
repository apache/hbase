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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Utility methods for interacting with the regions.
 */
@InterfaceAudience.Private
public abstract class ModifyRegionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ModifyRegionUtils.class);

  private ModifyRegionUtils() {
  }

  public interface RegionFillTask {
    void fillRegion(final HRegion region) throws IOException;
  }

  public interface RegionEditTask {
    void editRegion(final RegionInfo region) throws IOException;
  }

  public static RegionInfo[] createRegionInfos(TableDescriptor tableDescriptor,
    byte[][] splitKeys) {
    long regionId = System.currentTimeMillis();
    RegionInfo[] hRegionInfos = null;
    if (splitKeys == null || splitKeys.length == 0) {
      hRegionInfos = new RegionInfo[] { RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
        .setStartKey(null).setEndKey(null).setSplit(false).setRegionId(regionId).build() };
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new RegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        hRegionInfos[i] = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
          .setStartKey(startKey).setEndKey(endKey).setSplit(false).setRegionId(regionId).build();
        startKey = endKey;
      }
    }
    return hRegionInfos;
  }

  /**
   * Create new set of regions on the specified file-system. NOTE: that you should add the regions
   * to hbase:meta after this operation.
   * @param conf            {@link Configuration}
   * @param rootDir         Root directory for HBase instance
   * @param tableDescriptor description of the table
   * @param newRegions      {@link RegionInfo} that describes the regions to create
   * @param task            {@link RegionFillTask} custom code to populate region after creation n
   */
  public static List<RegionInfo> createRegions(final Configuration conf, final Path rootDir,
    final TableDescriptor tableDescriptor, final RegionInfo[] newRegions, final RegionFillTask task)
    throws IOException {
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
    ThreadPoolExecutor exec = getRegionOpenAndInitThreadPool(conf,
      "RegionOpenAndInit-" + tableDescriptor.getTableName(), regionNumber);
    try {
      return createRegions(exec, conf, rootDir, tableDescriptor, newRegions, task);
    } finally {
      exec.shutdownNow();
    }
  }

  /**
   * Create new set of regions on the specified file-system. NOTE: that you should add the regions
   * to hbase:meta after this operation.
   * @param exec            Thread Pool Executor
   * @param conf            {@link Configuration}
   * @param rootDir         Root directory for HBase instance
   * @param tableDescriptor description of the table
   * @param newRegions      {@link RegionInfo} that describes the regions to create
   * @param task            {@link RegionFillTask} custom code to populate region after creation n
   */
  public static List<RegionInfo> createRegions(final ThreadPoolExecutor exec,
    final Configuration conf, final Path rootDir, final TableDescriptor tableDescriptor,
    final RegionInfo[] newRegions, final RegionFillTask task) throws IOException {
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
    CompletionService<RegionInfo> completionService = new ExecutorCompletionService<>(exec);
    List<RegionInfo> regionInfos = new ArrayList<>();
    for (final RegionInfo newRegion : newRegions) {
      completionService.submit(new Callable<RegionInfo>() {
        @Override
        public RegionInfo call() throws IOException {
          return createRegion(conf, rootDir, tableDescriptor, newRegion, task);
        }
      });
    }
    try {
      // wait for all regions to finish creation
      for (int i = 0; i < regionNumber; i++) {
        regionInfos.add(completionService.take().get());
      }
    } catch (InterruptedException e) {
      LOG.error("Caught " + e + " during region creation");
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    return regionInfos;
  }

  /**
   * Create new set of regions on the specified file-system.
   * @param conf            {@link Configuration}
   * @param rootDir         Root directory for HBase instance
   * @param tableDescriptor description of the table
   * @param newRegion       {@link RegionInfo} that describes the region to create
   * @param task            {@link RegionFillTask} custom code to populate region after creation n
   */
  public static RegionInfo createRegion(final Configuration conf, final Path rootDir,
    final TableDescriptor tableDescriptor, final RegionInfo newRegion, final RegionFillTask task)
    throws IOException {
    // 1. Create HRegion
    // The WAL subsystem will use the default rootDir rather than the passed in rootDir
    // unless I pass along via the conf.
    Configuration confForWAL = new Configuration(conf);
    confForWAL.set(HConstants.HBASE_DIR, rootDir.toString());
    HRegion region = HRegion.createHRegion(newRegion, rootDir, conf, tableDescriptor, null, false);
    try {
      // 2. Custom user code to interact with the created region
      if (task != null) {
        task.fillRegion(region);
      }
    } finally {
      // 3. Close the new region to flush to disk. Close log file too.
      region.close();
    }
    return region.getRegionInfo();
  }

  /**
   * Execute the task on the specified set of regions.
   * @param exec    Thread Pool Executor
   * @param regions {@link RegionInfo} that describes the regions to edit
   * @param task    {@link RegionFillTask} custom code to edit the region n
   */
  public static void editRegions(final ThreadPoolExecutor exec,
    final Collection<RegionInfo> regions, final RegionEditTask task) throws IOException {
    final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(exec);
    for (final RegionInfo hri : regions) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          task.editRegion(hri);
          return null;
        }
      });
    }

    try {
      for (RegionInfo hri : regions) {
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  /*
   * used by createRegions() to get the thread pool executor based on the
   * "hbase.hregion.open.and.init.threads.max" property.
   */
  static ThreadPoolExecutor getRegionOpenAndInitThreadPool(final Configuration conf,
    final String threadNamePrefix, int regionNumber) {
    int maxThreads =
      Math.min(regionNumber, conf.getInt("hbase.hregion.open.and.init.threads.max", 16));
    ThreadPoolExecutor regionOpenAndInitThreadPool = Threads.getBoundedCachedThreadPool(maxThreads,
      30L, TimeUnit.SECONDS, new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    return regionOpenAndInitThreadPool;
  }
}
