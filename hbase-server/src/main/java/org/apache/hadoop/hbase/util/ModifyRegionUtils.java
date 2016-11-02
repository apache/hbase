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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.master.AssignmentManager;

/**
 * Utility methods for interacting with the regions.
 */
@InterfaceAudience.Private
public abstract class ModifyRegionUtils {
  private static final Log LOG = LogFactory.getLog(ModifyRegionUtils.class);

  private ModifyRegionUtils() {
  }

  public interface RegionFillTask {
    void fillRegion(final HRegion region) throws IOException;
  }

  public interface RegionEditTask {
    void editRegion(final HRegionInfo region) throws IOException;
  }

  public static HRegionInfo[] createHRegionInfos(HTableDescriptor hTableDescriptor,
      byte[][] splitKeys) {
    long regionId = System.currentTimeMillis();
    HRegionInfo[] hRegionInfos = null;
    if (splitKeys == null || splitKeys.length == 0) {
      hRegionInfos = new HRegionInfo[]{
        new HRegionInfo(hTableDescriptor.getTableName(), null, null, false, regionId)
      };
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new HRegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        hRegionInfos[i] =
             new HRegionInfo(hTableDescriptor.getTableName(), startKey, endKey,
                 false, regionId);
        startKey = endKey;
      }
    }
    return hRegionInfos;
  }

  /**
   * Create new set of regions on the specified file-system.
   * NOTE: that you should add the regions to hbase:meta after this operation.
   *
   * @param conf {@link Configuration}
   * @param rootDir Root directory for HBase instance
   * @param hTableDescriptor description of the table
   * @param newRegions {@link HRegionInfo} that describes the regions to create
   * @throws IOException
   */
  public static List<HRegionInfo> createRegions(final Configuration conf, final Path rootDir,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions) throws IOException {
    return createRegions(conf, rootDir, hTableDescriptor, newRegions, null);
  }

  /**
   * Create new set of regions on the specified file-system.
   * NOTE: that you should add the regions to hbase:meta after this operation.
   *
   * @param conf {@link Configuration}
   * @param rootDir Root directory for HBase instance
   * @param hTableDescriptor description of the table
   * @param newRegions {@link HRegionInfo} that describes the regions to create
   * @param task {@link RegionFillTask} custom code to populate region after creation
   * @throws IOException
   */
  public static List<HRegionInfo> createRegions(final Configuration conf, final Path rootDir,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions,
      final RegionFillTask task) throws IOException {

      Path tableDir = FSUtils.getTableDir(rootDir, hTableDescriptor.getTableName());
      return createRegions(conf, rootDir, tableDir, hTableDescriptor, newRegions, task);
  }

  /**
   * Create new set of regions on the specified file-system.
   * NOTE: that you should add the regions to hbase:meta after this operation.
   *
   * @param conf {@link Configuration}
   * @param rootDir Root directory for HBase instance
   * @param tableDir table directory
   * @param hTableDescriptor description of the table
   * @param newRegions {@link HRegionInfo} that describes the regions to create
   * @param task {@link RegionFillTask} custom code to populate region after creation
   * @throws IOException
   */
  public static List<HRegionInfo> createRegions(final Configuration conf, final Path rootDir,
      final Path tableDir, final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions,
      final RegionFillTask task) throws IOException {
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
    ThreadPoolExecutor exec = getRegionOpenAndInitThreadPool(conf,
        "RegionOpenAndInitThread-" + hTableDescriptor.getTableName(), regionNumber);
    try {
      return createRegions(exec, conf, rootDir, tableDir, hTableDescriptor, newRegions, task);
    } finally {
      exec.shutdownNow();
    }
  }

  /**
   * Create new set of regions on the specified file-system.
   * NOTE: that you should add the regions to hbase:meta after this operation.
   *
   * @param exec Thread Pool Executor
   * @param conf {@link Configuration}
   * @param rootDir Root directory for HBase instance
   * @param tableDir table directory
   * @param hTableDescriptor description of the table
   * @param newRegions {@link HRegionInfo} that describes the regions to create
   * @param task {@link RegionFillTask} custom code to populate region after creation
   * @throws IOException
   */
  public static List<HRegionInfo> createRegions(final ThreadPoolExecutor exec,
      final Configuration conf, final Path rootDir, final Path tableDir,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions,
      final RegionFillTask task) throws IOException {
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
    CompletionService<HRegionInfo> completionService =
      new ExecutorCompletionService<HRegionInfo>(exec);
    List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
    for (final HRegionInfo newRegion : newRegions) {
      completionService.submit(new Callable<HRegionInfo>() {
        @Override
        public HRegionInfo call() throws IOException {
          return createRegion(conf, rootDir, tableDir, hTableDescriptor, newRegion, task);
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
   * @param conf {@link Configuration}
   * @param rootDir Root directory for HBase instance
   * @param tableDir table directory
   * @param hTableDescriptor description of the table
   * @param newRegion {@link HRegionInfo} that describes the region to create
   * @param task {@link RegionFillTask} custom code to populate region after creation
   * @throws IOException
   */
  public static HRegionInfo createRegion(final Configuration conf, final Path rootDir,
      final Path tableDir, final HTableDescriptor hTableDescriptor, final HRegionInfo newRegion,
      final RegionFillTask task) throws IOException {
    // 1. Create HRegion
    HRegion region = HRegion.createHRegion(newRegion,
      rootDir, tableDir, conf, hTableDescriptor, null,
      false, true);
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
   *
   * @param exec Thread Pool Executor
   * @param regions {@link HRegionInfo} that describes the regions to edit
   * @param task {@link RegionFillTask} custom code to edit the region
   * @throws IOException
   */
  public static void editRegions(final ThreadPoolExecutor exec,
      final Collection<HRegionInfo> regions, final RegionEditTask task) throws IOException {
    final ExecutorCompletionService<Void> completionService =
      new ExecutorCompletionService<Void>(exec);
    for (final HRegionInfo hri: regions) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          task.editRegion(hri);
          return null;
        }
      });
    }

    try {
      for (HRegionInfo hri: regions) {
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      IOException ex = new IOException();
      ex.initCause(e.getCause());
      throw ex;
    }
  }

  /*
   * used by createRegions() to get the thread pool executor based on the
   * "hbase.hregion.open.and.init.threads.max" property.
   */
  static ThreadPoolExecutor getRegionOpenAndInitThreadPool(final Configuration conf,
      final String threadNamePrefix, int regionNumber) {
    int maxThreads = Math.min(regionNumber, conf.getInt(
        "hbase.hregion.open.and.init.threads.max", 10));
    ThreadPoolExecutor regionOpenAndInitThreadPool = Threads
    .getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
        new ThreadFactory() {
          private int count = 1;

          @Override
          public Thread newThread(Runnable r) {
            return new Thread(r, threadNamePrefix + "-" + count++);
          }
        });
    return regionOpenAndInitThreadPool;
  }

  /**
   * Triggers a bulk assignment of the specified regions
   *
   * @param assignmentManager the Assignment Manger
   * @param regionInfos the list of regions to assign
   * @throws IOException if an error occurred during the assignment
   */
  public static void assignRegions(final AssignmentManager assignmentManager,
      final List<HRegionInfo> regionInfos) throws IOException {
    try {
      assignmentManager.getRegionStates().createRegionStates(regionInfos);
      assignmentManager.assign(regionInfos);
    } catch (InterruptedException e) {
      LOG.error("Caught " + e + " during round-robin assignment");
      InterruptedIOException ie = new InterruptedIOException(e.getMessage());
      ie.initCause(e);
      throw ie;
    }
  }
}
