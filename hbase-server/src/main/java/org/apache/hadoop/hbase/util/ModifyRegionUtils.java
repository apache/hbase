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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;

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
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
    ThreadPoolExecutor regionOpenAndInitThreadPool = getRegionOpenAndInitThreadPool(conf,
        "RegionOpenAndInitThread-" + hTableDescriptor.getTableName(), regionNumber);
    CompletionService<HRegionInfo> completionService = new ExecutorCompletionService<HRegionInfo>(
        regionOpenAndInitThreadPool);
    List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
    for (final HRegionInfo newRegion : newRegions) {
      completionService.submit(new Callable<HRegionInfo>() {
        public HRegionInfo call() throws IOException {
          // 1. Create HRegion
          HRegion region = HRegion.createHRegion(newRegion,
              rootDir, conf, hTableDescriptor, null,
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
      });
    }
    try {
      // 4. wait for all regions to finish creation
      for (int i = 0; i < regionNumber; i++) {
        Future<HRegionInfo> future = completionService.take();
        HRegionInfo regionInfo = future.get();
        regionInfos.add(regionInfo);
      }
    } catch (InterruptedException e) {
      LOG.error("Caught " + e + " during region creation");
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e);
    } finally {
      regionOpenAndInitThreadPool.shutdownNow();
    }
    return regionInfos;
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

          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, threadNamePrefix + "-" + count++);
            return t;
          }
        });
    return regionOpenAndInitThreadPool;
  }
}
