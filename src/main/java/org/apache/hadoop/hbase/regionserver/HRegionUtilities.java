/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

public class HRegionUtilities {
  private static void checkFamily(final byte [] family, HRegionInfo regionInfo)
  throws NoSuchColumnFamilyException {
    if(!regionInfo.getTableDesc().hasFamily(family)) {
      throw new NoSuchColumnFamilyException("Column family " +
          Bytes.toString(family) + " does not exist in region " +
          regionInfo.getRegionNameAsString() + " in table " +
          regionInfo.getTableDesc());
    }
  }

  /**
   * Function to read the families registered with the scan object and validate
   * them against the families actually present in the table.
   * This function will add all the families if there were no families mentioned
   * @param scan : The scan object provided by the client.
   * @param regionInfo : The regionInfo object for the current region
   * in the table
   * @throws NoSuchColumnFamilyException
   */
  public static void checkAndAddFamilies(Scan scan, HRegionInfo regionInfo)
      throws NoSuchColumnFamilyException {
    // Verify families are all valid
    if(scan.hasFamilies()) {
      for(byte [] family : scan.getFamilyMap().keySet()) {
        HRegionUtilities.checkFamily(family, regionInfo);
      }
    } else { // Adding all families to scanner
      for(byte[] family: regionInfo.getTableDesc().getFamiliesKeys()){
        scan.addFamily(family);
      }
    }
  }

  // TODO(manukranthk) : Refactor HRegion code to use this common code path.
  public static void parallelStoreOpener(final HRegionInfo info,
      final Configuration conf, Collection<HColumnDescriptor> families,
      final Path tableDir, final FileSystem fs, Map<byte[], Store> stores,
      final boolean createNewHardlinks) throws IOException{
    // initialize the thread pool for opening stores in parallel.
    ThreadPoolExecutor storeOpenerThreadPool =
      StoreThreadUtils.getStoreOpenAndCloseThreadPool("StoreOpenerThread-"
    + info.getRegionNameAsString(), info, conf);
    CompletionService<Store> completionService =
      new ExecutorCompletionService<Store>(storeOpenerThreadPool);

    // initialize each store in parallel
    for (final HColumnDescriptor family : families) {
      completionService.submit(new Callable<Store>() {
        public Store call() throws IOException {
          return new ReadOnlyStore(tableDir, info, family,
              fs, conf, createNewHardlinks);
        }
      });
    }

    try {
      for (int i = 0; i < families.size(); i++) {
        Future<Store> future = completionService.take();
        Store store = future.get();
        stores.put(store.getColumnFamilyName().getBytes(), store);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      storeOpenerThreadPool.shutdownNow();
    }
  }

  /**
   * Adjusting the startRow of startBucket to region's startRow
   * and endRow of endBucket to region's endRow.
   * Modifies the current list
   * @param buckets
   * @return
   */
  public static List<Bucket> adjustHistogramBoundariesToRegionBoundaries(
      List<Bucket> buckets, byte[] startKey, byte[] endKey) {
    int size = buckets.size();
    Preconditions.checkArgument(size > 1);
    Bucket startBucket = buckets.get(0);
    Bucket endBucket = buckets.get(size - 1);
    buckets.set(0, new HFileHistogram.Bucket.Builder(startBucket)
      .setStartRow(startKey).create());
    buckets.set(size - 1, new HFileHistogram.Bucket.Builder(endBucket)
      .setEndRow(endKey).create());
    return buckets;
  }
}
