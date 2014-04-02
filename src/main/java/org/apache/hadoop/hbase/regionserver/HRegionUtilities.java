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
import java.util.ArrayList;
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
   * In some cases the end region can have empty end row. Special casing this
   * in the isValid check.
   * @param regionEndKey
   * @return
   */
  public static boolean isRegionWithEmptyEndKey(byte[] regionEndKey) {
    return regionEndKey.length == 0;
  }

  /**
   * A bucket is not valid if
   *  * bucket falls before the region boundaries.
   *  * bucket falls after the region boundaries.
   *  * bucket has same start row and end row.
   * @param b
   * @param regionStartKey
   * @param regionEndKey
   * @return
   */
  public static boolean isValidBucket(Bucket b, byte[] regionStartKey,
      byte[] regionEndKey) {
    if (Bytes.compareTo(regionStartKey, b.getEndRow()) >= 0) {
      return false;
    }
    if (!isRegionWithEmptyEndKey(regionEndKey)
        && Bytes.compareTo(regionEndKey, b.getStartRow()) <= 0) {
      return false;
    }
    if (Bytes.compareTo(b.getStartRow(), b.getEndRow()) == 0) {
      return false;
    }
    return true;
  }

  /**
   * Picking the buckets within the valid range of the [startKey, endKey)
   * and adjust the start and end rows of the start and end buckets of the
   * @param buckets
   * @return
   */
  public static List<Bucket> adjustHistogramBoundariesToRegionBoundaries(
      List<Bucket> buckets, byte[] startKey, byte[] endKey) {
    int size = buckets.size();
    Preconditions.checkArgument(size > 1);
    List<Bucket> retbuckets = new ArrayList<Bucket> (size);
    for (Bucket b : buckets) {
      if (isValidBucket(b, startKey, endKey)) {
        retbuckets.add(b);
      }
    }
    size = retbuckets.size();
    if (size == 0) return null;
    Bucket startBucket = retbuckets.get(0);
    Bucket endBucket = retbuckets.get(size - 1);
    retbuckets.set(0, new HFileHistogram.Bucket.Builder(startBucket)
      .setStartRow(startKey).create());
    retbuckets.set(size - 1, new HFileHistogram.Bucket.Builder(endBucket)
      .setEndRow(endKey).create());
    return retbuckets;
  }
}
