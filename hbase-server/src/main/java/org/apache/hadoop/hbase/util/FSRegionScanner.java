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
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.RegionStorage;
import org.apache.hadoop.hbase.fs.RegionStorage.StoreFileVisitor;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Thread that walks over the filesystem, and computes the mappings
 * Region -> BestHost and Region -> {@code Map<HostName, fractional-locality-of-region>}
 *
 */
@InterfaceAudience.Private
class FSRegionScanner implements Runnable {
  static private final Log LOG = LogFactory.getLog(FSRegionScanner.class);

  private final RegionStorage rfs;

  /**
   * Maps region encoded names to maps of hostnames to fractional locality of
   * that region on that host.
   */
  private final Map<String, Map<String, Float>> regionDegreeLocalityMapping;

  FSRegionScanner(Configuration conf, HRegionInfo hri,
                  Map<String, Map<String, Float>> regionDegreeLocalityMapping)
      throws IOException {
    this.rfs = RegionStorage.open(conf, hri, true);
    this.regionDegreeLocalityMapping = regionDegreeLocalityMapping;
  }

  @Override
  public void run() {
    try {
      // empty the map for each region
      final Map<String, AtomicInteger> blockCountMap = new HashMap<String, AtomicInteger>();
      final AtomicInteger totalBlkCount = new AtomicInteger(0);

      rfs.visitStoreFiles(new StoreFileVisitor() {
        @Override
        public void storeFile(HRegionInfo region, String family, StoreFileInfo storeFile)
            throws IOException {
          BlockLocation[] blkLocations = storeFile.getFileBlockLocations(rfs.getFileSystem());
          if (blkLocations == null) return;

          totalBlkCount.addAndGet(blkLocations.length);
          for(BlockLocation blk: blkLocations) {
            for (String host: blk.getHosts()) {
              AtomicInteger count = blockCountMap.get(host);
              if (count == null) {
                count = new AtomicInteger(0);
                blockCountMap.put(host, count);
              }
              count.incrementAndGet();
            }
          }
        }
      });

      if (regionDegreeLocalityMapping != null && totalBlkCount.get() > 0) {
        Map<String, Float> hostLocalityMap = new HashMap<String, Float>();
        for (Map.Entry<String, AtomicInteger> entry : blockCountMap.entrySet()) {
          String host = entry.getKey();
          if (host.endsWith(".")) {
            host = host.substring(0, host.length() - 1);
          }
          // Locality is fraction of blocks local to this host.
          float locality = ((float)entry.getValue().get()) / totalBlkCount.get();
          hostLocalityMap.put(host, locality);
        }
        // Put the locality map into the result map, keyed by the encoded name
        // of the region.
        regionDegreeLocalityMapping.put(rfs.getRegionInfo().getEncodedName(), hostLocalityMap);
      }
    } catch (IOException e) {
      LOG.warn("Problem scanning file system", e);
    } catch (RuntimeException e) {
      LOG.warn("Problem scanning file system", e);
    }
  }
}
