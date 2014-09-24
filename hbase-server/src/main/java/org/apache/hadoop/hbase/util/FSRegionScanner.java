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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Thread that walks over the filesystem, and computes the mappings
 * <Region -> BestHost> and <Region -> Map<HostName, fractional-locality-of-region>>
 *
 */
@InterfaceAudience.Private
class FSRegionScanner implements Runnable {
  static private final Log LOG = LogFactory.getLog(FSRegionScanner.class);

  private Path regionPath;

  /**
   * The file system used
   */
  private FileSystem fs;

  /**
   * Maps each region to the RS with highest locality for that region.
   */
  private Map<String,String> regionToBestLocalityRSMapping;

  /**
   * Maps region encoded names to maps of hostnames to fractional locality of
   * that region on that host.
   */
  private Map<String, Map<String, Float>> regionDegreeLocalityMapping;

  FSRegionScanner(FileSystem fs, Path regionPath,
                  Map<String, String> regionToBestLocalityRSMapping,
                  Map<String, Map<String, Float>> regionDegreeLocalityMapping) {
    this.fs = fs;
    this.regionPath = regionPath;
    this.regionToBestLocalityRSMapping = regionToBestLocalityRSMapping;
    this.regionDegreeLocalityMapping = regionDegreeLocalityMapping;
  }

  @Override
  public void run() {
    try {
      // empty the map for each region
      Map<String, AtomicInteger> blockCountMap = new HashMap<String, AtomicInteger>();

      //get table name
      String tableName = regionPath.getParent().getName();
      int totalBlkCount = 0;

      // ignore null
      FileStatus[] cfList = fs.listStatus(regionPath, new FSUtils.FamilyDirFilter(fs));
      if (null == cfList) {
        return;
      }

      // for each cf, get all the blocks information
      for (FileStatus cfStatus : cfList) {
        if (!cfStatus.isDir()) {
          // skip because this is not a CF directory
          continue;
        }

        FileStatus[] storeFileLists = fs.listStatus(cfStatus.getPath());
        if (null == storeFileLists) {
          continue;
        }

        for (FileStatus storeFile : storeFileLists) {
          BlockLocation[] blkLocations =
            fs.getFileBlockLocations(storeFile, 0, storeFile.getLen());
          if (null == blkLocations) {
            continue;
          }

          totalBlkCount += blkLocations.length;
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
      }

      if (regionToBestLocalityRSMapping != null) {
        int largestBlkCount = 0;
        String hostToRun = null;
        for (Map.Entry<String, AtomicInteger> entry : blockCountMap.entrySet()) {
          String host = entry.getKey();

          int tmp = entry.getValue().get();
          if (tmp > largestBlkCount) {
            largestBlkCount = tmp;
            hostToRun = host;
          }
        }

        // empty regions could make this null
        if (null == hostToRun) {
          return;
        }

        if (hostToRun.endsWith(".")) {
          hostToRun = hostToRun.substring(0, hostToRun.length()-1);
        }
        String name = tableName + ":" + regionPath.getName();
        synchronized (regionToBestLocalityRSMapping) {
          regionToBestLocalityRSMapping.put(name,  hostToRun);
        }
      }

      if (regionDegreeLocalityMapping != null && totalBlkCount > 0) {
        Map<String, Float> hostLocalityMap = new HashMap<String, Float>();
        for (Map.Entry<String, AtomicInteger> entry : blockCountMap.entrySet()) {
          String host = entry.getKey();
          if (host.endsWith(".")) {
            host = host.substring(0, host.length() - 1);
          }
          // Locality is fraction of blocks local to this host.
          float locality = ((float)entry.getValue().get()) / totalBlkCount;
          hostLocalityMap.put(host, locality);
        }
        // Put the locality map into the result map, keyed by the encoded name
        // of the region.
        regionDegreeLocalityMapping.put(regionPath.getName(), hostLocalityMap);
      }
    } catch (IOException e) {
      LOG.warn("Problem scanning file system", e);
    } catch (RuntimeException e) {
      LOG.warn("Problem scanning file system", e);
    }
  }
}
