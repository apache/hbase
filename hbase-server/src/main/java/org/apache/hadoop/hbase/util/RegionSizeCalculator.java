/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Computes size of each region for given table and given column families.
 * The value is used by MapReduce for better scheduling.
 * */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class RegionSizeCalculator {

  private final Log LOG = LogFactory.getLog(RegionSizeCalculator.class);

  /**
   * Maps each region to its size in bytes.
   * */
  private final Map<byte[], Long> sizeMap = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  static final String ENABLE_REGIONSIZECALCULATOR = "hbase.regionsizecalculator.enable";

  /**
   * Computes size of each region for table and given column families.
   * */
  public RegionSizeCalculator(HTable table) throws IOException {
    this(table, new HBaseAdmin(table.getConfiguration()));
  }

  /** ctor for unit testing */
  RegionSizeCalculator (HTable table, HBaseAdmin admin) throws IOException {

    try {
      if (!enabled(table.getConfiguration())) {
        LOG.info("Region size calculation disabled.");
        return;
      }

      LOG.info("Calculating region sizes for table \"" + new String(table.getTableName()) + "\".");

      //get regions for table
      Set<HRegionInfo> tableRegionInfos = table.getRegionLocations().keySet();
      Set<byte[]> tableRegions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      for (HRegionInfo regionInfo : tableRegionInfos) {
        tableRegions.add(regionInfo.getRegionName());
      }

      ClusterStatus clusterStatus = admin.getClusterStatus();
      Collection<ServerName> servers = clusterStatus.getServers();
      final long megaByte = 1024L * 1024L;

      //iterate all cluster regions, filter regions from our table and compute their size
      for (ServerName serverName: servers) {
        ServerLoad serverLoad = clusterStatus.getLoad(serverName);

        for (RegionLoad regionLoad: serverLoad.getRegionsLoad().values()) {
          byte[] regionId = regionLoad.getName();

          if (tableRegions.contains(regionId)) {

            long regionSizeBytes = regionLoad.getStorefileSizeMB() * megaByte;
            sizeMap.put(regionId, regionSizeBytes);

            if (LOG.isDebugEnabled()) {
              LOG.debug("Region " + regionLoad.getNameAsString() + " has size " + regionSizeBytes);
            }
          }
        }
      }
      LOG.debug("Region sizes calculated");

    } finally {
      admin.close();
    }

  }

  boolean enabled(Configuration configuration) {
    return configuration.getBoolean(ENABLE_REGIONSIZECALCULATOR, true);
  }

  /**
   * Returns size of given region in bytes. Returns 0 if region was not found.
   * */
  public long getRegionSize(byte[] regionId) {
    Long size = sizeMap.get(regionId);
    if (size == null) {
      LOG.debug("Unknown region:" + Arrays.toString(regionId));
      return 0;
    } else {
      return size;
    }
  }

  public Map<byte[], Long> getRegionSizeMap() {
    return Collections.unmodifiableMap(sizeMap);
  }
}
