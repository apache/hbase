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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Computes size of each region for given table and given column families.
 * The value is used by MapReduce for better scheduling.
 * */
@InterfaceAudience.Private
public class RegionSizeCalculator {

  private static final Logger LOG = LoggerFactory.getLogger(RegionSizeCalculator.class);

  /**
   * Maps each region to its size in bytes.
   * */
  private final Map<byte[], Long> sizeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  static final String ENABLE_REGIONSIZECALCULATOR = "hbase.regionsizecalculator.enable";
  private static final long MEGABYTE = 1024L * 1024L;

  /**
   * Computes size of each region for table and given column families.
   * */
  public RegionSizeCalculator(RegionLocator regionLocator, Admin admin) throws IOException {
    init(regionLocator, admin);
  }

  private void init(RegionLocator regionLocator, Admin admin)
      throws IOException {
    if (!enabled(admin.getConfiguration())) {
      LOG.info("Region size calculation disabled.");
      return;
    }

    if (regionLocator.getName().isSystemTable()) {
      LOG.info("Region size calculation disabled for system tables.");
      return;
    }

    LOG.info("Calculating region sizes for table \"" + regionLocator.getName() + "\".");

    // Get the servers which host regions of the table
    Set<ServerName> tableServers = getRegionServersOfTable(regionLocator);

    for (ServerName tableServerName : tableServers) {
      for (RegionMetrics regionLoad : admin.getRegionMetrics(
        tableServerName,regionLocator.getName())) {

        byte[] regionId = regionLoad.getRegionName();
        long regionSizeBytes
          = ((long) regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE)) * MEGABYTE;

        sizeMap.put(regionId, regionSizeBytes);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Region " + regionLoad.getNameAsString() + " has size " + regionSizeBytes);
        }
      }
    }
    LOG.debug("Region sizes calculated");
  }

  private Set<ServerName> getRegionServersOfTable(RegionLocator regionLocator)
      throws IOException {

    Set<ServerName> tableServers = Sets.newHashSet();
    for (HRegionLocation regionLocation : regionLocator.getAllRegionLocations()) {
      tableServers.add(regionLocation.getServerName());
    }
    return tableServers;
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
