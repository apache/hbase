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
package org.apache.hadoop.hbase.master.balancer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

/**
 * This will find where data for a region is located in HDFS. It ranks
 * {@link ServerName}'s by the size of the store files they are holding for a
 * given region.
 *
 */
class RegionLocationFinder {

  private static Log LOG = LogFactory.getLog(RegionLocationFinder.class);

  private Configuration conf;
  private volatile ClusterStatus status;
  private MasterServices services;

  private CacheLoader<HRegionInfo, HDFSBlocksDistribution> loader =
           new CacheLoader<HRegionInfo, HDFSBlocksDistribution>() {

    @Override
    public HDFSBlocksDistribution load(HRegionInfo key) throws Exception {
      return internalGetTopBlockLocation(key);
    }
  };

  // The cache for where regions are located.
  private LoadingCache<HRegionInfo, HDFSBlocksDistribution> cache = null;

  /**
   * Create a cache for region to list of servers
   * @param mins Number of mins to cache
   * @return A new Cache.
   */
  private LoadingCache<HRegionInfo, HDFSBlocksDistribution> createCache(int mins) {
    return CacheBuilder.newBuilder().expireAfterAccess(mins, TimeUnit.MINUTES).build(loader);
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    cache = createCache(conf.getInt("hbase.master.balancer.regionLocationCacheTime", 30));
  }

  public void setServices(MasterServices services) {
    this.services = services;
  }

  public void setClusterStatus(ClusterStatus status) {
    this.status = status;
  }

  protected List<ServerName> getTopBlockLocations(HRegionInfo region) {
    HDFSBlocksDistribution blocksDistribution = getBlockDistribution(region);
    List<String> topHosts = blocksDistribution.getTopHosts();
    return mapHostNameToServerName(topHosts);
  }

  /**
   * Returns an ordered list of hosts that are hosting the blocks for this
   * region. The weight of each host is the sum of the block lengths of all
   * files on that host, so the first host in the list is the server which holds
   * the most bytes of the given region's HFiles.
   *
   * @param region region
   * @return ordered list of hosts holding blocks of the specified region
   */
  protected HDFSBlocksDistribution internalGetTopBlockLocation(HRegionInfo region) {
    try {
      HTableDescriptor tableDescriptor = getTableDescriptor(region.getTable());
      if (tableDescriptor != null) {
        HDFSBlocksDistribution blocksDistribution =
            HRegion.computeHDFSBlocksDistribution(getConf(), tableDescriptor, region);
        return blocksDistribution;
      }
    } catch (IOException ioe) {
      LOG.warn("IOException during HDFSBlocksDistribution computation. for " + "region = "
          + region.getEncodedName(), ioe);
    }

    return new HDFSBlocksDistribution();
  }

  /**
   * return HTableDescriptor for a given tableName
   *
   * @param tableName the table name
   * @return HTableDescriptor
   * @throws IOException
   */
  protected HTableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    HTableDescriptor tableDescriptor = null;
    try {
      if (this.services != null) {
        tableDescriptor = this.services.getTableDescriptors().get(tableName);
      }
    } catch (FileNotFoundException fnfe) {
      LOG.debug("FileNotFoundException during getTableDescriptors." + " Current table name = "
          + tableName, fnfe);
    }

    return tableDescriptor;
  }

  /**
   * Map hostname to ServerName, The output ServerName list will have the same
   * order as input hosts.
   *
   * @param hosts the list of hosts
   * @return ServerName list
   */
  protected List<ServerName> mapHostNameToServerName(List<String> hosts) {
    if (hosts == null || status == null) {
      if (hosts == null) {
        LOG.warn("RegionLocationFinder top hosts is null");
      }
      return Lists.newArrayList();
    }

    List<ServerName> topServerNames = new ArrayList<ServerName>();
    Collection<ServerName> regionServers = status.getServers();

    // create a mapping from hostname to ServerName for fast lookup
    HashMap<String, ServerName> hostToServerName = new HashMap<String, ServerName>();
    for (ServerName sn : regionServers) {
      hostToServerName.put(sn.getHostname(), sn);
    }

    for (String host : hosts) {
      ServerName sn = hostToServerName.get(host);
      // it is possible that HDFS is up ( thus host is valid ),
      // but RS is down ( thus sn is null )
      if (sn != null) {
        topServerNames.add(sn);
      }
    }
    return topServerNames;
  }

  public HDFSBlocksDistribution getBlockDistribution(HRegionInfo hri) {
    HDFSBlocksDistribution blockDistbn = null;
    try {
      if (cache.asMap().containsKey(hri)) {
        blockDistbn = cache.get(hri);
        return blockDistbn;
      } else {
        LOG.debug("HDFSBlocksDistribution not found in cache for region "
            + hri.getRegionNameAsString());
        blockDistbn = internalGetTopBlockLocation(hri);
        cache.put(hri, blockDistbn);
        return blockDistbn;
      }
    } catch (ExecutionException e) {
      LOG.warn("Error while fetching cache entry ", e);
      blockDistbn = internalGetTopBlockLocation(hri);
      cache.put(hri, blockDistbn);
      return blockDistbn;
    }
  }
}
