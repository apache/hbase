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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This will find where data for a region is located in HDFS. It ranks
 * {@link ServerName}'s by the size of the store files they are holding for a
 * given region.
 *
 */
@InterfaceAudience.Private
class RegionLocationFinder {
  private static final Log LOG = LogFactory.getLog(RegionLocationFinder.class);
  private static final long CACHE_TIME = 240 * 60 * 1000;
  private static final HDFSBlocksDistribution EMPTY_BLOCK_DISTRIBUTION = new HDFSBlocksDistribution();
  private Configuration conf;
  private volatile ClusterStatus status;
  private MasterServices services;
  private final ListeningExecutorService executor;
  // Do not scheduleFullRefresh at master startup
  private long lastFullRefresh = EnvironmentEdgeManager.currentTime();

  private CacheLoader<HRegionInfo, HDFSBlocksDistribution> loader =
      new CacheLoader<HRegionInfo, HDFSBlocksDistribution>() {

        public ListenableFuture<HDFSBlocksDistribution> reload(final HRegionInfo hri,
               HDFSBlocksDistribution oldValue) throws Exception {
          return executor.submit(new Callable<HDFSBlocksDistribution>() {
            @Override
            public HDFSBlocksDistribution call() throws Exception {
              return internalGetTopBlockLocation(hri);
            }
          });
        }

        @Override
        public HDFSBlocksDistribution load(HRegionInfo key) throws Exception {
          return internalGetTopBlockLocation(key);
        }
      };

  // The cache for where regions are located.
  private LoadingCache<HRegionInfo, HDFSBlocksDistribution> cache = null;

  RegionLocationFinder() {
    this.cache = createCache();
    executor = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(
            5,
            new ThreadFactoryBuilder().
                setDaemon(true)
                .setNameFormat("region-location-%d")
                .build()));
  }

  /**
   * Create a cache for region to list of servers
   * @param time time to cache the locations
   * @return A new Cache.
   */
  private LoadingCache<HRegionInfo, HDFSBlocksDistribution> createCache() {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(CACHE_TIME, TimeUnit.MILLISECONDS)
        .build(loader);
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public void setServices(MasterServices services) {
    this.services = services;
  }

  public void setClusterStatus(ClusterStatus status) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    this.status = status;
    if (currentTime > lastFullRefresh + (CACHE_TIME / 2)) {
      // Only count the refresh if it includes user tables ( eg more than meta and namespace ).
      lastFullRefresh = scheduleFullRefresh()?currentTime:lastFullRefresh;
    }

  }

  /**
   * Refresh all the region locations.
   *
   * @return true if user created regions got refreshed.
   */
  private boolean scheduleFullRefresh() {
    // Protect from anything being null while starting up.
    if (services == null) {
      return false;
    }
    AssignmentManager am = services.getAssignmentManager();

    if (am == null) {
      return false;
    }
    RegionStates regionStates = am.getRegionStates();
    if (regionStates == null) {
      return false;
    }

    Set<HRegionInfo> regions = regionStates.getRegionAssignments().keySet();
    boolean includesUserTables = false;
    for (final HRegionInfo hri : regions) {
      cache.refresh(hri);
      includesUserTables = includesUserTables || !hri.isSystemTable();
    }
    return includesUserTables;
  }

  protected List<ServerName> getTopBlockLocations(HRegionInfo region) {
    List<String> topHosts = getBlockDistribution(region).getTopHosts();
    return mapHostNameToServerName(topHosts);
  }

  /**
   * Returns an ordered list of hosts which have better locality for this region
   * than the current host.
   */
  protected List<ServerName> getTopBlockLocations(HRegionInfo region, String currentHost) {
    HDFSBlocksDistribution blocksDistribution = getBlockDistribution(region);
    List<String> topHosts = new ArrayList<String>();
    for (String host : blocksDistribution.getTopHosts()) {
      if (host.equals(currentHost)) {
        break;
      }
      topHosts.add(host);
    }
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

    return EMPTY_BLOCK_DISTRIBUTION;
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
      if (this.services != null && this.services.getTableDescriptors() != null) {
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
    HashMap<String, List<ServerName>> hostToServerName = new HashMap<String, List<ServerName>>();
    for (ServerName sn : regionServers) {
      String host = sn.getHostname();
      if (!hostToServerName.containsKey(host)) {
        hostToServerName.put(host, new ArrayList<ServerName>());
      }
      hostToServerName.get(host).add(sn);
    }

    for (String host : hosts) {
      if (!hostToServerName.containsKey(host)) {
        continue;
      }
      for (ServerName sn : hostToServerName.get(host)) {
        // it is possible that HDFS is up ( thus host is valid ),
        // but RS is down ( thus sn is null )
        if (sn != null) {
          topServerNames.add(sn);
        }
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

  private ListenableFuture<HDFSBlocksDistribution> asyncGetBlockDistribution(
      HRegionInfo hri) {
    try {
      return loader.reload(hri, EMPTY_BLOCK_DISTRIBUTION);
    } catch (Exception e) {
      return Futures.immediateFuture(EMPTY_BLOCK_DISTRIBUTION);
    }
  }

  public void refreshAndWait(Collection<HRegionInfo> hris) {
    ArrayList<ListenableFuture<HDFSBlocksDistribution>> regionLocationFutures =
        new ArrayList<ListenableFuture<HDFSBlocksDistribution>>(hris.size());
    for (HRegionInfo hregionInfo : hris) {
      regionLocationFutures.add(asyncGetBlockDistribution(hregionInfo));
    }
    int index = 0;
    for (HRegionInfo hregionInfo : hris) {
      ListenableFuture<HDFSBlocksDistribution> future = regionLocationFutures
          .get(index);
      try {
        cache.put(hregionInfo, future.get());
      } catch (InterruptedException ite) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException ee) {
        LOG.debug(
            "ExecutionException during HDFSBlocksDistribution computation. for region = "
                + hregionInfo.getEncodedName(), ee);
      }
      index++;
    }
  }

  // For test
  LoadingCache<HRegionInfo, HDFSBlocksDistribution> getCache() {
    return cache;
  }
}
