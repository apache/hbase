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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This will find where data for a region is located in HDFS. It ranks
 * {@link ServerName}'s by the size of the store files they are holding for a
 * given region.
 *
 */
@InterfaceAudience.Private
class RegionLocationFinder {
  private static final Logger LOG = LoggerFactory.getLogger(RegionLocationFinder.class);
  private static final long CACHE_TIME = 240 * 60 * 1000;
  private static final HDFSBlocksDistribution EMPTY_BLOCK_DISTRIBUTION = new HDFSBlocksDistribution();
  private Configuration conf;
  private volatile ClusterMetrics status;
  private MasterServices services;
  private final ListeningExecutorService executor;
  // Do not scheduleFullRefresh at master startup
  private long lastFullRefresh = EnvironmentEdgeManager.currentTime();

  private CacheLoader<RegionInfo, HDFSBlocksDistribution> loader =
      new CacheLoader<RegionInfo, HDFSBlocksDistribution>() {

    @Override
    public ListenableFuture<HDFSBlocksDistribution> reload(final RegionInfo hri,
        HDFSBlocksDistribution oldValue) throws Exception {
      return executor.submit(new Callable<HDFSBlocksDistribution>() {
        @Override
        public HDFSBlocksDistribution call() throws Exception {
          return internalGetTopBlockLocation(hri);
        }
      });
    }

    @Override
    public HDFSBlocksDistribution load(RegionInfo key) throws Exception {
      return internalGetTopBlockLocation(key);
    }
  };

  // The cache for where regions are located.
  private LoadingCache<RegionInfo, HDFSBlocksDistribution> cache = null;

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
   * @return A new Cache.
   */
  private LoadingCache<RegionInfo, HDFSBlocksDistribution> createCache() {
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

  public void setClusterMetrics(ClusterMetrics status) {
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

    final AssignmentManager am = services.getAssignmentManager();
    if (am == null) {
      return false;
    }

    // TODO: Should this refresh all the regions or only the ones assigned?
    boolean includesUserTables = false;
    for (final RegionInfo hri : am.getAssignedRegions()) {
      cache.refresh(hri);
      includesUserTables = includesUserTables || !hri.getTable().isSystemTable();
    }
    return includesUserTables;
  }

  protected List<ServerName> getTopBlockLocations(RegionInfo region) {
    List<String> topHosts = getBlockDistribution(region).getTopHosts();
    return mapHostNameToServerName(topHosts);
  }

  /**
   * Returns an ordered list of hosts which have better locality for this region
   * than the current host.
   */
  protected List<ServerName> getTopBlockLocations(RegionInfo region, String currentHost) {
    HDFSBlocksDistribution blocksDistribution = getBlockDistribution(region);
    List<String> topHosts = new ArrayList<>();
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
  protected HDFSBlocksDistribution internalGetTopBlockLocation(RegionInfo region) {
    try {
      TableDescriptor tableDescriptor = getTableDescriptor(region.getTable());
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
   * return TableDescriptor for a given tableName
   *
   * @param tableName the table name
   * @return TableDescriptor
   * @throws IOException
   */
  protected TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    TableDescriptor tableDescriptor = null;
    try {
      if (this.services != null && this.services.getTableDescriptors() != null) {
        tableDescriptor = this.services.getTableDescriptors().get(tableName);
      }
    } catch (FileNotFoundException fnfe) {
      LOG.debug("tableName={}", tableName, fnfe);
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

    List<ServerName> topServerNames = new ArrayList<>();
    Collection<ServerName> regionServers = status.getLiveServerMetrics().keySet();

    // create a mapping from hostname to ServerName for fast lookup
    HashMap<String, List<ServerName>> hostToServerName = new HashMap<>();
    for (ServerName sn : regionServers) {
      String host = sn.getHostname();
      if (!hostToServerName.containsKey(host)) {
        hostToServerName.put(host, new ArrayList<>());
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

  public HDFSBlocksDistribution getBlockDistribution(RegionInfo hri) {
    HDFSBlocksDistribution blockDistbn = null;
    try {
      if (cache.asMap().containsKey(hri)) {
        blockDistbn = cache.get(hri);
        return blockDistbn;
      } else {
        LOG.trace("HDFSBlocksDistribution not found in cache for {}", hri.getRegionNameAsString());
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
      RegionInfo hri) {
    try {
      return loader.reload(hri, EMPTY_BLOCK_DISTRIBUTION);
    } catch (Exception e) {
      return Futures.immediateFuture(EMPTY_BLOCK_DISTRIBUTION);
    }
  }

  public void refreshAndWait(Collection<RegionInfo> hris) {
    ArrayList<ListenableFuture<HDFSBlocksDistribution>> regionLocationFutures = new ArrayList<>(hris.size());
    for (RegionInfo hregionInfo : hris) {
      regionLocationFutures.add(asyncGetBlockDistribution(hregionInfo));
    }
    int index = 0;
    for (RegionInfo hregionInfo : hris) {
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
  LoadingCache<RegionInfo, HDFSBlocksDistribution> getCache() {
    return cache;
  }
}
