/*
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
package org.apache.hadoop.hbase.io.asyncfs.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;

/**
 * The class to manage the excluded datanodes of the WALs on the regionserver.
 */
@InterfaceAudience.Private
public class ExcludeDatanodeManager implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(ExcludeDatanodeManager.class);

  /**
   * Configure for the max count the excluded datanodes.
   */
  public static final String WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY =
    "hbase.regionserver.async.wal.max.exclude.datanode.count";
  public static final int DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT = 3;

  /**
   * Configure for the TTL time of the datanodes excluded
   */
  public static final String WAL_EXCLUDE_DATANODE_TTL_KEY =
    "hbase.regionserver.async.wal.exclude.datanode.info.ttl.hour";
  public static final int DEFAULT_WAL_EXCLUDE_DATANODE_TTL = 6; // 6 hours

  private volatile Cache<DatanodeInfo, Pair<String, Long>> excludeDNsCache;
  private final int maxExcludeDNCount;
  private final Configuration conf;
  // This is a map of providerId->StreamSlowMonitor
  private final Map<String, StreamSlowMonitor> streamSlowMonitors = new ConcurrentHashMap<>(1);

  public ExcludeDatanodeManager(Configuration conf) {
    this.conf = conf;
    this.maxExcludeDNCount = conf.getInt(WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY,
      DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT);
    this.excludeDNsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(
        this.conf.getLong(WAL_EXCLUDE_DATANODE_TTL_KEY, DEFAULT_WAL_EXCLUDE_DATANODE_TTL),
        TimeUnit.HOURS)
      .maximumSize(this.maxExcludeDNCount).build();
  }

  /**
   * Try to add a datanode to the regionserver excluding cache
   * @param datanodeInfo the datanode to be added to the excluded cache
   * @param cause        the cause that the datanode is hope to be excluded
   * @return True if the datanode is added to the regionserver excluding cache, false otherwise
   */
  public boolean tryAddExcludeDN(DatanodeInfo datanodeInfo, String cause) {
    boolean alreadyMarkedSlow = getExcludeDNs().containsKey(datanodeInfo);
    if (!alreadyMarkedSlow) {
      excludeDNsCache.put(datanodeInfo, new Pair<>(cause, EnvironmentEdgeManager.currentTime()));
      LOG.info(
        "Added datanode: {} to exclude cache by [{}] success, current excludeDNsCache size={}",
        datanodeInfo, cause, excludeDNsCache.size());
      return true;
    }
    LOG.debug(
      "Try add datanode {} to exclude cache by [{}] failed, " + "current exclude DNs are {}",
      datanodeInfo, cause, getExcludeDNs().keySet());
    return false;
  }

  public StreamSlowMonitor getStreamSlowMonitor(String name) {
    String key = name == null || name.isEmpty() ? "defaultMonitorName" : name;
    return streamSlowMonitors.computeIfAbsent(key, k -> new StreamSlowMonitor(conf, key, this));
  }

  /**
   * Enumerates the reasons for excluding a datanode from certain operations. Each enum constant
   * represents a specific cause leading to exclusion.
   */
  public enum ExcludeCause {
    CONNECT_ERROR("connect error"),
    SLOW_PACKET_ACK("slow packet ack");

    private final String cause;

    ExcludeCause(String cause) {
      this.cause = cause;
    }

    public String getCause() {
      return cause;
    }

    @Override
    public String toString() {
      return cause;
    }
  }

  public Map<DatanodeInfo, Pair<String, Long>> getExcludeDNs() {
    return excludeDNsCache.asMap();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    for (StreamSlowMonitor monitor : streamSlowMonitors.values()) {
      monitor.onConfigurationChange(conf);
    }
    this.excludeDNsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(
        this.conf.getLong(WAL_EXCLUDE_DATANODE_TTL_KEY, DEFAULT_WAL_EXCLUDE_DATANODE_TTL),
        TimeUnit.HOURS)
      .maximumSize(this.conf.getInt(WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY,
        DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT))
      .build();
  }
}
