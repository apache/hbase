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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcludeDatanodeManager implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(ExcludeDatanodeManager.class);

  private static final String WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY =
    "hbase.regionserver.async.wal.max.exclude.datanode.count";
  private static final int DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT = 3;

  private static final String WAL_EXCLUDE_DATANODE_TTL_KEY =
    "hbase.regionserver.async.wal.exclude.datanode.info.ttl.hour";
  private static final int DEFAULT_WAL_EXCLUDE_DATANODE_TTL = 6; // 6 hours

  private Cache<DatanodeInfo, Long> excludeDNsCache;
  private final int maxExcludeDNCount;
  private final Configuration conf;
  private final Map<String, StreamSlowMonitor> streamSlowMonitors =
    new ConcurrentHashMap<>(1);
  private ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();

  public ExcludeDatanodeManager(Configuration conf) {
    this.conf = conf;
    this.maxExcludeDNCount = conf.getInt(WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY,
      DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT);
    this.excludeDNsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(this.conf.getLong(WAL_EXCLUDE_DATANODE_TTL_KEY,
        DEFAULT_WAL_EXCLUDE_DATANODE_TTL),
        TimeUnit.HOURS)
      .maximumSize(this.maxExcludeDNCount)
      .concurrencyLevel(10)
      .build();
  }

  public Map<DatanodeInfo, Long> getExcludeDNs() {
    cacheLock.readLock().lock();
    try {
      return Collections.unmodifiableMap(excludeDNsCache.asMap());
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public boolean addExcludeDN(DatanodeInfo datanodeInfo, String cause) {
    cacheLock.readLock().lock();
    try {
      boolean alreadyMarkedSlow = getExcludeDNs().containsKey(datanodeInfo);
      if (excludeDNsCache.size() < maxExcludeDNCount) {
        if (!alreadyMarkedSlow) {
          excludeDNsCache.put(datanodeInfo, System.currentTimeMillis());
          LOG.info(
            "Added datanode: {} to exclude cache by [{}] success, current excludeDNsCache size={}",
            datanodeInfo, cause, excludeDNsCache.size());
          return true;
        }
      } else {
        LOG.warn("Try add datanode {} to exclude cache by [{}] failed, up to max exclude limit {}, "
            + "current exclude DNs are {}", datanodeInfo, cause, excludeDNsCache.size(),
          getExcludeDNs().keySet());
      }
      return false;
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void invalidAllExcludeDNs() {
    cacheLock.readLock().lock();
    try {
      excludeDNsCache.invalidateAll();
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public StreamSlowMonitor getStreamSlowMonitor(String name) {
    String key = name == null || name.isEmpty() ? "defaultMonitorName" : name;
    return streamSlowMonitors
      .computeIfAbsent(name, k -> new StreamSlowMonitor(conf, key, this));
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    for (StreamSlowMonitor monitor : streamSlowMonitors.values()) {
      monitor.onConfigurationChange(conf);
    }
    cacheLock.writeLock().lock();
    try {
      this.excludeDNsCache.invalidateAll();
      this.excludeDNsCache = CacheBuilder.newBuilder()
        .expireAfterWrite(this.conf.getLong(WAL_EXCLUDE_DATANODE_TTL_KEY,
          DEFAULT_WAL_EXCLUDE_DATANODE_TTL), TimeUnit.HOURS)
        .maximumSize(this.conf.getInt(WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY,
          DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT))
        .concurrencyLevel(10)
        .build();
    } finally {
      cacheLock.writeLock().unlock();
    }
  }
}
