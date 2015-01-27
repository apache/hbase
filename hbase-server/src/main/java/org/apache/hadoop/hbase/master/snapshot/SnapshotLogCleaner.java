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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Implementation of a log cleaner that checks if a log is still used by
 * snapshots of HBase tables.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Evolving
public class SnapshotLogCleaner extends BaseLogCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(SnapshotLogCleaner.class);

  /**
   * Conf key for the frequency to attempt to refresh the cache of hfiles currently used in
   * snapshots (ms)
   */
  static final String HLOG_CACHE_REFRESH_PERIOD_CONF_KEY =
      "hbase.master.hlogcleaner.plugins.snapshot.period";

  /** Refresh cache, by default, every 5 minutes */
  private static final long DEFAULT_HLOG_CACHE_REFRESH_PERIOD = 300000;

  private SnapshotFileCache cache;

  @Override
  public synchronized Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    try {
      return cache.getUnreferencedFiles(files);
    } catch (IOException e) {
      LOG.error("Exception while checking if files were valid, keeping them just in case.", e);
      return Collections.emptyList();
    }
  }

  /**
   * This method should only be called <b>once</b>, as it starts a thread to keep the cache
   * up-to-date.
   * <p>
   * {@inheritDoc}
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      long cacheRefreshPeriod = conf.getLong(
        HLOG_CACHE_REFRESH_PERIOD_CONF_KEY, DEFAULT_HLOG_CACHE_REFRESH_PERIOD);
      final FileSystem fs = FSUtils.getCurrentFileSystem(conf);
      Path rootDir = FSUtils.getRootDir(conf);
      cache = new SnapshotFileCache(fs, rootDir, cacheRefreshPeriod, cacheRefreshPeriod,
          "snapshot-log-cleaner-cache-refresher", new SnapshotFileCache.SnapshotFileInspector() {
            public Collection<String> filesUnderSnapshot(final Path snapshotDir)
                throws IOException {
              return SnapshotReferenceUtil.getHLogNames(fs, snapshotDir);
            }
          });
    } catch (IOException e) {
      LOG.error("Failed to create snapshot log cleaner", e);
    }
  }

  @Override
  public void stop(String why) {
    this.cache.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.cache.isStopped();
  }
}
