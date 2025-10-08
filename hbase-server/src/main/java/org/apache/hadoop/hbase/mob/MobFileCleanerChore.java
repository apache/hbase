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
package org.apache.hadoop.hbase.mob;

import static org.apache.hadoop.hbase.mob.MobConstants.DEFAULT_MOB_FILE_CLEANER_CHORE_TIME_OUT;
import static org.apache.hadoop.hbase.mob.MobConstants.MOB_FILE_CLEANER_CHORE_TIME_OUT;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The class MobFileCleanerChore for running cleaner regularly to remove the expired and obsolete
 * (files which have no active references to) mob files.
 */
@InterfaceAudience.Private
public class MobFileCleanerChore extends ScheduledChore implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCleanerChore.class);

  private final HMaster master;
  private final ExpiredMobFileCleaner cleaner;
  private final ThreadPoolExecutor executor;
  private final int cleanerFutureTimeout;
  private int threadCount;

  public MobFileCleanerChore(HMaster master) {
    super(master.getServerName() + "-MobFileCleanerChore", master,
      master.getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD),
      master.getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD),
      TimeUnit.SECONDS);
    this.master = master;
    cleaner = new ExpiredMobFileCleaner();
    cleaner.setConf(master.getConfiguration());
    threadCount = master.getConfiguration().getInt(MobConstants.MOB_CLEANER_THREAD_COUNT,
      MobConstants.DEFAULT_MOB_CLEANER_THREAD_COUNT);
    if (threadCount <= 1) {
      threadCount = 1;
    }

    ThreadFactory threadFactory =
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("mobfile-cleaner-pool-%d").build();

    executor = new ThreadPoolExecutor(threadCount, threadCount, 60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(), threadFactory);

    checkObsoleteConfigurations();
    cleanerFutureTimeout = master.getConfiguration().getInt(MOB_FILE_CLEANER_CHORE_TIME_OUT,
      DEFAULT_MOB_FILE_CLEANER_CHORE_TIME_OUT);
  }

  private void checkObsoleteConfigurations() {
    Configuration conf = master.getConfiguration();

    if (conf.get("hbase.mob.compaction.mergeable.threshold") != null) {
      LOG.warn("'hbase.mob.compaction.mergeable.threshold' is obsolete and not used anymore.");
    }
    if (conf.get("hbase.mob.delfile.max.count") != null) {
      LOG.warn("'hbase.mob.delfile.max.count' is obsolete and not used anymore.");
    }
    if (conf.get("hbase.mob.compaction.threads.max") != null) {
      LOG.warn("'hbase.mob.compaction.threads.max' is obsolete and not used anymore.");
    }
    if (conf.get("hbase.mob.compaction.batch.size") != null) {
      LOG.warn("'hbase.mob.compaction.batch.size' is obsolete and not used anymore.");
    }
  }

  @Override
  protected void chore() {
    TableDescriptors htds = master.getTableDescriptors();

    Map<String, TableDescriptor> map = null;
    try {
      map = htds.getAll();
    } catch (IOException e) {
      LOG.error("MobFileCleanerChore failed", e);
      return;
    }
    List<Future<?>> futureList = new ArrayList<>(map.size());
    for (TableDescriptor htd : map.values()) {
      Future<?> future = executor.submit(() -> handleOneTable(htd));
      futureList.add(future);
    }

    for (Future<?> future : futureList) {
      try {
        future.get(cleanerFutureTimeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("MobFileCleanerChore interrupted while waiting for futures", e);
        Thread.currentThread().interrupt();
        cancelAllFutures(futureList);
        break;
      } catch (ExecutionException e) {
        LOG.error("Exception during execution of MobFileCleanerChore task", e);
      } catch (TimeoutException e) {
        LOG.error("MobFileCleanerChore timed out waiting for a task to complete", e);
      }
    }
  }

  private void cancelAllFutures(List<Future<?>> futureList) {
    for (Future<?> f : futureList) {
      if (!f.isDone()) {
        f.cancel(true); // interrupt running tasks
      }
    }
    LOG.info("Cancelled all pending mob file cleaner tasks");
  }

  private void handleOneTable(TableDescriptor htd) {
    for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
      if (hcd.isMobEnabled() && hcd.getMinVersions() == 0) {
        try {
          cleaner.cleanExpiredMobFiles(htd, hcd);
        } catch (IOException e) {
          LOG.error("Failed to clean the expired mob files table={} family={}",
            htd.getTableName().getNameAsString(), hcd.getNameAsString(), e);
        }
      }
    }
    try {
      // Now clean obsolete files for a table
      LOG.info("Cleaning obsolete MOB files from table={}", htd.getTableName());
      try (final Admin admin = master.getConnection().getAdmin()) {
        MobFileCleanupUtil.cleanupObsoleteMobFiles(master.getConfiguration(), htd.getTableName(),
          admin);
      }
      LOG.info("Cleaning obsolete MOB files finished for table={}", htd.getTableName());
    } catch (IOException e) {
      LOG.error("Failed to clean the obsolete mob files for table={}", htd.getTableName(), e);
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    int newThreadCount = conf.getInt(MobConstants.MOB_CLEANER_THREAD_COUNT,
      MobConstants.DEFAULT_MOB_CLEANER_THREAD_COUNT);
    if (newThreadCount < 1) {
      return; // invalid value , skip the config change
    }

    if (newThreadCount != threadCount) {
      resizeThreadPool(newThreadCount, newThreadCount);
      threadCount = newThreadCount;
    }
  }

  private void resizeThreadPool(int newCoreSize, int newMaxSize) {
    int currentCoreSize = executor.getCorePoolSize();
    if (newCoreSize > currentCoreSize) {
      // Increasing the pool size: Set max first, then core
      executor.setMaximumPoolSize(newMaxSize);
      executor.setCorePoolSize(newCoreSize);
    } else {
      // Decreasing the pool size: Set core first, then max
      executor.setCorePoolSize(newCoreSize);
      executor.setMaximumPoolSize(newMaxSize);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public ThreadPoolExecutor getExecutor() {
    return executor;
  }
}
