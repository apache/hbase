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
package org.apache.hadoop.hbase.procedure2.store.region;

import static org.apache.hadoop.hbase.HConstants.HREGION_OLDLOGDIR_NAME;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As long as there is no RegionServerServices for the procedure store region, we need implement log
 * roller logic by our own.
 * <p/>
 * We can reuse most of the code for normal wal roller, the only difference is that there is only
 * one region, so in {@link #scheduleFlush(String)} method we can just schedule flush for the
 * procedure store region.
 */
@InterfaceAudience.Private
final class RegionProcedureStoreWALRoller extends AbstractWALRoller<Abortable> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionProcedureStoreWALRoller.class);

  static final String ROLL_PERIOD_MS_KEY = "hbase.procedure.store.region.walroll.period.ms";

  private static final long DEFAULT_ROLL_PERIOD_MS = TimeUnit.MINUTES.toMillis(15);

  private volatile RegionFlusherAndCompactor flusherAndCompactor;

  private final FileSystem fs;

  private final Path walArchiveDir;

  private final Path globalWALArchiveDir;

  private RegionProcedureStoreWALRoller(Configuration conf, Abortable abortable, FileSystem fs,
    Path walRootDir, Path globalWALRootDir) {
    super("RegionProcedureStoreWALRoller", conf, abortable);
    this.fs = fs;
    this.walArchiveDir = new Path(walRootDir, HREGION_OLDLOGDIR_NAME);
    this.globalWALArchiveDir = new Path(globalWALRootDir, HREGION_OLDLOGDIR_NAME);
  }

  @Override
  protected void afterRoll(WAL wal) {
    // move the archived WAL files to the global archive path
    try {
      if (!fs.exists(globalWALArchiveDir) && !fs.mkdirs(globalWALArchiveDir)) {
        LOG.warn("Failed to create global archive dir {}", globalWALArchiveDir);
        return;
      }
      FileStatus[] archivedWALFiles = fs.listStatus(walArchiveDir);
      if (archivedWALFiles == null) {
        return;
      }
      for (FileStatus status : archivedWALFiles) {
        Path file = status.getPath();
        Path newFile = new Path(globalWALArchiveDir,
          file.getName() + MasterProcedureUtil.ARCHIVED_PROC_WAL_SUFFIX);
        if (fs.rename(file, newFile)) {
          LOG.info("Successfully moved {} to {}", file, newFile);
        } else {
          LOG.warn("Failed to move archived wal from {} to global place {}", file, newFile);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to move archived wals from {} to global dir {}", walArchiveDir,
        globalWALArchiveDir, e);
    }
  }

  @Override
  protected void scheduleFlush(String encodedRegionName) {
    RegionFlusherAndCompactor flusher = this.flusherAndCompactor;
    if (flusher != null) {
      flusher.requestFlush();
    }
  }

  void setFlusherAndCompactor(RegionFlusherAndCompactor flusherAndCompactor) {
    this.flusherAndCompactor = flusherAndCompactor;
  }

  static RegionProcedureStoreWALRoller create(Configuration conf, Abortable abortable,
    FileSystem fs, Path walRootDir, Path globalWALRootDir) {
    // we can not run with wal disabled, so force set it to true.
    conf.setBoolean(WALFactory.WAL_ENABLED, true);
    // we do not need this feature, so force disable it.
    conf.setBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR, false);
    conf.setLong(WAL_ROLL_PERIOD_KEY, conf.getLong(ROLL_PERIOD_MS_KEY, DEFAULT_ROLL_PERIOD_MS));
    long flushSize = conf.getLong(RegionFlusherAndCompactor.FLUSH_SIZE_KEY,
      RegionFlusherAndCompactor.DEFAULT_FLUSH_SIZE);
    // make the roll size the same with the flush size, as we only have one region here
    conf.setLong(WALUtil.WAL_BLOCK_SIZE, flushSize * 2);
    conf.setFloat(AbstractFSWAL.WAL_ROLL_MULTIPLIER, 0.5f);
    return new RegionProcedureStoreWALRoller(conf, abortable, fs, walRootDir, globalWALRootDir);
  }
}