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
package org.apache.hadoop.hbase.master.region;

import static org.apache.hadoop.hbase.HConstants.HREGION_OLDLOGDIR_NAME;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As long as there is no RegionServerServices for a master local region, we need implement log
 * roller logic by our own.
 * <p/>
 * We can reuse most of the code for normal wal roller, the only difference is that there is only
 * one region, so in {@link #scheduleFlush(String, List)} method we can just schedule flush
 * for the master local region.
 */
@InterfaceAudience.Private
public final class MasterRegionWALRoller extends AbstractWALRoller<Abortable> {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegionWALRoller.class);

  private volatile MasterRegionFlusherAndCompactor flusherAndCompactor;

  private final FileSystem fs;

  private final Path walArchiveDir;

  private final Path globalWALArchiveDir;

  private final String archivedWALSuffix;

  private MasterRegionWALRoller(String name, Configuration conf, Abortable abortable, FileSystem fs,
    Path walRootDir, Path globalWALRootDir, String archivedWALSuffix) {
    super(name, conf, abortable);
    this.fs = fs;
    this.walArchiveDir = new Path(walRootDir, HREGION_OLDLOGDIR_NAME);
    this.globalWALArchiveDir = new Path(globalWALRootDir, HREGION_OLDLOGDIR_NAME);
    this.archivedWALSuffix = archivedWALSuffix;
  }

  @Override
  protected void afterWALArchive(Path oldPath, Path newPath) {
    // move the archived WAL files to the global archive path
    // here we do not use the newPath directly, so that even if we fail to move some of the
    // newPaths, we are still safe because every time we will get all the files under the archive
    // directory.
    try {
      MasterRegionUtils.moveFilesUnderDir(fs, walArchiveDir, globalWALArchiveDir,
        archivedWALSuffix);
    } catch (IOException e) {
      LOG.warn("Failed to move archived wals from {} to global dir {}", walArchiveDir,
        globalWALArchiveDir, e);
    }
  }

  @Override
  protected void scheduleFlush(String encodedRegionName, List<byte[]> families) {
    MasterRegionFlusherAndCompactor flusher = this.flusherAndCompactor;
    if (flusher != null) {
      flusher.requestFlush();
    }
  }

  void setFlusherAndCompactor(MasterRegionFlusherAndCompactor flusherAndCompactor) {
    this.flusherAndCompactor = flusherAndCompactor;
  }

  static MasterRegionWALRoller create(String name, Configuration conf, Abortable abortable,
    FileSystem fs, Path walRootDir, Path globalWALRootDir, String archivedWALSuffix,
    long rollPeriodMs, long flushSize) {
    // we can not run with wal disabled, so force set it to true.
    conf.setBoolean(WALFactory.WAL_ENABLED, true);
    // we do not need this feature, so force disable it.
    conf.setBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR, false);
    conf.setLong(WAL_ROLL_PERIOD_KEY, rollPeriodMs);
    // make the roll size the same with the flush size, as we only have one region here
    conf.setLong(WALUtil.WAL_BLOCK_SIZE, flushSize * 2);
    conf.setFloat(AbstractFSWAL.WAL_ROLL_MULTIPLIER, 0.5f);
    return new MasterRegionWALRoller(name, conf, abortable, fs, walRootDir, globalWALRootDir,
      archivedWALSuffix);
  }

}
