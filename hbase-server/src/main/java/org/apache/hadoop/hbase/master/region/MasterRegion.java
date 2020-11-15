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

import static org.apache.hadoop.hbase.HConstants.HREGION_LOGDIR_NAME;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.math.IntMath;

/**
 * A region that stores data in a separated directory, which can be used to store master local data.
 * <p/>
 * FileSystem layout:
 *
 * <pre>
 * hbase
 *   |
 *   --&lt;region dir&gt;
 *       |
 *       --data
 *       |  |
 *       |  --/&lt;ns&gt/&lt;table&gt/&lt;encoded-region-name&gt; <---- The region data
 *       |      |
 *       |      --replay <---- The edits to replay
 *       |
 *       --WALs
 *          |
 *          --&lt;master-server-name&gt; <---- The WAL dir for active master
 *          |
 *          --&lt;master-server-name&gt;-dead <---- The WAL dir for dead master
 * </pre>
 *
 * Notice that, you can use different root file system and WAL file system. Then the above directory
 * will be on two file systems, the root file system will have the data directory while the WAL
 * filesystem will have the WALs directory. The archived HFile will be moved to the global HFile
 * archived directory with the {@link MasterRegionParams#archivedWalSuffix()} suffix. The archived
 * WAL will be moved to the global WAL archived directory with the
 * {@link MasterRegionParams#archivedHFileSuffix()} suffix.
 */
@InterfaceAudience.Private
public final class MasterRegion {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegion.class);

  private static final String REPLAY_EDITS_DIR = "recovered.wals";

  private static final String DEAD_WAL_DIR_SUFFIX = "-dead";

  private static final int REGION_ID = 1;

  private final WALFactory walFactory;

  @VisibleForTesting
  final HRegion region;

  @VisibleForTesting
  final MasterRegionFlusherAndCompactor flusherAndCompactor;

  private MasterRegionWALRoller walRoller;

  private MasterRegion(HRegion region, WALFactory walFactory,
    MasterRegionFlusherAndCompactor flusherAndCompactor, MasterRegionWALRoller walRoller) {
    this.region = region;
    this.walFactory = walFactory;
    this.flusherAndCompactor = flusherAndCompactor;
    this.walRoller = walRoller;
  }

  private void closeRegion(boolean abort) {
    try {
      region.close(abort);
    } catch (IOException e) {
      LOG.warn("Failed to close region", e);
    }
  }

  private void shutdownWAL() {
    try {
      walFactory.shutdown();
    } catch (IOException e) {
      LOG.warn("Failed to shutdown WAL", e);
    }
  }

  public void update(UpdateMasterRegion action) throws IOException {
    action.update(region);
    flusherAndCompactor.onUpdate();
  }

  public Result get(Get get) throws IOException {
    return region.get(get);
  }

  public RegionScanner getScanner(Scan scan) throws IOException {
    return region.getScanner(scan);
  }

  @VisibleForTesting
  public FlushResult flush(boolean force) throws IOException {
    return region.flush(force);
  }

  @VisibleForTesting
  public void requestRollAll() {
    walRoller.requestRollAll();
  }

  @VisibleForTesting
  public void waitUntilWalRollFinished() throws InterruptedException {
    walRoller.waitUntilWalRollFinished();
  }

  public void close(boolean abort) {
    LOG.info("Closing local region {}, isAbort={}", region.getRegionInfo(), abort);
    if (flusherAndCompactor != null) {
      flusherAndCompactor.close();
    }
    // if abort, we shutdown wal first to fail the ongoing updates to the region, and then close the
    // region, otherwise there will be dead lock.
    if (abort) {
      shutdownWAL();
      closeRegion(true);
    } else {
      closeRegion(false);
      shutdownWAL();
    }

    if (walRoller != null) {
      walRoller.close();
    }
  }

  private static WAL createWAL(WALFactory walFactory, MasterRegionWALRoller walRoller,
    String serverName, FileSystem walFs, Path walRootDir, RegionInfo regionInfo)
    throws IOException {
    String logName = AbstractFSWALProvider.getWALDirectoryName(serverName);
    Path walDir = new Path(walRootDir, logName);
    LOG.debug("WALDir={}", walDir);
    if (walFs.exists(walDir)) {
      throw new HBaseIOException(
        "Already created wal directory at " + walDir + " for local region " + regionInfo);
    }
    if (!walFs.mkdirs(walDir)) {
      throw new IOException(
        "Can not create wal directory " + walDir + " for local region " + regionInfo);
    }
    WAL wal = walFactory.getWAL(regionInfo);
    walRoller.addWAL(wal);
    return wal;
  }

  private static HRegion bootstrap(Configuration conf, TableDescriptor td, FileSystem fs,
    Path rootDir, FileSystem walFs, Path walRootDir, WALFactory walFactory,
    MasterRegionWALRoller walRoller, String serverName) throws IOException {
    TableName tn = td.getTableName();
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tn).setRegionId(REGION_ID).build();
    Path tmpTableDir = CommonFSUtils.getTableDir(rootDir,
      TableName.valueOf(tn.getNamespaceAsString(), tn.getQualifierAsString() + "-tmp"));
    if (fs.exists(tmpTableDir) && !fs.delete(tmpTableDir, true)) {
      throw new IOException("Can not delete partial created proc region " + tmpTableDir);
    }
    HRegion.createHRegion(conf, regionInfo, fs, tmpTableDir, td).close();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tn);
    if (!fs.rename(tmpTableDir, tableDir)) {
      throw new IOException("Can not rename " + tmpTableDir + " to " + tableDir);
    }
    WAL wal = createWAL(walFactory, walRoller, serverName, walFs, walRootDir, regionInfo);
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, td, wal, null, null);
  }

  private static HRegion open(Configuration conf, TableDescriptor td, FileSystem fs, Path rootDir,
    FileSystem walFs, Path walRootDir, WALFactory walFactory, MasterRegionWALRoller walRoller,
    String serverName) throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
    Path regionDir =
      fs.listStatus(tableDir, p -> RegionInfo.isEncodedRegionName(Bytes.toBytes(p.getName())))[0]
        .getPath();
    RegionInfo regionInfo = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);

    Path walRegionDir = FSUtils.getRegionDirFromRootDir(walRootDir, regionInfo);
    Path replayEditsDir = new Path(walRegionDir, REPLAY_EDITS_DIR);
    if (!walFs.exists(replayEditsDir) && !walFs.mkdirs(replayEditsDir)) {
      throw new IOException("Failed to create replay directory: " + replayEditsDir);
    }
    Path walsDir = new Path(walRootDir, HREGION_LOGDIR_NAME);
    for (FileStatus walDir : walFs.listStatus(walsDir)) {
      if (!walDir.isDirectory()) {
        continue;
      }
      if (walDir.getPath().getName().startsWith(serverName)) {
        LOG.warn("This should not happen in real production as we have not created our WAL " +
          "directory yet, ignore if you are running a local region related UT");
      }
      Path deadWALDir;
      if (!walDir.getPath().getName().endsWith(DEAD_WAL_DIR_SUFFIX)) {
        deadWALDir =
          new Path(walDir.getPath().getParent(), walDir.getPath().getName() + DEAD_WAL_DIR_SUFFIX);
        if (!walFs.rename(walDir.getPath(), deadWALDir)) {
          throw new IOException("Can not rename " + walDir + " to " + deadWALDir +
            " when recovering lease of proc store");
        }
        LOG.info("Renamed {} to {} as it is dead", walDir.getPath(), deadWALDir);
      } else {
        deadWALDir = walDir.getPath();
        LOG.info("{} is already marked as dead", deadWALDir);
      }
      for (FileStatus walFile : walFs.listStatus(deadWALDir)) {
        Path replayEditsFile = new Path(replayEditsDir, walFile.getPath().getName());
        RecoverLeaseFSUtils.recoverFileLease(walFs, walFile.getPath(), conf);
        if (!walFs.rename(walFile.getPath(), replayEditsFile)) {
          throw new IOException("Can not rename " + walFile.getPath() + " to " + replayEditsFile +
            " when recovering lease for local region");
        }
        LOG.info("Renamed {} to {}", walFile.getPath(), replayEditsFile);
      }
      LOG.info("Delete empty local region wal dir {}", deadWALDir);
      walFs.delete(deadWALDir, true);
    }

    WAL wal = createWAL(walFactory, walRoller, serverName, walFs, walRootDir, regionInfo);
    conf.set(HRegion.SPECIAL_RECOVERED_EDITS_DIR,
      replayEditsDir.makeQualified(walFs.getUri(), walFs.getWorkingDirectory()).toString());
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, td, wal, null, null);
  }

  public static MasterRegion create(MasterRegionParams params) throws IOException {
    TableDescriptor td = params.tableDescriptor();
    LOG.info("Create or load local region for table " + td);
    Server server = params.server();
    Configuration baseConf = server.getConfiguration();
    FileSystem fs = CommonFSUtils.getRootDirFileSystem(baseConf);
    FileSystem walFs = CommonFSUtils.getWALFileSystem(baseConf);
    Path globalRootDir = CommonFSUtils.getRootDir(baseConf);
    Path globalWALRootDir = CommonFSUtils.getWALRootDir(baseConf);
    Path rootDir = new Path(globalRootDir, params.regionDirName());
    Path walRootDir = new Path(globalWALRootDir, params.regionDirName());
    // we will override some configurations so create a new one.
    Configuration conf = new Configuration(baseConf);
    CommonFSUtils.setRootDir(conf, rootDir);
    CommonFSUtils.setWALRootDir(conf, walRootDir);
    MasterRegionFlusherAndCompactor.setupConf(conf, params.flushSize(), params.flushPerChanges(),
      params.flushIntervalMs());
    conf.setInt(AbstractFSWAL.MAX_LOGS, params.maxWals());
    if (params.useHsync() != null) {
      conf.setBoolean(HRegion.WAL_HSYNC_CONF_KEY, params.useHsync());
    }
    if (params.useMetaCellComparator() != null) {
      conf.setBoolean(HRegion.USE_META_CELL_COMPARATOR, params.useMetaCellComparator());
    }
    conf.setInt(AbstractFSWAL.RING_BUFFER_SLOT_COUNT,
      IntMath.ceilingPowerOfTwo(params.ringBufferSlotCount()));

    MasterRegionWALRoller walRoller = MasterRegionWALRoller.create(
      td.getTableName() + "-WAL-Roller", conf, server, walFs, walRootDir, globalWALRootDir,
      params.archivedWalSuffix(), params.rollPeriodMs(), params.flushSize());
    walRoller.start();

    WALFactory walFactory = new WALFactory(conf, server.getServerName().toString());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
    HRegion region;
    if (fs.exists(tableDir)) {
      // load the existing region.
      region = open(conf, td, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
        server.getServerName().toString());
    } else {
      // bootstrapping...
      region = bootstrap(conf, td, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
        server.getServerName().toString());
    }
    Path globalArchiveDir = HFileArchiveUtil.getArchivePath(baseConf);
    MasterRegionFlusherAndCompactor flusherAndCompactor = new MasterRegionFlusherAndCompactor(conf,
      server, region, params.flushSize(), params.flushPerChanges(), params.flushIntervalMs(),
      params.compactMin(), globalArchiveDir, params.archivedHFileSuffix());
    walRoller.setFlusherAndCompactor(flusherAndCompactor);
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    if (!fs.mkdirs(archiveDir)) {
      LOG.warn("Failed to create archive directory {}. Usually this should not happen but it will" +
        " be created again when we actually archive the hfiles later, so continue", archiveDir);
    }
    return new MasterRegion(region, walFactory, flusherAndCompactor, walRoller);
  }
}
