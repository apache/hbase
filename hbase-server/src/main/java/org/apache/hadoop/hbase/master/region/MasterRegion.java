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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * archived directory with the {@link MasterRegionParams#archivedHFileSuffix()} suffix. The archived
 * WAL will be moved to the global WAL archived directory with the
 * {@link MasterRegionParams#archivedWalSuffix()} suffix.
 */
@InterfaceAudience.Private
public final class MasterRegion {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegion.class);

  private static final String REPLAY_EDITS_DIR = "recovered.wals";

  private static final String DEAD_WAL_DIR_SUFFIX = "-dead";

  static final String INITIALIZING_FLAG = ".initializing";

  static final String INITIALIZED_FLAG = ".initialized";

  private static final int REGION_ID = 1;

  private final WALFactory walFactory;

  final HRegion region;

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

  public ResultScanner getScanner(Scan scan) throws IOException {
    return new RegionScannerAsResultScanner(region.getScanner(scan));
  }

  public RegionScanner getRegionScanner(Scan scan) throws IOException {
    return region.getScanner(scan);
  }

  public FlushResult flush(boolean force) throws IOException {
    return region.flush(force);
  }

  public void requestRollAll() {
    walRoller.requestRollAll();
  }

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
    MasterRegionWALRoller walRoller, String serverName, boolean touchInitializingFlag)
    throws IOException {
    TableName tn = td.getTableName();
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tn).setRegionId(REGION_ID).build();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tn);
    // persist table descriptor
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, td, true);
    HRegion.createHRegion(conf, regionInfo, fs, tableDir, td).close();
    Path initializedFlag = new Path(tableDir, INITIALIZED_FLAG);
    if (!fs.mkdirs(initializedFlag)) {
      throw new IOException("Can not touch initialized flag: " + initializedFlag);
    }
    Path initializingFlag = new Path(tableDir, INITIALIZING_FLAG);
    if (!fs.delete(initializingFlag, true)) {
      LOG.warn("failed to clean up initializing flag: " + initializingFlag);
    }
    WAL wal = createWAL(walFactory, walRoller, serverName, walFs, walRootDir, regionInfo);
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, td, wal, null, null);
  }

  private static RegionInfo loadRegionInfo(FileSystem fs, Path tableDir) throws IOException {
    Path regionDir =
      fs.listStatus(tableDir, p -> RegionInfo.isEncodedRegionName(Bytes.toBytes(p.getName())))[0]
        .getPath();
    return HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
  }

  private static HRegion open(Configuration conf, TableDescriptor td, RegionInfo regionInfo,
    FileSystem fs, Path rootDir, FileSystem walFs, Path walRootDir, WALFactory walFactory,
    MasterRegionWALRoller walRoller, String serverName) throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
    Path walRegionDir = FSUtils.getRegionDirFromRootDir(walRootDir, regionInfo);
    Path replayEditsDir = new Path(walRegionDir, REPLAY_EDITS_DIR);
    if (!walFs.exists(replayEditsDir) && !walFs.mkdirs(replayEditsDir)) {
      throw new IOException("Failed to create replay directory: " + replayEditsDir);
    }

    // Replay any WALs for the Master Region before opening it.
    Path walsDir = new Path(walRootDir, HREGION_LOGDIR_NAME);
    // In open(...), we expect that the WAL directory for the MasterRegion to already exist.
    // This is in contrast to bootstrap() where we create the MasterRegion data and WAL dir.
    // However, it's possible that users directly remove the WAL directory. We expect walsDir
    // to always exist in normal situations, but we should guard against users changing the
    // filesystem outside of HBase's line of sight.
    if (walFs.exists(walsDir)) {
      replayWALs(conf, walFs, walRootDir, walsDir, regionInfo, serverName, replayEditsDir);
    } else {
      LOG.error("UNEXPECTED: WAL directory for MasterRegion is missing."
          + " {} is unexpectedly missing.", walsDir);
    }

    // Create a new WAL
    WAL wal = createWAL(walFactory, walRoller, serverName, walFs, walRootDir, regionInfo);
    conf.set(HRegion.SPECIAL_RECOVERED_EDITS_DIR,
      replayEditsDir.makeQualified(walFs.getUri(), walFs.getWorkingDirectory()).toString());
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, td, wal, null, null);
  }

  private static void replayWALs(Configuration conf, FileSystem walFs, Path walRootDir,
      Path walsDir, RegionInfo regionInfo, String serverName, Path replayEditsDir)
          throws IOException {
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
  }

  private static void tryMigrate(Configuration conf, FileSystem fs, Path tableDir,
    RegionInfo regionInfo, TableDescriptor oldTd, TableDescriptor newTd) throws IOException {
    Class<? extends StoreFileTracker> oldSft =
      StoreFileTrackerFactory.getTrackerClass(oldTd.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    Class<? extends StoreFileTracker> newSft =
      StoreFileTrackerFactory.getTrackerClass(newTd.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    if (oldSft.equals(newSft)) {
      LOG.debug("old store file tracker {} is the same with new store file tracker, skip migration",
        StoreFileTrackerFactory.getStoreFileTrackerName(oldSft));
      if (!oldTd.equals(newTd)) {
        // we may change other things such as adding a new family, so here we still need to persist
        // the new table descriptor
        LOG.info("Update table descriptor from {} to {}", oldTd, newTd);
        FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, newTd, true);
      }
      return;
    }
    LOG.info("Migrate store file tracker from {} to {}", oldSft.getSimpleName(),
      newSft.getSimpleName());
    HRegionFileSystem hfs =
      HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, regionInfo, false);
    for (ColumnFamilyDescriptor oldCfd : oldTd.getColumnFamilies()) {
      StoreFileTracker oldTracker = StoreFileTrackerFactory.create(conf, oldTd, oldCfd, hfs);
      StoreFileTracker newTracker = StoreFileTrackerFactory.create(conf, oldTd, oldCfd, hfs);
      List<StoreFileInfo> files = oldTracker.load();
      LOG.debug("Store file list for {}: {}", oldCfd.getNameAsString(), files);
      newTracker.set(oldTracker.load());
    }
    // persist the new table descriptor after migration
    LOG.info("Update table descriptor from {} to {}", oldTd, newTd);
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, newTd, true);
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

    WALFactory walFactory = new WALFactory(conf, server.getServerName().toString(), server, false);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
    Path initializingFlag = new Path(tableDir, INITIALIZING_FLAG);
    Path initializedFlag = new Path(tableDir, INITIALIZED_FLAG);
    HRegion region;
    if (!fs.exists(tableDir)) {
      // bootstrap, no doubt
      if (!fs.mkdirs(initializedFlag)) {
        throw new IOException("Can not touch initialized flag");
      }
      region = bootstrap(conf, td, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
        server.getServerName().toString(), true);
    } else {
      if (!fs.exists(initializedFlag)) {
        if (!fs.exists(initializingFlag)) {
          // should be old style, where we do not have the initializing or initialized file, persist
          // the table descriptor, touch the initialized flag and then open the region.
          // the store file tracker must be DEFAULT
          LOG.info("No {} or {} file, try upgrading", INITIALIZING_FLAG, INITIALIZED_FLAG);
          TableDescriptor oldTd =
            TableDescriptorBuilder.newBuilder(td).setValue(StoreFileTrackerFactory.TRACKER_IMPL,
              StoreFileTrackerFactory.Trackers.DEFAULT.name()).build();
          FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, oldTd, true);
          if (!fs.mkdirs(initializedFlag)) {
            throw new IOException("Can not touch initialized flag: " + initializedFlag);
          }
          RegionInfo regionInfo = loadRegionInfo(fs, tableDir);
          tryMigrate(conf, fs, tableDir, regionInfo, oldTd, td);
          region = open(conf, td, regionInfo, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
            server.getServerName().toString());
        } else {
          // delete all contents besides the initializing flag, here we can make sure tableDir
          // exists(unless someone delete it manually...), so we do not do null check here.
          for (FileStatus status : fs.listStatus(tableDir)) {
            if (!status.getPath().getName().equals(INITIALIZING_FLAG)) {
              fs.delete(status.getPath(), true);
            }
          }
          region = bootstrap(conf, td, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
            server.getServerName().toString(), false);
        }
      } else {
        if (fs.exists(initializingFlag) && !fs.delete(initializingFlag, true)) {
          LOG.warn("failed to clean up initializing flag: " + initializingFlag);
        }
        // open it, make sure to load the table descriptor from fs
        TableDescriptor oldTd = FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir);
        RegionInfo regionInfo = loadRegionInfo(fs, tableDir);
        tryMigrate(conf, fs, tableDir, regionInfo, oldTd, td);
        region = open(conf, td, regionInfo, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
          server.getServerName().toString());
      }
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
