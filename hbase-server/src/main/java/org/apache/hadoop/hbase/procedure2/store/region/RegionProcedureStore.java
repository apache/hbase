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

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.HConstants.HREGION_LOGDIR_NAME;
import static org.apache.hadoop.hbase.HConstants.NO_NONCE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreBase;
import org.apache.hadoop.hbase.procedure2.store.ProcedureTree;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.math.IntMath;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A procedure store which uses a region to store all the procedures.
 * <p/>
 * FileSystem layout:
 *
 * <pre>
 * hbase
 *   |
 *   --MasterProcs
 *       |
 *       --data
 *       |  |
 *       |  --/master/procedure/&lt;encoded-region-name&gt; <---- The region data
 *       |      |
 *       |      --replay <---- The edits to replay
 *       |
 *       --WALs
 *          |
 *          --&lt;master-server-name&gt; <---- The WAL dir for active master
 *          |
 *          --&lt;master-server-name&gt;-dead <---- The WAL dir dead master
 * </pre>
 *
 * We use p:d column to store the serialized protobuf format procedure, and when deleting we will
 * first fill the info:proc column with an empty byte array, and then actually delete them in the
 * {@link #cleanup()} method. This is because that we need to retain the max procedure id, so we can
 * not directly delete a procedure row as we do not know if it is the one with the max procedure id.
 */
@InterfaceAudience.Private
public class RegionProcedureStore extends ProcedureStoreBase {

  private static final Logger LOG = LoggerFactory.getLogger(RegionProcedureStore.class);

  static final String MAX_WALS_KEY = "hbase.procedure.store.region.maxwals";

  private static final int DEFAULT_MAX_WALS = 10;

  static final String USE_HSYNC_KEY = "hbase.procedure.store.region.wal.hsync";

  static final String MASTER_PROCEDURE_DIR = "MasterProcs";

  static final String LOGCLEANER_PLUGINS = "hbase.procedure.store.region.logcleaner.plugins";

  private static final String REPLAY_EDITS_DIR = "recovered.wals";

  private static final String DEAD_WAL_DIR_SUFFIX = "-dead";

  static final TableName TABLE_NAME = TableName.valueOf("master:procedure");

  static final byte[] FAMILY = Bytes.toBytes("p");

  static final byte[] PROC_QUALIFIER = Bytes.toBytes("d");

  private static final int REGION_ID = 1;

  private static final TableDescriptor TABLE_DESC = TableDescriptorBuilder.newBuilder(TABLE_NAME)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();

  private final Server server;

  private final LeaseRecovery leaseRecovery;

  private WALFactory walFactory;

  @VisibleForTesting
  HRegion region;

  private RegionFlusherAndCompactor flusherAndCompactor;

  @VisibleForTesting
  RegionProcedureStoreWALRoller walRoller;

  private int numThreads;

  public RegionProcedureStore(Server server, LeaseRecovery leaseRecovery) {
    this.server = server;
    this.leaseRecovery = leaseRecovery;
  }

  @Override
  public void start(int numThreads) throws IOException {
    if (!setRunning(true)) {
      return;
    }
    LOG.info("Starting the Region Procedure Store, number threads={}", numThreads);
    this.numThreads = numThreads;
  }

  private void shutdownWAL() {
    if (walFactory != null) {
      try {
        walFactory.shutdown();
      } catch (IOException e) {
        LOG.warn("Failed to shutdown WAL", e);
      }
    }
  }

  private void closeRegion(boolean abort) {
    if (region != null) {
      try {
        region.close(abort);
      } catch (IOException e) {
        LOG.warn("Failed to close region", e);
      }
    }

  }

  @Override
  public void stop(boolean abort) {
    if (!setRunning(false)) {
      return;
    }
    LOG.info("Stopping the Region Procedure Store, isAbort={}", abort);
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

  @Override
  public int getNumThreads() {
    return numThreads;
  }

  @Override
  public int setRunningProcedureCount(int count) {
    // useless for region based storage.
    return count;
  }

  private WAL createWAL(FileSystem fs, Path rootDir, RegionInfo regionInfo) throws IOException {
    String logName = AbstractFSWALProvider.getWALDirectoryName(server.getServerName().toString());
    Path walDir = new Path(rootDir, logName);
    LOG.debug("WALDir={}", walDir);
    if (fs.exists(walDir)) {
      throw new HBaseIOException(
        "Master procedure store has already created directory at " + walDir);
    }
    if (!fs.mkdirs(walDir)) {
      throw new IOException("Can not create master procedure wal directory " + walDir);
    }
    WAL wal = walFactory.getWAL(regionInfo);
    walRoller.addWAL(wal);
    return wal;
  }

  private HRegion bootstrap(Configuration conf, FileSystem fs, Path rootDir) throws IOException {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(REGION_ID).build();
    Path tmpTableDir = CommonFSUtils.getTableDir(rootDir, TableName
      .valueOf(TABLE_NAME.getNamespaceAsString(), TABLE_NAME.getQualifierAsString() + "-tmp"));
    if (fs.exists(tmpTableDir) && !fs.delete(tmpTableDir, true)) {
      throw new IOException("Can not delete partial created proc region " + tmpTableDir);
    }
    HRegion.createHRegion(conf, regionInfo, fs, tmpTableDir, TABLE_DESC).close();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, TABLE_NAME);
    if (!fs.rename(tmpTableDir, tableDir)) {
      throw new IOException("Can not rename " + tmpTableDir + " to " + tableDir);
    }
    WAL wal = createWAL(fs, rootDir, regionInfo);
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, TABLE_DESC, wal, null,
      null);
  }

  private HRegion open(Configuration conf, FileSystem fs, Path rootDir) throws IOException {
    String factoryId = server.getServerName().toString();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, TABLE_NAME);
    Path regionDir =
      fs.listStatus(tableDir, p -> RegionInfo.isEncodedRegionName(Bytes.toBytes(p.getName())))[0]
        .getPath();
    Path replayEditsDir = new Path(regionDir, REPLAY_EDITS_DIR);
    if (!fs.exists(replayEditsDir) && !fs.mkdirs(replayEditsDir)) {
      throw new IOException("Failed to create replay directory: " + replayEditsDir);
    }
    Path walsDir = new Path(rootDir, HREGION_LOGDIR_NAME);
    for (FileStatus walDir : fs.listStatus(walsDir)) {
      if (!walDir.isDirectory()) {
        continue;
      }
      if (walDir.getPath().getName().startsWith(factoryId)) {
        LOG.warn("This should not happen in real production as we have not created our WAL " +
          "directory yet, ignore if you are running a procedure related UT");
      }
      Path deadWALDir;
      if (!walDir.getPath().getName().endsWith(DEAD_WAL_DIR_SUFFIX)) {
        deadWALDir =
          new Path(walDir.getPath().getParent(), walDir.getPath().getName() + DEAD_WAL_DIR_SUFFIX);
        if (!fs.rename(walDir.getPath(), deadWALDir)) {
          throw new IOException("Can not rename " + walDir + " to " + deadWALDir +
            " when recovering lease of proc store");
        }
        LOG.info("Renamed {} to {} as it is dead", walDir.getPath(), deadWALDir);
      } else {
        deadWALDir = walDir.getPath();
        LOG.info("{} is already marked as dead", deadWALDir);
      }
      for (FileStatus walFile : fs.listStatus(deadWALDir)) {
        Path replayEditsFile = new Path(replayEditsDir, walFile.getPath().getName());
        leaseRecovery.recoverFileLease(fs, walFile.getPath());
        if (!fs.rename(walFile.getPath(), replayEditsFile)) {
          throw new IOException("Can not rename " + walFile.getPath() + " to " + replayEditsFile +
            " when recovering lease of proc store");
        }
        LOG.info("Renamed {} to {}", walFile.getPath(), replayEditsFile);
      }
      LOG.info("Delete empty proc wal dir {}", deadWALDir);
      fs.delete(deadWALDir, true);
    }
    RegionInfo regionInfo = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
    WAL wal = createWAL(fs, rootDir, regionInfo);
    conf.set(HRegion.SPECIAL_RECOVERED_EDITS_DIR,
      replayEditsDir.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString());
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, TABLE_DESC, wal, null,
      null);
  }

  @SuppressWarnings("deprecation")
  private void tryMigrate(FileSystem fs) throws IOException {
    Configuration conf = server.getConfiguration();
    Path procWALDir =
      new Path(CommonFSUtils.getWALRootDir(conf), WALProcedureStore.MASTER_PROCEDURE_LOGDIR);
    if (!fs.exists(procWALDir)) {
      return;
    }
    LOG.info("The old WALProcedureStore wal directory {} exists, migrating...", procWALDir);
    WALProcedureStore store = new WALProcedureStore(conf, leaseRecovery);
    store.start(numThreads);
    store.recoverLease();
    MutableLong maxProcIdSet = new MutableLong(-1);
    MutableLong maxProcIdFromProcs = new MutableLong(-1);
    store.load(new ProcedureLoader() {

      @Override
      public void setMaxProcId(long maxProcId) {
        maxProcIdSet.setValue(maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        long procCount = 0;
        while (procIter.hasNext()) {
          Procedure<?> proc = procIter.next();
          update(proc);
          procCount++;
          if (proc.getProcId() > maxProcIdFromProcs.longValue()) {
            maxProcIdFromProcs.setValue(proc.getProcId());
          }
        }
        LOG.info("Migrated {} procedures", procCount);
      }

      @Override
      public void handleCorrupted(ProcedureIterator procIter) throws IOException {
        long corruptedCount = 0;
        while (procIter.hasNext()) {
          LOG.error("Corrupted procedure {}", procIter.next());
          corruptedCount++;
        }
        if (corruptedCount > 0) {
          throw new IOException("There are " + corruptedCount + " corrupted procedures when" +
            " migrating from the old WAL based store to the new region based store, please" +
            " fix them before upgrading again.");
        }
      }
    });
    LOG.info("The WALProcedureStore max pid is {}, and the max pid of all loaded procedures is {}",
      maxProcIdSet.longValue(), maxProcIdFromProcs.longValue());
    // Theoretically, the maxProcIdSet should be greater than or equal to maxProcIdFromProcs, but
    // anyway, let's do a check here.
    if (maxProcIdSet.longValue() > maxProcIdFromProcs.longValue()) {
      if (maxProcIdSet.longValue() > 0) {
        // let's add a fake row to retain the max proc id
        region.put(new Put(Bytes.toBytes(maxProcIdSet.longValue())).addColumn(FAMILY,
          PROC_QUALIFIER, EMPTY_BYTE_ARRAY));
      }
    } else if (maxProcIdSet.longValue() < maxProcIdFromProcs.longValue()) {
      LOG.warn("The WALProcedureStore max pid is less than the max pid of all loaded procedures");
    }
    if (!fs.delete(procWALDir, true)) {
      throw new IOException("Failed to delete the WALProcedureStore migrated proc wal directory " +
        procWALDir);
    }
    LOG.info("Migration of WALProcedureStore finished");
  }

  @Override
  public void recoverLease() throws IOException {
    LOG.debug("Starting Region Procedure Store lease recovery...");
    Configuration baseConf = server.getConfiguration();
    FileSystem fs = CommonFSUtils.getWALFileSystem(baseConf);
    Path globalWALRootDir = CommonFSUtils.getWALRootDir(baseConf);
    Path rootDir = new Path(globalWALRootDir, MASTER_PROCEDURE_DIR);
    // we will override some configurations so create a new one.
    Configuration conf = new Configuration(baseConf);
    CommonFSUtils.setRootDir(conf, rootDir);
    CommonFSUtils.setWALRootDir(conf, rootDir);
    RegionFlusherAndCompactor.setupConf(conf);
    conf.setInt(AbstractFSWAL.MAX_LOGS, conf.getInt(MAX_WALS_KEY, DEFAULT_MAX_WALS));
    if (conf.get(USE_HSYNC_KEY) != null) {
      conf.set(HRegion.WAL_HSYNC_CONF_KEY, conf.get(USE_HSYNC_KEY));
    }
    conf.setInt(AbstractFSWAL.RING_BUFFER_SLOT_COUNT, IntMath.ceilingPowerOfTwo(16 * numThreads));

    walRoller = RegionProcedureStoreWALRoller.create(conf, server, fs, rootDir, globalWALRootDir);
    walRoller.start();

    walFactory = new WALFactory(conf, server.getServerName().toString(), false);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, TABLE_NAME);
    if (fs.exists(tableDir)) {
      // load the existing region.
      region = open(conf, fs, rootDir);
    } else {
      // bootstrapping...
      region = bootstrap(conf, fs, rootDir);
    }
    flusherAndCompactor = new RegionFlusherAndCompactor(conf, server, region);
    walRoller.setFlusherAndCompactor(flusherAndCompactor);
    tryMigrate(fs);
  }

  @Override
  public void load(ProcedureLoader loader) throws IOException {
    List<ProcedureProtos.Procedure> procs = new ArrayList<>();
    long maxProcId = 0;

    try (RegionScanner scanner = region.getScanner(new Scan().addColumn(FAMILY, PROC_QUALIFIER))) {
      List<Cell> cells = new ArrayList<>();
      boolean moreRows;
      do {
        moreRows = scanner.next(cells);
        if (cells.isEmpty()) {
          continue;
        }
        Cell cell = cells.get(0);
        cells.clear();
        maxProcId = Math.max(maxProcId,
          Bytes.toLong(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        if (cell.getValueLength() > 0) {
          ProcedureProtos.Procedure proto = ProcedureProtos.Procedure.parser()
            .parseFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
          procs.add(proto);
        }
      } while (moreRows);
    }
    loader.setMaxProcId(maxProcId);
    ProcedureTree tree = ProcedureTree.build(procs);
    loader.load(tree.getValidProcs());
    loader.handleCorrupted(tree.getCorruptedProcs());
  }

  private void serializePut(Procedure<?> proc, List<Mutation> mutations, List<byte[]> rowsToLock)
    throws IOException {
    ProcedureProtos.Procedure proto = ProcedureUtil.convertToProtoProcedure(proc);
    byte[] row = Bytes.toBytes(proc.getProcId());
    mutations.add(new Put(row).addColumn(FAMILY, PROC_QUALIFIER, proto.toByteArray()));
    rowsToLock.add(row);
  }

  // As we need to keep the max procedure id, here we can not simply delete the procedure, just fill
  // the proc column with an empty array.
  private void serializeDelete(long procId, List<Mutation> mutations, List<byte[]> rowsToLock) {
    byte[] row = Bytes.toBytes(procId);
    mutations.add(new Put(row).addColumn(FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY));
    rowsToLock.add(row);
  }

  @Override
  public void insert(Procedure<?> proc, Procedure<?>[] subProcs) {
    if (subProcs == null || subProcs.length == 0) {
      // same with update, just insert a single procedure
      update(proc);
      return;
    }
    List<Mutation> mutations = new ArrayList<>(subProcs.length + 1);
    List<byte[]> rowsToLock = new ArrayList<>(subProcs.length + 1);
    try {
      serializePut(proc, mutations, rowsToLock);
      for (Procedure<?> subProc : subProcs) {
        serializePut(subProc, mutations, rowsToLock);
      }
      region.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE);
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to insert proc {}, sub procs {}", proc,
        Arrays.toString(subProcs), e);
      throw new UncheckedIOException(e);
    }
    flusherAndCompactor.onUpdate();
  }

  @Override
  public void insert(Procedure<?>[] procs) {
    List<Mutation> mutations = new ArrayList<>(procs.length);
    List<byte[]> rowsToLock = new ArrayList<>(procs.length);
    try {
      for (Procedure<?> proc : procs) {
        serializePut(proc, mutations, rowsToLock);
      }
      region.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE);
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to insert procs {}", Arrays.toString(procs), e);
      throw new UncheckedIOException(e);
    }
    flusherAndCompactor.onUpdate();
  }

  @Override
  public void update(Procedure<?> proc) {
    try {
      ProcedureProtos.Procedure proto = ProcedureUtil.convertToProtoProcedure(proc);
      region.put(new Put(Bytes.toBytes(proc.getProcId())).addColumn(FAMILY, PROC_QUALIFIER,
        proto.toByteArray()));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to update proc {}", proc, e);
      throw new UncheckedIOException(e);
    }
    flusherAndCompactor.onUpdate();
  }

  @Override
  public void delete(long procId) {
    try {
      region
        .put(new Put(Bytes.toBytes(procId)).addColumn(FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete pid={}", procId, e);
      throw new UncheckedIOException(e);
    }
    flusherAndCompactor.onUpdate();
  }

  @Override
  public void delete(Procedure<?> parentProc, long[] subProcIds) {
    List<Mutation> mutations = new ArrayList<>(subProcIds.length + 1);
    List<byte[]> rowsToLock = new ArrayList<>(subProcIds.length + 1);
    try {
      serializePut(parentProc, mutations, rowsToLock);
      for (long subProcId : subProcIds) {
        serializeDelete(subProcId, mutations, rowsToLock);
      }
      region.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE);
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete parent proc {}, sub pids={}", parentProc,
        Arrays.toString(subProcIds), e);
      throw new UncheckedIOException(e);
    }
    flusherAndCompactor.onUpdate();
  }

  @Override
  public void delete(long[] procIds, int offset, int count) {
    if (count == 0) {
      return;
    }
    if (count == 1) {
      delete(procIds[offset]);
      return;
    }
    List<Mutation> mutations = new ArrayList<>(count);
    List<byte[]> rowsToLock = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      long procId = procIds[offset + i];
      serializeDelete(procId, mutations, rowsToLock);
    }
    try {
      region.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE);
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete pids={}", Arrays.toString(procIds), e);
      throw new UncheckedIOException(e);
    }
    flusherAndCompactor.onUpdate();
  }

  @Override
  public void cleanup() {
    // actually delete the procedures if it is not the one with the max procedure id.
    List<Cell> cells = new ArrayList<Cell>();
    try (RegionScanner scanner =
      region.getScanner(new Scan().addColumn(FAMILY, PROC_QUALIFIER).setReversed(true))) {
      // skip the row with max procedure id
      boolean moreRows = scanner.next(cells);
      if (cells.isEmpty()) {
        return;
      }
      cells.clear();
      while (moreRows) {
        moreRows = scanner.next(cells);
        if (cells.isEmpty()) {
          continue;
        }
        Cell cell = cells.get(0);
        cells.clear();
        if (cell.getValueLength() == 0) {
          region.delete(new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to clean up delete procedures", e);
    }
  }
}