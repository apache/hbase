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
import static org.apache.hadoop.hbase.HConstants.NO_NONCE;
import static org.apache.hadoop.hbase.master.region.MasterRegionFactory.PROC_FAMILY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.assignment.MoveRegionProcedure;
import org.apache.hadoop.hbase.master.assignment.UnassignProcedure;
import org.apache.hadoop.hbase.master.procedure.RecoverMetaProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreBase;
import org.apache.hadoop.hbase.procedure2.store.ProcedureTree;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A procedure store which uses the master local store to store all the procedures.
 * <p/>
 * We use proc:d column to store the serialized protobuf format procedure, and when deleting we will
 * first fill the info:proc column with an empty byte array, and then actually delete them in the
 * {@link #cleanup()} method. This is because that we need to retain the max procedure id, so we can
 * not directly delete a procedure row as we do not know if it is the one with the max procedure id.
 */
@InterfaceAudience.Private
public class RegionProcedureStore extends ProcedureStoreBase {

  private static final Logger LOG = LoggerFactory.getLogger(RegionProcedureStore.class);

  static final byte[] PROC_QUALIFIER = Bytes.toBytes("d");

  private final Server server;

  private final LeaseRecovery leaseRecovery;

  final MasterRegion region;

  private int numThreads;

  public RegionProcedureStore(Server server, MasterRegion region, LeaseRecovery leaseRecovery) {
    this.server = server;
    this.region = region;
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

  @Override
  public void stop(boolean abort) {
    if (!setRunning(false)) {
      return;
    }
    LOG.info("Stopping the Region Procedure Store, isAbort={}", abort);
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

  @SuppressWarnings("deprecation")
  private static final ImmutableSet<Class<?>> UNSUPPORTED_PROCEDURES =
    ImmutableSet.of(RecoverMetaProcedure.class, AssignProcedure.class, UnassignProcedure.class,
      MoveRegionProcedure.class);

  /**
   * In HBASE-20811, we have introduced a new TRSP to assign/unassign/move regions, and it is
   * incompatible with the old AssignProcedure/UnassignProcedure/MoveRegionProcedure. So we need to
   * make sure that there are none these procedures when upgrading. If there are, the master will
   * quit, you need to go back to the old version to finish these procedures first before upgrading.
   */
  private void checkUnsupportedProcedure(Map<Class<?>, List<Procedure<?>>> procsByType)
    throws HBaseIOException {
    // Confirm that we do not have unfinished assign/unassign related procedures. It is not easy to
    // support both the old assign/unassign procedures and the new TransitRegionStateProcedure as
    // there will be conflict in the code for AM. We should finish all these procedures before
    // upgrading.
    for (Class<?> clazz : UNSUPPORTED_PROCEDURES) {
      List<Procedure<?>> procs = procsByType.get(clazz);
      if (procs != null) {
        LOG.error("Unsupported procedure type {} found, please rollback your master to the old" +
          " version to finish them, and then try to upgrade again." +
          " See https://hbase.apache.org/book.html#upgrade2.2 for more details." +
          " The full procedure list: {}", clazz, procs);
        throw new HBaseIOException("Unsupported procedure type " + clazz + " found");
      }
    }
    // A special check for SCP, as we do not support RecoverMetaProcedure any more so we need to
    // make sure that no one will try to schedule it but SCP does have a state which will schedule
    // it.
    if (procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream()
      .map(p -> (ServerCrashProcedure) p).anyMatch(ServerCrashProcedure::isInRecoverMetaState)) {
      LOG.error("At least one ServerCrashProcedure is going to schedule a RecoverMetaProcedure," +
        " which is not supported any more. Please rollback your master to the old version to" +
        " finish them, and then try to upgrade again." +
        " See https://hbase.apache.org/book.html#upgrade2.2 for more details.");
      throw new HBaseIOException("Unsupported procedure state found for ServerCrashProcedure");
    }
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
    List<Procedure<?>> procs = new ArrayList<>();
    Map<Class<?>, List<Procedure<?>>> activeProcsByType = new HashMap<>();
    store.load(new ProcedureLoader() {

      @Override
      public void setMaxProcId(long maxProcId) {
        maxProcIdSet.setValue(maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        while (procIter.hasNext()) {
          Procedure<?> proc = procIter.next();
          procs.add(proc);
          if (!proc.isFinished()) {
            activeProcsByType.computeIfAbsent(proc.getClass(), k -> new ArrayList<>()).add(proc);
          }
        }
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

    // check whether there are unsupported procedures, this could happen when we are migrating from
    // 2.1-. We used to do this in HMaster, after loading all the procedures from procedure store,
    // but here we have to do it before migrating, otherwise, if we find some unsupported
    // procedures, the users can not go back to 2.1 to finish them any more, as all the data are now
    // in the new region based procedure store, which is not supported in 2.1-.
    checkUnsupportedProcedure(activeProcsByType);

    MutableLong maxProcIdFromProcs = new MutableLong(-1);
    for (Procedure<?> proc : procs) {
      update(proc);
      if (proc.getProcId() > maxProcIdFromProcs.longValue()) {
        maxProcIdFromProcs.setValue(proc.getProcId());
      }
    }
    LOG.info("Migrated {} existing procedures from the old storage format.", procs.size());
    LOG.info("The WALProcedureStore max pid is {}, and the max pid of all loaded procedures is {}",
      maxProcIdSet.longValue(), maxProcIdFromProcs.longValue());
    // Theoretically, the maxProcIdSet should be greater than or equal to maxProcIdFromProcs, but
    // anyway, let's do a check here.
    if (maxProcIdSet.longValue() > maxProcIdFromProcs.longValue()) {
      if (maxProcIdSet.longValue() > 0) {
        // let's add a fake row to retain the max proc id
        region.update(r -> r.put(new Put(Bytes.toBytes(maxProcIdSet.longValue()))
          .addColumn(PROC_FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY)));
      }
    } else if (maxProcIdSet.longValue() < maxProcIdFromProcs.longValue()) {
      LOG.warn("The WALProcedureStore max pid is less than the max pid of all loaded procedures");
    }
    store.stop(false);
    if (!fs.delete(procWALDir, true)) {
      throw new IOException(
        "Failed to delete the WALProcedureStore migrated proc wal directory " + procWALDir);
    }
    LOG.info("Migration of WALProcedureStore finished");
  }

  @Override
  public void recoverLease() throws IOException {
    LOG.info("Starting Region Procedure Store lease recovery...");
    FileSystem fs = CommonFSUtils.getWALFileSystem(server.getConfiguration());
    tryMigrate(fs);
  }

  @Override
  public void load(ProcedureLoader loader) throws IOException {
    List<ProcedureProtos.Procedure> procs = new ArrayList<>();
    long maxProcId = 0;

    try (RegionScanner scanner =
      region.getScanner(new Scan().addColumn(PROC_FAMILY, PROC_QUALIFIER))) {
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
    mutations.add(new Put(row).addColumn(PROC_FAMILY, PROC_QUALIFIER, proto.toByteArray()));
    rowsToLock.add(row);
  }

  // As we need to keep the max procedure id, here we can not simply delete the procedure, just fill
  // the proc column with an empty array.
  private void serializeDelete(long procId, List<Mutation> mutations, List<byte[]> rowsToLock) {
    byte[] row = Bytes.toBytes(procId);
    mutations.add(new Put(row).addColumn(PROC_FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY));
    rowsToLock.add(row);
  }

  /**
   * Insert procedure may be called by master's rpc call. There are some check about the rpc call
   * when mutate region. Here unset the current rpc call and set it back in finally block. See
   * HBASE-23895 for more details.
   */
  private void runWithoutRpcCall(Runnable runnable) {
    Optional<RpcCall> rpcCall = RpcServer.unsetCurrentCall();
    try {
      runnable.run();
    } finally {
      rpcCall.ifPresent(RpcServer::setCurrentCall);
    }
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
    runWithoutRpcCall(() -> {
      try {
        serializePut(proc, mutations, rowsToLock);
        for (Procedure<?> subProc : subProcs) {
          serializePut(subProc, mutations, rowsToLock);
        }
        region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to insert proc {}, sub procs {}", proc,
          Arrays.toString(subProcs), e);
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public void insert(Procedure<?>[] procs) {
    List<Mutation> mutations = new ArrayList<>(procs.length);
    List<byte[]> rowsToLock = new ArrayList<>(procs.length);
    runWithoutRpcCall(() -> {
      try {
        for (Procedure<?> proc : procs) {
          serializePut(proc, mutations, rowsToLock);
        }
        region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to insert procs {}", Arrays.toString(procs), e);
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public void update(Procedure<?> proc) {
    runWithoutRpcCall(() -> {
      try {
        ProcedureProtos.Procedure proto = ProcedureUtil.convertToProtoProcedure(proc);
        region.update(r -> r.put(new Put(Bytes.toBytes(proc.getProcId())).addColumn(PROC_FAMILY,
          PROC_QUALIFIER, proto.toByteArray())));
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to update proc {}", proc, e);
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public void delete(long procId) {
    try {
      region.update(r -> r.put(
        new Put(Bytes.toBytes(procId)).addColumn(PROC_FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY)));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete pid={}", procId, e);
      throw new UncheckedIOException(e);
    }
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
      region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete parent proc {}, sub pids={}", parentProc,
        Arrays.toString(subProcIds), e);
      throw new UncheckedIOException(e);
    }
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
      region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete pids={}", Arrays.toString(procIds), e);
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void cleanup() {
    // actually delete the procedures if it is not the one with the max procedure id.
    List<Cell> cells = new ArrayList<Cell>();
    try (RegionScanner scanner =
      region.getScanner(new Scan().addColumn(PROC_FAMILY, PROC_QUALIFIER).setReversed(true))) {
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
          region.update(r -> r
            .delete(new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())));
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to clean up delete procedures", e);
    }
  }
}
