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
package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.procedure2.store.ProcedureTree;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureWALEntry;

/**
 * Helper class that loads the procedures stored in a WAL.
 * @deprecated Since 2.3.0, will be removed in 4.0.0. Keep here only for rolling upgrading, now we
 *             use the new region based procedure store.
 */
@Deprecated
@InterfaceAudience.Private
class ProcedureWALFormatReader {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWALFormatReader.class);

  /**
   * We will use the localProcedureMap to track the active procedures for the current proc wal file,
   * and when we finished reading one proc wal file, we will merge he localProcedureMap to the
   * procedureMap, which tracks the global active procedures.
   * <p/>
   * See the comments of {@link WALProcedureMap} for more details.
   * <p/>
   * After reading all the proc wal files, we will use the procedures in the procedureMap to build a
   * {@link ProcedureTree}, and then give the result to the upper layer. See the comments of
   * {@link ProcedureTree} and the code in {@link #finish()} for more details.
   */
  private final WALProcedureMap localProcedureMap = new WALProcedureMap();
  private final WALProcedureMap procedureMap = new WALProcedureMap();

  private final ProcedureWALFormat.Loader loader;

  /**
   * Global tracker that will be used by the WALProcedureStore after load.
   * If the last WAL was closed cleanly we already have a full tracker ready to be used.
   * If the last WAL was truncated (e.g. master killed) the tracker will be empty
   * and the 'partial' flag will be set. In this case, on WAL replay we are going
   * to rebuild the tracker.
   */
  private final ProcedureStoreTracker tracker;

  /**
   * If tracker for a log file is partial (see {@link ProcedureStoreTracker#partial}), we re-build
   * the list of procedures modified in that WAL because we need it for log cleaning purposes. If
   * all procedures modified in a WAL are found to be obsolete, it can be safely deleted. (see
   * {@link WALProcedureStore#removeInactiveLogs()}).
   * <p/>
   * Notice that, the deleted part for this tracker will not be global valid as we can only count
   * the deletes in the current file, but it is not big problem as finally, the above tracker will
   * have the global state of deleted, and it will also be used to build the cleanup tracker.
   */
  private ProcedureStoreTracker localTracker;

  private long maxProcId = 0;

  public ProcedureWALFormatReader(final ProcedureStoreTracker tracker,
      ProcedureWALFormat.Loader loader) {
    this.tracker = tracker;
    this.loader = loader;
  }

  public void read(ProcedureWALFile log) throws IOException {
    localTracker = log.getTracker();
    if (localTracker.isPartial()) {
      LOG.info("Rebuilding tracker for {}", log);
    }

    long count = 0;
    FSDataInputStream stream = log.getStream();
    try {
      boolean hasMore = true;
      while (hasMore) {
        ProcedureWALEntry entry = ProcedureWALFormat.readEntry(stream);
        if (entry == null) {
          LOG.warn("Nothing left to decode. Exiting with missing EOF, log={}", log);
          break;
        }
        count++;
        switch (entry.getType()) {
          case PROCEDURE_WAL_INIT:
            readInitEntry(entry);
            break;
          case PROCEDURE_WAL_INSERT:
            readInsertEntry(entry);
            break;
          case PROCEDURE_WAL_UPDATE:
          case PROCEDURE_WAL_COMPACT:
            readUpdateEntry(entry);
            break;
          case PROCEDURE_WAL_DELETE:
            readDeleteEntry(entry);
            break;
          case PROCEDURE_WAL_EOF:
            hasMore = false;
            break;
          default:
            throw new CorruptedWALProcedureStoreException("Invalid entry: " + entry);
        }
      }
      LOG.info("Read {} entries in {}", count, log);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("While reading entry #{} in {}", count, log, e);
      loader.markCorruptedWAL(log, e);
    }

    if (!localProcedureMap.isEmpty()) {
      log.setProcIds(localProcedureMap.getMinModifiedProcId(),
        localProcedureMap.getMaxModifiedProcId());
      if (localTracker.isPartial()) {
        localTracker.setMinMaxModifiedProcIds(localProcedureMap.getMinModifiedProcId(),
          localProcedureMap.getMaxModifiedProcId());
      }
      procedureMap.merge(localProcedureMap);
    }
    // Do not reset the partial flag for local tracker, as here the local tracker only know the
    // procedures which are modified in this file.
  }

  public void finish() throws IOException {
    // notify the loader about the max proc ID
    loader.setMaxProcId(maxProcId);

    // build the procedure execution tree. When building we will verify that whether a procedure is
    // valid.
    ProcedureTree tree = ProcedureTree.build(procedureMap.getProcedures());
    loader.load(tree.getValidProcs());
    loader.handleCorrupted(tree.getCorruptedProcs());
  }

  private void setDeletedIfPartial(ProcedureStoreTracker tracker, long procId) {
    if (tracker.isPartial()) {
      tracker.setDeleted(procId, true);
    }
  }

  private void insertIfPartial(ProcedureStoreTracker tracker, ProcedureProtos.Procedure proc) {
    if (tracker.isPartial()) {
      tracker.insert(proc.getProcId());
    }
  }

  private void loadProcedure(ProcedureWALEntry entry, ProcedureProtos.Procedure proc) {
    maxProcId = Math.max(maxProcId, proc.getProcId());
    if (isRequired(proc.getProcId())) {
      LOG.trace("Read {} entry {}", entry.getType(), proc.getProcId());
      localProcedureMap.add(proc);
      insertIfPartial(tracker, proc);
    }
    insertIfPartial(localTracker, proc);
  }

  private void readInitEntry(ProcedureWALEntry entry) {
    assert entry.getProcedureCount() == 1 : "Expected only one procedure";
    loadProcedure(entry, entry.getProcedure(0));
  }

  private void readInsertEntry(ProcedureWALEntry entry) {
    assert entry.getProcedureCount() >= 1 : "Expected one or more procedures";
    loadProcedure(entry, entry.getProcedure(0));
    for (int i = 1; i < entry.getProcedureCount(); ++i) {
      loadProcedure(entry, entry.getProcedure(i));
    }
  }

  private void readUpdateEntry(ProcedureWALEntry entry) {
    assert entry.getProcedureCount() == 1 : "Expected only one procedure";
    loadProcedure(entry, entry.getProcedure(0));
  }

  private void readDeleteEntry(ProcedureWALEntry entry) {
    assert entry.hasProcId() : "expected ProcID";

    if (entry.getChildIdCount() > 0) {
      assert entry.getProcedureCount() == 1 : "Expected only one procedure";

      // update the parent procedure
      loadProcedure(entry, entry.getProcedure(0));

      // remove the child procedures of entry.getProcId()
      for (int i = 0, count = entry.getChildIdCount(); i < count; ++i) {
        deleteEntry(entry.getChildId(i));
      }
    } else {
      assert entry.getProcedureCount() == 0 : "Expected no procedures";

      // delete the procedure
      deleteEntry(entry.getProcId());
    }
  }

  private void deleteEntry(final long procId) {
    LOG.trace("delete entry {}", procId);
    maxProcId = Math.max(maxProcId, procId);
    localProcedureMap.remove(procId);
    assert !procedureMap.contains(procId);
    setDeletedIfPartial(tracker, procId);
    setDeletedIfPartial(localTracker, procId);
  }

  private boolean isDeleted(long procId) {
    return tracker.isDeleted(procId) == ProcedureStoreTracker.DeleteState.YES;
  }

  private boolean isRequired(long procId) {
    return !isDeleted(procId) && !procedureMap.contains(procId);
  }
}
