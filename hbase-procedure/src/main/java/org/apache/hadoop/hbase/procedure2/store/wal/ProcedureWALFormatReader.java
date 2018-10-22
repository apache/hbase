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
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureWALEntry;

/**
 * Helper class that loads the procedures stored in a WAL
 */
@InterfaceAudience.Private
public class ProcedureWALFormatReader {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWALFormatReader.class);

  // ==============================================================================================
  //  We read the WALs in reverse order from the newest to the oldest.
  //  We have different entry types:
  //   - INIT: Procedure submitted by the user (also known as 'root procedure')
  //   - INSERT: Children added to the procedure <parentId>:[<childId>, ...]
  //   - UPDATE: The specified procedure was updated
  //   - DELETE: The procedure was removed (finished/rolledback and result TTL expired)
  //
  // In the WAL we can find multiple times the same procedure as UPDATE or INSERT.
  // We read the WAL from top to bottom, so every time we find an entry of the
  // same procedure, that will be the "latest" update (Caveat: with multiple threads writing
  // the store, this assumption does not hold).
  //
  // We keep two in-memory maps:
  //  - localProcedureMap: is the map containing the entries in the WAL we are processing
  //  - procedureMap: is the map containing all the procedures we found up to the WAL in process.
  // localProcedureMap is merged with the procedureMap once we reach the WAL EOF.
  //
  // Since we are reading the WALs in reverse order (newest to oldest),
  // if we find an entry related to a procedure we already have in 'procedureMap' we can discard it.
  //
  // The WAL is append-only so the last procedure in the WAL is the one that
  // was in execution at the time we crashed/closed the server.
  // Given that, the procedure replay order can be inferred by the WAL order.
  //
  // Example:
  //    WAL-2: [A, B, A, C, D]
  //    WAL-1: [F, G, A, F, B]
  //    Replay-Order: [D, C, A, B, F, G]
  //
  // The "localProcedureMap" keeps a "replayOrder" list. Every time we add the
  // record to the map that record is moved to the head of the "replayOrder" list.
  // Using the example above:
  //    WAL-2 localProcedureMap.replayOrder is [D, C, A, B]
  //    WAL-1 localProcedureMap.replayOrder is [F, G]
  //
  // Each time we reach the WAL-EOF, the "replayOrder" list is merged/appended in 'procedureMap'
  // so using the example above we end up with: [D, C, A, B] + [F, G] as replay order.
  //
  //  Fast Start: INIT/INSERT record and StackIDs
  // ---------------------------------------------
  // We have two special records, INIT and INSERT, that track the first time
  // the procedure was added to the WAL. We can use this information to be able
  // to start procedures before reaching the end of the WAL, or before reading all WALs.
  // But in some cases, the WAL with that record can be already gone.
  // As an alternative, we can use the stackIds on each procedure,
  // to identify when a procedure is ready to start.
  // If there are gaps in the sum of the stackIds we need to read more WALs.
  //
  // Example (all procs child of A):
  //   WAL-2: [A, B]                   A stackIds = [0, 4], B stackIds = [1, 5]
  //   WAL-1: [A, B, C, D]
  //
  // In the case above we need to read one more WAL to be able to consider
  // the root procedure A and all children as ready.
  // ==============================================================================================
  private final WALProcedureMap localProcedureMap = new WALProcedureMap(1024);
  private final WALProcedureMap procedureMap = new WALProcedureMap(1024);

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
      procedureMap.mergeTail(localProcedureMap);
    }
    if (localTracker.isPartial()) {
      localTracker.setPartialFlag(false);
    }
  }

  public void finish() throws IOException {
    // notify the loader about the max proc ID
    loader.setMaxProcId(maxProcId);

    // fetch the procedure ready to run.
    ProcedureIterator procIter = procedureMap.fetchReady();
    if (procIter != null) {
      loader.load(procIter);
    }

    // remaining procedures have missing link or dependencies
    // consider them as corrupted, manual fix is probably required.
    procIter = procedureMap.fetchAll();
    if (procIter != null) {
      loader.handleCorrupted(procIter);
    }
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
