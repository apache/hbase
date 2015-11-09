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
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALEntry;

/**
 * Helper class that loads the procedures stored in a WAL
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALFormatReader {
  private static final Log LOG = LogFactory.getLog(ProcedureWALFormatReader.class);

  private final ProcedureStoreTracker tracker;
  //private final long compactionLogId;

  private final Map<Long, Procedure> procedures = new HashMap<Long, Procedure>();
  private final Map<Long, ProcedureProtos.Procedure> localProcedures =
    new HashMap<Long, ProcedureProtos.Procedure>();

  private long maxProcId = 0;

  public ProcedureWALFormatReader(final ProcedureStoreTracker tracker) {
    this.tracker = tracker;
  }

  public void read(ProcedureWALFile log, ProcedureWALFormat.Loader loader) throws IOException {
    FSDataInputStream stream = log.getStream();
    try {
      boolean hasMore = true;
      while (hasMore) {
        ProcedureWALEntry entry = ProcedureWALFormat.readEntry(stream);
        if (entry == null) {
          LOG.warn("nothing left to decode. exiting with missing EOF");
          hasMore = false;
          break;
        }
        switch (entry.getType()) {
          case INIT:
            readInitEntry(entry);
            break;
          case INSERT:
            readInsertEntry(entry);
            break;
          case UPDATE:
          case COMPACT:
            readUpdateEntry(entry);
            break;
          case DELETE:
            readDeleteEntry(entry);
            break;
          case EOF:
            hasMore = false;
            break;
          default:
            throw new CorruptedWALProcedureStoreException("Invalid entry: " + entry);
        }
      }
    } catch (IOException e) {
      LOG.error("got an exception while reading the procedure WAL: " + log, e);
      loader.markCorruptedWAL(log, e);
    }

    if (localProcedures.isEmpty()) {
      LOG.info("No active entry found in state log " + log + ". removing it");
      loader.removeLog(log);
    } else {
      Iterator<Map.Entry<Long, ProcedureProtos.Procedure>> itd =
        localProcedures.entrySet().iterator();
      long minProcId = Long.MAX_VALUE;
      long maxProcId = Long.MIN_VALUE;
      while (itd.hasNext()) {
        Map.Entry<Long, ProcedureProtos.Procedure> entry = itd.next();
        itd.remove();

        long procId = entry.getKey();
        minProcId = Math.min(minProcId, procId);
        maxProcId = Math.max(maxProcId, procId);

        // Deserialize the procedure
        Procedure proc = Procedure.convert(entry.getValue());
        procedures.put(procId, proc);
      }

      // TODO: Some procedure may be already runnables (see readInitEntry())
      //       (we can also check the "update map" in the log trackers)
      log.setProcIds(minProcId, maxProcId);
    }
  }

  public Iterator<Procedure> getProcedures() {
    return procedures.values().iterator();
  }

  private void loadEntries(final ProcedureWALEntry entry) {
    for (ProcedureProtos.Procedure proc: entry.getProcedureList()) {
      maxProcId = Math.max(maxProcId, proc.getProcId());
      if (isRequired(proc.getProcId())) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("read " + entry.getType() + " entry " + proc.getProcId());
        }
        localProcedures.put(proc.getProcId(), proc);
        tracker.setDeleted(proc.getProcId(), false);
      }
    }
  }

  private void readInitEntry(final ProcedureWALEntry entry)
      throws IOException {
    assert entry.getProcedureCount() == 1 : "Expected only one procedure";
    // TODO: Make it runnable, before reading other files
    loadEntries(entry);
  }

  private void readInsertEntry(final ProcedureWALEntry entry) throws IOException {
    assert entry.getProcedureCount() >= 1 : "Expected one or more procedures";
    loadEntries(entry);
  }

  private void readUpdateEntry(final ProcedureWALEntry entry) throws IOException {
    assert entry.getProcedureCount() == 1 : "Expected only one procedure";
    loadEntries(entry);
  }

  private void readDeleteEntry(final ProcedureWALEntry entry) throws IOException {
    assert entry.getProcedureCount() == 0 : "Expected no procedures";
    assert entry.hasProcId() : "expected ProcID";
    if (LOG.isTraceEnabled()) {
      LOG.trace("read delete entry " + entry.getProcId());
    }
    maxProcId = Math.max(maxProcId, entry.getProcId());
    localProcedures.remove(entry.getProcId());
    tracker.setDeleted(entry.getProcId(), true);
  }

  private boolean isDeleted(final long procId) {
    return tracker.isDeleted(procId) == ProcedureStoreTracker.DeleteState.YES;
  }

  private boolean isRequired(final long procId) {
    return !isDeleted(procId) && !procedures.containsKey(procId);
  }
}
