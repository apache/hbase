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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * This class is used to track the active procedures when loading procedures from proc wal file.
 * <p/>
 * We will read proc wal files from new to old, but when reading a proc wal file, we will still read
 * from top to bottom, so there are two groups of methods for this class.
 * <p/>
 * The first group is {@link #add(ProcedureProtos.Procedure)} and {@link #remove(long)}. It is used
 * when reading a proc wal file. In these methods, for the same procedure, typically the one comes
 * later should win, please see the comment for
 * {@link #isIncreasing(ProcedureProtos.Procedure, ProcedureProtos.Procedure)} to see the
 * exceptions.
 * <p/>
 * The second group is {@link #merge(WALProcedureMap)}. We will have a global
 * {@link WALProcedureMap} to hold global the active procedures, and a local {@link WALProcedureMap}
 * to hold the active procedures for the current proc wal file. And when we finish reading a proc
 * wal file, we will merge the local one into the global one, by calling the
 * {@link #merge(WALProcedureMap)} method of the global one and pass the local one in. In this
 * method, for the same procedure, the one comes earlier will win, as we read the proc wal files
 * from new to old(the reverse order).
 * @deprecated Since 2.3.0, will be removed in 4.0.0. Keep here only for rolling upgrading, now we
 *             use the new region based procedure store.
 */
@Deprecated
@InterfaceAudience.Private
class WALProcedureMap {

  private static final Logger LOG = LoggerFactory.getLogger(WALProcedureMap.class);

  private final Map<Long, ProcedureProtos.Procedure> procMap = new HashMap<>();

  private long minModifiedProcId = Long.MAX_VALUE;

  private long maxModifiedProcId = Long.MIN_VALUE;

  private void trackProcId(long procId) {
    minModifiedProcId = Math.min(minModifiedProcId, procId);
    maxModifiedProcId = Math.max(maxModifiedProcId, procId);
  }

  /**
   * @return True if this new procedure is 'richer' than the current one else false and we log this
   *         incidence where it appears that the WAL has older entries appended after newer ones.
   *         See HBASE-18152.
   */
  private static boolean isIncreasing(ProcedureProtos.Procedure current,
      ProcedureProtos.Procedure candidate) {
    // Check that the procedures we see are 'increasing'. We used to compare
    // procedure id first and then update time but it can legitimately go backwards if the
    // procedure is failed or rolled back so that was unreliable. Was going to compare
    // state but lets see if comparing update time enough (unfortunately this issue only
    // seen under load...)
    boolean increasing = current.getLastUpdate() <= candidate.getLastUpdate();
    if (!increasing) {
      LOG.warn("NOT INCREASING! current=" + current + ", candidate=" + candidate);
    }
    return increasing;
  }

  public void add(ProcedureProtos.Procedure proc) {
    procMap.compute(proc.getProcId(), (procId, existingProc) -> {
      if (existingProc == null || isIncreasing(existingProc, proc)) {
        return proc;
      } else {
        return existingProc;
      }
    });
    trackProcId(proc.getProcId());
  }

  public void remove(long procId) {
    procMap.remove(procId);
  }

  public boolean isEmpty() {
    return procMap.isEmpty();
  }

  public boolean contains(long procId) {
    return procMap.containsKey(procId);
  }

  /**
   * Merge the given {@link WALProcedureMap} into this one. The {@link WALProcedureMap} passed in
   * will be cleared after merging.
   */
  public void merge(WALProcedureMap other) {
    other.procMap.forEach(procMap::putIfAbsent);
    maxModifiedProcId = Math.max(maxModifiedProcId, other.maxModifiedProcId);
    minModifiedProcId = Math.max(minModifiedProcId, other.minModifiedProcId);
    other.procMap.clear();
    other.maxModifiedProcId = Long.MIN_VALUE;
    other.minModifiedProcId = Long.MAX_VALUE;
  }

  public Collection<ProcedureProtos.Procedure> getProcedures() {
    return Collections.unmodifiableCollection(procMap.values());
  }

  public long getMinModifiedProcId() {
    return minModifiedProcId;
  }

  public long getMaxModifiedProcId() {
    return maxModifiedProcId;
  }
}
