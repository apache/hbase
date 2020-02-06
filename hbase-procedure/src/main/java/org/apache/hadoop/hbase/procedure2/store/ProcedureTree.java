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
package org.apache.hadoop.hbase.procedure2.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Used to build the tree for procedures.
 * <p/>
 * We will group the procedures with the root procedure, and then validate each group. For each
 * group of procedures(with the same root procedure), we will collect all the stack ids, if the max
 * stack id is n, then all the stack ids should be from 0 to n, non-repetition and non-omission. If
 * not, we will consider all the procedures in this group as corrupted. Please see the code in
 * {@link #checkReady(Entry, Map)} method.
 * <p/>
 * For the procedures not in any group, i.e, can not find the root procedure for these procedures,
 * we will also consider them as corrupted. Please see the code in {@link #checkOrphan(Map)} method.
 */
@InterfaceAudience.Private
public final class ProcedureTree {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureTree.class);

  private static final class Entry {

    private final ProcedureProtos.Procedure proc;

    private final List<Entry> subProcs = new ArrayList<>();

    public Entry(ProcedureProtos.Procedure proc) {
      this.proc = proc;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Procedure(pid=");
      sb.append(proc.getProcId());
      sb.append(", ppid=");
      sb.append(proc.hasParentId() ? proc.getParentId() : Procedure.NO_PROC_ID);
      sb.append(", class=");
      sb.append(proc.getClassName());
      sb.append(")");
      return sb.toString();
    }
  }

  private final List<ProtoAndProcedure> validProcs = new ArrayList<>();

  private final List<ProtoAndProcedure> corruptedProcs = new ArrayList<>();

  private ProcedureTree(Map<Long, Entry> procMap) {
    List<Entry> rootEntries = buildTree(procMap);
    for (Entry rootEntry : rootEntries) {
      checkReady(rootEntry, procMap);
    }
    checkOrphan(procMap);
    Comparator<ProtoAndProcedure> cmp =
      (p1, p2) -> Long.compare(p1.getProto().getProcId(), p2.getProto().getProcId());
    Collections.sort(validProcs, cmp);
    Collections.sort(corruptedProcs, cmp);
  }

  private List<Entry> buildTree(Map<Long, Entry> procMap) {
    List<Entry> rootEntries = new ArrayList<>();
    procMap.values().forEach(entry -> {
      if (!entry.proc.hasParentId()) {
        rootEntries.add(entry);
      } else {
        Entry parentEntry = procMap.get(entry.proc.getParentId());
        // For a valid procedure this should not be null. We will log the error later if it is null,
        // as it will not be referenced by any root procedures.
        if (parentEntry != null) {
          parentEntry.subProcs.add(entry);
        }
      }
    });
    return rootEntries;
  }

  private void collectStackId(Entry entry, Map<Integer, List<Entry>> stackId2Proc,
    MutableInt maxStackId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Procedure {} stack ids={}", entry, entry.proc.getStackIdList());
    }
    for (int i = 0, n = entry.proc.getStackIdCount(); i < n; i++) {
      int stackId = entry.proc.getStackId(i);
      if (stackId > maxStackId.intValue()) {
        maxStackId.setValue(stackId);
      }
      stackId2Proc.computeIfAbsent(stackId, k -> new ArrayList<>()).add(entry);
    }
    entry.subProcs.forEach(e -> collectStackId(e, stackId2Proc, maxStackId));
  }

  private void addAllToCorruptedAndRemoveFromProcMap(Entry entry,
    Map<Long, Entry> remainingProcMap) {
    corruptedProcs.add(new ProtoAndProcedure(entry.proc));
    remainingProcMap.remove(entry.proc.getProcId());
    for (Entry e : entry.subProcs) {
      addAllToCorruptedAndRemoveFromProcMap(e, remainingProcMap);
    }
  }

  private void addAllToValidAndRemoveFromProcMap(Entry entry, Map<Long, Entry> remainingProcMap) {
    validProcs.add(new ProtoAndProcedure(entry.proc));
    remainingProcMap.remove(entry.proc.getProcId());
    for (Entry e : entry.subProcs) {
      addAllToValidAndRemoveFromProcMap(e, remainingProcMap);
    }
  }

  // In this method first we will check whether the given root procedure and all its sub procedures
  // are valid, through the procedure stack. And we will also remove all these procedures from the
  // remainingProcMap, so at last, if there are still procedures in the map, we know that there are
  // orphan procedures.
  private void checkReady(Entry rootEntry, Map<Long, Entry> remainingProcMap) {
    if (ProcedureUtil.isFinished(rootEntry.proc)) {
      if (!rootEntry.subProcs.isEmpty()) {
        LOG.error("unexpected active children for root-procedure: {}", rootEntry);
        rootEntry.subProcs.forEach(e -> LOG.error("unexpected active children: {}", e));
        addAllToCorruptedAndRemoveFromProcMap(rootEntry, remainingProcMap);
      } else {
        addAllToValidAndRemoveFromProcMap(rootEntry, remainingProcMap);
      }
      return;
    }
    Map<Integer, List<Entry>> stackId2Proc = new HashMap<>();
    MutableInt maxStackId = new MutableInt(Integer.MIN_VALUE);
    collectStackId(rootEntry, stackId2Proc, maxStackId);
    // the stack ids should start from 0 and increase by one every time
    boolean valid = true;
    for (int i = 0; i <= maxStackId.intValue(); i++) {
      List<Entry> entries = stackId2Proc.get(i);
      if (entries == null) {
        LOG.error("Missing stack id {}, max stack id is {}, root procedure is {}", i, maxStackId,
          rootEntry);
        valid = false;
      } else if (entries.size() > 1) {
        LOG.error("Multiple procedures {} have the same stack id {}, max stack id is {}," +
          " root procedure is {}", entries, i, maxStackId, rootEntry);
        valid = false;
      }
    }
    if (valid) {
      addAllToValidAndRemoveFromProcMap(rootEntry, remainingProcMap);
    } else {
      addAllToCorruptedAndRemoveFromProcMap(rootEntry, remainingProcMap);
    }
  }

  private void checkOrphan(Map<Long, Entry> procMap) {
    procMap.values().forEach(entry -> {
      LOG.error("Orphan procedure: {}", entry);
      corruptedProcs.add(new ProtoAndProcedure(entry.proc));
    });
  }

  public ProcedureIterator getValidProcs() {
    return new InMemoryProcedureIterator(validProcs);
  }

  public ProcedureIterator getCorruptedProcs() {
    return new InMemoryProcedureIterator(corruptedProcs);
  }

  public static ProcedureTree build(Collection<ProcedureProtos.Procedure> procedures) {
    Map<Long, Entry> procMap = new HashMap<>();
    for (ProcedureProtos.Procedure proc : procedures) {
      procMap.put(proc.getProcId(), new Entry(proc));
    }
    return new ProcedureTree(procMap);
  }
}
