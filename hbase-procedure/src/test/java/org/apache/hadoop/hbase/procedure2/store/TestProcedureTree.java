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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@Category({ MasterTests.class, SmallTests.class })
public class TestProcedureTree {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedureTree.class);

  public static final class TestProcedure extends Procedure<Void> {

    @Override
    public void setProcId(long procId) {
      super.setProcId(procId);
    }

    @Override
    public void setParentProcId(long parentProcId) {
      super.setParentProcId(parentProcId);
    }

    @Override
    public synchronized void addStackIndex(int index) {
      super.addStackIndex(index);
    }

    @Override
    protected Procedure<Void>[] execute(Void env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      return null;
    }

    @Override
    protected void rollback(Void env) throws IOException, InterruptedException {
    }

    @Override
    protected boolean abort(Void env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  private TestProcedure createProc(long procId, long parentProcId) {
    TestProcedure proc = new TestProcedure();
    proc.setProcId(procId);
    if (parentProcId != Procedure.NO_PROC_ID) {
      proc.setParentProcId(parentProcId);
    }
    return proc;
  }

  private List<ProcedureProtos.Procedure> toProtos(TestProcedure... procs) {
    return Arrays.stream(procs).map(p -> {
      try {
        return ProcedureUtil.convertToProtoProcedure(p);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).collect(Collectors.toList());
  }

  private List<TestProcedure> getProcs(ProcedureIterator iter) throws IOException {
    List<TestProcedure> procs = new ArrayList<>();
    while (iter.hasNext()) {
      procs.add((TestProcedure) iter.next());
    }
    return procs;
  }

  @Test
  public void testMissingStackId() throws IOException {
    TestProcedure proc0 = createProc(1, Procedure.NO_PROC_ID);
    proc0.addStackIndex(0);
    TestProcedure proc1 = createProc(2, 1);
    proc1.addStackIndex(1);
    TestProcedure proc2 = createProc(3, 2);
    proc2.addStackIndex(3);
    ProcedureTree tree = ProcedureTree.build(toProtos(proc0, proc1, proc2));
    List<TestProcedure> validProcs = getProcs(tree.getValidProcs());
    assertEquals(0, validProcs.size());
    List<TestProcedure> corruptedProcs = getProcs(tree.getCorruptedProcs());
    assertEquals(3, corruptedProcs.size());
    assertEquals(1, corruptedProcs.get(0).getProcId());
    assertEquals(2, corruptedProcs.get(1).getProcId());
    assertEquals(3, corruptedProcs.get(2).getProcId());
  }

  @Test
  public void testDuplicatedStackId() throws IOException {
    TestProcedure proc0 = createProc(1, Procedure.NO_PROC_ID);
    proc0.addStackIndex(0);
    TestProcedure proc1 = createProc(2, 1);
    proc1.addStackIndex(1);
    TestProcedure proc2 = createProc(3, 2);
    proc2.addStackIndex(1);
    ProcedureTree tree = ProcedureTree.build(toProtos(proc0, proc1, proc2));
    List<TestProcedure> validProcs = getProcs(tree.getValidProcs());
    assertEquals(0, validProcs.size());
    List<TestProcedure> corruptedProcs = getProcs(tree.getCorruptedProcs());
    assertEquals(3, corruptedProcs.size());
    assertEquals(1, corruptedProcs.get(0).getProcId());
    assertEquals(2, corruptedProcs.get(1).getProcId());
    assertEquals(3, corruptedProcs.get(2).getProcId());
  }

  @Test
  public void testOrphan() throws IOException {
    TestProcedure proc0 = createProc(1, Procedure.NO_PROC_ID);
    proc0.addStackIndex(0);
    TestProcedure proc1 = createProc(2, 1);
    proc1.addStackIndex(1);
    TestProcedure proc2 = createProc(3, Procedure.NO_PROC_ID);
    proc2.addStackIndex(0);
    TestProcedure proc3 = createProc(5, 4);
    proc3.addStackIndex(1);
    ProcedureTree tree = ProcedureTree.build(toProtos(proc0, proc1, proc2, proc3));
    List<TestProcedure> validProcs = getProcs(tree.getValidProcs());
    assertEquals(3, validProcs.size());
    List<TestProcedure> corruptedProcs = getProcs(tree.getCorruptedProcs());
    assertEquals(1, corruptedProcs.size());
    assertEquals(5, corruptedProcs.get(0).getProcId());
    assertEquals(4, corruptedProcs.get(0).getParentProcId());
  }

}
