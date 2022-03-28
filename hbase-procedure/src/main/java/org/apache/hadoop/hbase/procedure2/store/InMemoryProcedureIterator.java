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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A procedure iterator which holds all the procedure protos in memory. For fast access.
 */
@InterfaceAudience.Private
public class InMemoryProcedureIterator implements ProcedureIterator {

  private final List<ProtoAndProcedure> procs;

  private Iterator<ProtoAndProcedure> iter;

  private ProtoAndProcedure current;

  public InMemoryProcedureIterator(List<ProtoAndProcedure> procs) {
    this.procs = procs;
    reset();
  }

  @Override
  public void reset() {
    iter = procs.iterator();
    if (iter.hasNext()) {
      current = iter.next();
    } else {
      current = null;
    }
  }

  @Override
  public boolean hasNext() {
    return current != null;
  }

  private void checkNext() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
  }

  @Override
  public boolean isNextFinished() {
    checkNext();
    return ProcedureUtil.isFinished(current.getProto());
  }

  private void moveToNext() {
    if (iter.hasNext()) {
      current = iter.next();
    } else {
      current = null;
    }
  }

  @Override
  public void skipNext() {
    checkNext();
    moveToNext();
  }

  @Override
  public Procedure<?> next() throws IOException {
    checkNext();
    Procedure<?> proc = current.getProcedure();
    moveToNext();
    return proc;
  }
}