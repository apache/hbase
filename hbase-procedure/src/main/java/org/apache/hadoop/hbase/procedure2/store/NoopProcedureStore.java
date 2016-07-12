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

import org.apache.hadoop.hbase.procedure2.Procedure;

/**
 * An In-Memory store that does not keep track of the procedures inserted.
 */
public class NoopProcedureStore extends ProcedureStoreBase {
  private int numThreads;

  @Override
  public void start(int numThreads) throws IOException {
    if (!setRunning(true)) {
      return;
    }
    this.numThreads = numThreads;
  }

  @Override
  public void stop(boolean abort) {
    setRunning(false);
  }

  @Override
  public void recoverLease() throws IOException {
    // no-op
  }

  @Override
  public int getNumThreads() {
    return numThreads;
  }

  @Override
  public void load(final ProcedureLoader loader) throws IOException {
    loader.setMaxProcId(0);
  }

  @Override
  public void insert(Procedure proc, Procedure[] subprocs) {
    // no-op
  }

  @Override
  public void update(Procedure proc) {
    // no-op
  }

  @Override
  public void delete(long procId) {
    // no-op
  }

  @Override
  public void delete(Procedure proc, long[] subprocs) {
    // no-op
  }
}
