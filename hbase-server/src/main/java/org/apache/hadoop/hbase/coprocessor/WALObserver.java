/*
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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * It's provided to have a way for coprocessors to observe, rewrite, or skip WALEdits as they are
 * being written to the WAL. Note that implementers of WALObserver will not see WALEdits that report
 * themselves as empty via {@link WALEdit#isEmpty()}.
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} provides hooks for adding logic for
 * WALEdits in the region context during reconstruction. Defines coprocessor hooks for interacting
 * with operations on the {@link org.apache.hadoop.hbase.wal.WAL}. Since most implementations will
 * be interested in only a subset of hooks, this class uses 'default' functions to avoid having to
 * add unnecessary overrides. When the functions are non-empty, it's simply to satisfy the compiler
 * by returning value of expected (non-void) type. It is done in a way that these default
 * definitions act as no-op. So our suggestion to implementation would be to not call these
 * 'default' methods from overrides. <br>
 * <br>
 * <h3>Exception Handling</h3> For all functions, exception handling is done as follows:
 * <ul>
 * <li>Exceptions of type {@link IOException} are reported back to client.</li>
 * <li>For any other kind of exception:
 * <ul>
 * <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then the
 * server aborts.</li>
 * <li>Otherwise, coprocessor is removed from the server and
 * {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 * </ul>
 * </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface WALObserver {
  /**
   * Called before a {@link WALEdit} is writen to WAL.
   * <p>
   * The method is marked as deprecated in 2.0.0, but later we abstracted the WALKey interface for
   * coprocessors, now it is OK to expose this to coprocessor users, so we revert the deprecation.
   * But you still need to be careful while changing {@link WALEdit}, as when reaching here, if you
   * add some cells to WALEdit, it will only be written to WAL but no in memstore, but when
   * replaying you will get these cells and there are CP hooks to intercept these cells.
   * <p>
   * See HBASE-28580.
   */
  default void preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
    RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
  }

  /**
   * Called after a {@link WALEdit} is writen to WAL.
   * <p>
   * The method is marked as deprecated in 2.0.0, but later we abstracted the WALKey interface for
   * coprocessors, now it is OK to expose this to coprocessor users, so we revert the deprecation.
   * But you still need to be careful while changing {@link WALEdit}, as when reaching here, if you
   * add some cells to WALEdit, it will only be written to WAL but no in memstore, but when
   * replaying you will get these cells and there are CP hooks to intercept these cells.
   * <p>
   * See HBASE-28580.
   */
  default void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
    RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
  }

  /**
   * Called before rolling the current WAL
   * @param oldPath the path of the current wal that we are replacing
   * @param newPath the path of the wal we are going to create
   */
  default void preWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx, Path oldPath,
    Path newPath) throws IOException {
  }

  /**
   * Called after rolling the current WAL
   * @param oldPath the path of the wal that we replaced
   * @param newPath the path of the wal we have created and now is the current
   */
  default void postWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx, Path oldPath,
    Path newPath) throws IOException {
  }
}
