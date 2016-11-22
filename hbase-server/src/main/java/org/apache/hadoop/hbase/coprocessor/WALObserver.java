/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * It's provided to have a way for coprocessors to observe, rewrite,
 * or skip WALEdits as they are being written to the WAL.
 *
 * Note that implementers of WALObserver will not see WALEdits that report themselves
 * as empty via {@link WALEdit#isEmpty()}.
 *
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} provides
 * hooks for adding logic for WALEdits in the region context during reconstruction,
 *
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.wal.WAL}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface WALObserver extends Coprocessor {

  /**
   * Called before a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * @return true if default behavior should be bypassed, false otherwise
   */
  // TODO: return value is not used
  boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException;

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   */
  void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException;

  /**
   * Called before rolling the current WAL
   * @param oldPath the path of the current wal that we are replacing
   * @param newPath the path of the wal we are going to create
   */
  void preWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException;

  /**
   * Called after rolling the current WAL
   * @param oldPath the path of the wal that we replaced
   * @param newPath the path of the wal we have created and now is the current
   */
  void postWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException;
}

