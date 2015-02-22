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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

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
   * Called before a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * This method is left in place to maintain binary compatibility with older
   * {@link WALObserver}s. If an implementation directly overrides
   * {@link #preWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)} then this version
   * won't be called at all, barring problems with the Security Manager. To work correctly
   * in the presence of a strict Security Manager, or in the case of an implementation that
   * relies on a parent class to implement preWALWrite, you should implement this method
   * as a call to the non-deprecated version.
   *
   * Users of this method will see all edits that can be treated as HLogKey. If there are
   * edits that can't be treated as HLogKey they won't be offered to coprocessors that rely
   * on this method. If a coprocessor gets skipped because of this mechanism, a log message
   * at ERROR will be generated per coprocessor on the logger for {@link CoprocessorHost} once per
   * classloader.
   *
   * @return true if default behavior should be bypassed, false otherwise
   * @deprecated use {@link #preWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)}
   */
  @Deprecated
  boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException;

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   */
  void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException;

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * This method is left in place to maintain binary compatibility with older
   * {@link WALObserver}s. If an implementation directly overrides
   * {@link #postWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)} then this version
   * won't be called at all, barring problems with the Security Manager. To work correctly
   * in the presence of a strict Security Manager, or in the case of an implementation that
   * relies on a parent class to implement preWALWrite, you should implement this method
   * as a call to the non-deprecated version.
   *
   * Users of this method will see all edits that can be treated as HLogKey. If there are
   * edits that can't be treated as HLogKey they won't be offered to coprocessors that rely
   * on this method. If a coprocessor gets skipped because of this mechanism, a log message
   * at ERROR will be generated per coprocessor on the logger for {@link CoprocessorHost} once per
   * classloader.
   *
   * @deprecated use {@link #postWALWrite(ObserverContext, HRegionInfo, WALKey, WALEdit)}
   */
  @Deprecated
  void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException;
}
