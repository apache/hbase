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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * Get notification of WAL events. The invocations are inline
 * so make sure your implementation is fast else you'll slow hbase.
 */
@InterfaceAudience.Private
public interface WALActionsListener {

  /**
   * The WAL is going to be rolled. The oldPath can be null if this is
   * the first log file from the regionserver.
   * @param oldPath the path to the old wal
   * @param newPath the path to the new wal
   */
  void preLogRoll(Path oldPath, Path newPath) throws IOException;

  /**
   * The WAL has been rolled. The oldPath can be null if this is
   * the first log file from the regionserver.
   * @param oldPath the path to the old wal
   * @param newPath the path to the new wal
   */
  void postLogRoll(Path oldPath, Path newPath) throws IOException;

  /**
   * The WAL is going to be archived.
   * @param oldPath the path to the old wal
   * @param newPath the path to the new wal
   */
  void preLogArchive(Path oldPath, Path newPath) throws IOException;

  /**
   * The WAL has been archived.
   * @param oldPath the path to the old wal
   * @param newPath the path to the new wal
   */
  void postLogArchive(Path oldPath, Path newPath) throws IOException;

  /**
   * A request was made that the WAL be rolled.
   */
  void logRollRequested(boolean tooFewReplicas);

  /**
   * The WAL is about to close.
   */
  void logCloseRequested();

  /**
  * Called before each write.
  * @param info
  * @param logKey
  * @param logEdit
  */
  void visitLogEntryBeforeWrite(
    HRegionInfo info, WALKey logKey, WALEdit logEdit
  );

  /**
   *
   * @param htd
   * @param logKey
   * @param logEdit
   * TODO: Retire this in favor of {@link #visitLogEntryBeforeWrite(HRegionInfo, WALKey, WALEdit)}
   * It only exists to get scope when replicating.  Scope should be in the WALKey and not need
   * us passing in a <code>htd</code>.
   */
  void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey,
      WALEdit logEdit) throws IOException;

  /**
   * For notification post append to the writer.  Used by metrics system at least.
   * TODO: Combine this with above.
   * @param entryLen approx length of cells in this append.
   * @param elapsedTimeMillis elapsed time in milliseconds.
   */
  void postAppend(final long entryLen, final long elapsedTimeMillis);

  /**
   * For notification post writer sync.  Used by metrics system at least.
   * @param timeInNanos How long the filesystem sync took in nanoseconds.
   * @param handlerSyncs How many sync handler calls were released by this call to filesystem
   * sync.
   */
  void postSync(final long timeInNanos, final int handlerSyncs);

  static class Base implements WALActionsListener {
    @Override
    public void preLogRoll(Path oldPath, Path newPath) throws IOException {}

    @Override
    public void postLogRoll(Path oldPath, Path newPath) throws IOException {}

    @Override
    public void preLogArchive(Path oldPath, Path newPath) throws IOException {}

    @Override
    public void postLogArchive(Path oldPath, Path newPath) throws IOException {}

    @Override
    public void logRollRequested(boolean tooFewReplicas) {}

    @Override
    public void logCloseRequested() {}

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, WALKey logKey, WALEdit logEdit) {}

    @Override
    public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey,
        WALEdit logEdit) throws IOException {
    }

    @Override
    public void postAppend(final long entryLen, final long elapsedTimeMillis) {}

    @Override
    public void postSync(final long timeInNanos, final int handlerSyncs) {}
  }
}
