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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Get notification of {@link FSHLog}/WAL log events. The invocations are inline
 * so make sure your implementation is fast else you'll slow hbase.
 */
@InterfaceAudience.Private
public interface WALActionsListener {

  /**
   * The WAL is going to be rolled. The oldPath can be null if this is
   * the first log file from the regionserver.
   * @param oldPath the path to the old hlog
   * @param newPath the path to the new hlog
   */
  void preLogRoll(Path oldPath, Path newPath) throws IOException;

  /**
   * The WAL has been rolled. The oldPath can be null if this is
   * the first log file from the regionserver.
   * @param oldPath the path to the old hlog
   * @param newPath the path to the new hlog
   */
  void postLogRoll(Path oldPath, Path newPath) throws IOException;

  /**
   * The WAL is going to be archived.
   * @param oldPath the path to the old hlog
   * @param newPath the path to the new hlog
   */
  void preLogArchive(Path oldPath, Path newPath) throws IOException;

  /**
   * The WAL has been archived.
   * @param oldPath the path to the old hlog
   * @param newPath the path to the new hlog
   */
  void postLogArchive(Path oldPath, Path newPath) throws IOException;

  /**
   * A request was made that the WAL be rolled.
   * @param tooFewReplicas roll requested because of too few replicas if true
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
    HRegionInfo info, HLogKey logKey, WALEdit logEdit
  );

  /**
   *
   * @param htd
   * @param logKey
   * @param logEdit
   */
  void visitLogEntryBeforeWrite(
    HTableDescriptor htd, HLogKey logKey, WALEdit logEdit
  );
}
