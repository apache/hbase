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
package org.apache.hadoop.hbase.server.snapshot.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * General snapshot operation taken on a regionserver
 */
public abstract class SnapshotTask implements ExceptionCheckable<HBaseSnapshotException>, Runnable {

  private static final Log LOG = LogFactory.getLog(SnapshotTask.class);

  private final SnapshotExceptionSnare errorMonitor;
  private final String desc;

  protected final SnapshotDescription snapshot;

  /**
   * @param snapshot Description of the snapshot we are going to operate on
   * @param monitor listener interested in failures to the snapshot caused by this operation
   * @param description description of the task being run, for logging
   */
  public SnapshotTask(SnapshotDescription snapshot, SnapshotExceptionSnare monitor,
      String description) {
    this.snapshot = snapshot;
    this.errorMonitor = monitor;
    this.desc = description;
  }

  protected final void snapshotFailure(String message, Exception e) {
    this.errorMonitor.snapshotFailure(message, this.snapshot, e);
  }

  @Override
  public void failOnError() throws HBaseSnapshotException {
    this.errorMonitor.failOnError();
  }

  @Override
  public boolean checkForError() {
    return this.errorMonitor.checkForError();
  }

  @Override
  public void run() {
    try {
      LOG.debug("Running: " + desc);
      this.process();
    } catch (Exception e) {
      this.snapshotFailure("Failed to run " + this.desc, e);
    }
  }

  /**
   * Run the task for the snapshot.
   * @throws Exception if the task fails. Will be propagated to any other tasks watching the same
   *           {@link SnapshotErrorListener}.
   */
  protected abstract void process() throws Exception;
}
