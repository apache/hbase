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

package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsReplicationSourceSource extends BaseSource {

  public static final String SOURCE_SIZE_OF_LOG_QUEUE = "source.sizeOfLogQueue";
  public static final String SOURCE_AGE_OF_LAST_SHIPPED_OP = "source.ageOfLastShippedOp";
  public static final String SOURCE_SHIPPED_BATCHES = "source.shippedBatches";
  public static final String SOURCE_FAILED_BATCHES = "source.failedBatches";

  @Deprecated
  /** @deprecated Use SOURCE_SHIPPED_BYTES instead */
  public static final String SOURCE_SHIPPED_KBS = "source.shippedKBs";
  public static final String SOURCE_SHIPPED_BYTES = "source.shippedBytes";
  public static final String SOURCE_SHIPPED_OPS = "source.shippedOps";

  public static final String SOURCE_LOG_READ_IN_BYTES = "source.logReadInBytes";
  public static final String SOURCE_LOG_READ_IN_EDITS = "source.logEditsRead";

  public static final String SOURCE_LOG_EDITS_FILTERED = "source.logEditsFiltered";

  public static final String SOURCE_SHIPPED_HFILES = "source.shippedHFiles";
  public static final String SOURCE_SIZE_OF_HFILE_REFS_QUEUE = "source.sizeOfHFileRefsQueue";

  public static final String SOURCE_CLOSED_LOGS_WITH_UNKNOWN_LENGTH =
      "source.closedLogsWithUnknownFileLength";
  public static final String SOURCE_UNCLEANLY_CLOSED_LOGS = "source.uncleanlyClosedLogs";
  public static final String SOURCE_UNCLEANLY_CLOSED_IGNORED_IN_BYTES =
      "source.ignoredUncleanlyClosedLogContentsInBytes";
  public static final String SOURCE_RESTARTED_LOG_READING = "source.restartedLogReading";
  public static final String SOURCE_REPEATED_LOG_FILE_BYTES = "source.repeatedLogFileBytes";
  public static final String SOURCE_COMPLETED_LOGS = "source.completedLogs";
  public static final String SOURCE_COMPLETED_RECOVERY_QUEUES = "source.completedRecoverQueues";
  public static final String SOURCE_FAILED_RECOVERY_QUEUES = "source.failedRecoverQueues";
  // This is to track the num of replication sources getting initialized
  public static final String SOURCE_INITIALIZING = "source.numInitializing";

  void setLastShippedAge(long age);
  void incrSizeOfLogQueue(int size);
  void decrSizeOfLogQueue(int size);
  void incrLogEditsFiltered(long size);
  void incrBatchesShipped(int batches);
  void incrFailedBatches();
  void incrOpsShipped(long ops);
  void incrShippedBytes(long size);
  void incrLogReadInBytes(long size);
  void incrLogReadInEdits(long size);
  void clear();
  long getLastShippedAge();
  int getSizeOfLogQueue();
  void incrHFilesShipped(long hfiles);
  void incrSizeOfHFileRefsQueue(long size);
  void decrSizeOfHFileRefsQueue(long size);
  void incrUnknownFileLengthForClosedWAL();
  void incrUncleanlyClosedWALs();
  long getUncleanlyClosedWALs();
  void incrBytesSkippedInUncleanlyClosedWALs(final long bytes);
  void incrRestartedWALReading();
  void incrRepeatedFileBytes(final long bytes);
  void incrCompletedWAL();
  void incrCompletedRecoveryQueue();
  void incrFailedRecoveryQueue();
  long getWALEditsRead();
  long getShippedOps();
  long getEditsFiltered();
  void setOldestWalAge(long age);
  long getOldestWalAge();
  void incrSourceInitializing();
  void decrSourceInitializing();
  int getSourceInitializing();
}
