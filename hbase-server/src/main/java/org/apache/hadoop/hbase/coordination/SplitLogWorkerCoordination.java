 /**
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
package org.apache.hadoop.hbase.coordination;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor;

import com.google.common.annotations.VisibleForTesting;

/**
 * Coordinated operations for {@link SplitLogWorker} and 
 * {@link org.apache.hadoop.hbase.regionserver.handler.HLogSplitterHandler} Important
 * methods for SplitLogWorker: <BR>
 * {@link #isReady()} called from {@link SplitLogWorker#run()} to check whether the coordination is
 * ready to supply the tasks <BR>
 * {@link #taskLoop()} loop for new tasks until the worker is stopped <BR>
 * {@link #isStop()} a flag indicates whether worker should finish <BR>
 * {@link #registerListener()} called from {@link SplitLogWorker#run()} and could register listener
 * for external changes in coordination (if required) <BR>
 * {@link #endTask(SplitLogTask, AtomicLong, SplitTaskDetails)} notify coordination engine that
 * <p>
 * Important methods for WALSplitterHandler: <BR>
 * splitting task has completed.
 */
@InterfaceAudience.Private
public interface SplitLogWorkerCoordination {

/* SplitLogWorker part */
  public static final int DEFAULT_MAX_SPLITTERS = 2;

  /**
   * Initialize internal values. This method should be used when corresponding SplitLogWorker
   * instance is created
   * @param server instance of RegionServerServices to work with
   * @param conf is current configuration.
   * @param splitTaskExecutor split executor from SplitLogWorker
   * @param worker instance of SplitLogWorker
   */
  void init(RegionServerServices server, Configuration conf,
      TaskExecutor splitTaskExecutor, SplitLogWorker worker);

  /**
   *  called when Coordination should stop processing tasks and exit
   */
  void stopProcessingTasks();

  /**
   * @return the current value of exitWorker
   */
  boolean isStop();

  /**
   * Wait for the new tasks and grab one
   * @throws InterruptedException if the SplitLogWorker was stopped
   */
  void taskLoop() throws InterruptedException;

  /**
   * marks log file as corrupted
   * @param rootDir where to find the log
   * @param name of the log
   * @param fs file system
   */
  void markCorrupted(Path rootDir, String name, FileSystem fs);

  /**
   * Check whether the log splitter is ready to supply tasks
   * @return false if there is no tasks
   * @throws InterruptedException if the SplitLogWorker was stopped
   */
  boolean isReady() throws InterruptedException;

  /**
   * Used by unit tests to check how many tasks were processed
   * @return number of tasks
   */
  @VisibleForTesting
  int getTaskReadySeq();

  /**
   * set the listener for task changes. Implementation specific
   */
  void registerListener();

  /**
   * remove the listener for task changes. Implementation specific
   */
  void removeListener();

  /* WALSplitterHandler part */

  /**
   * Notify coordination engine that splitting task has completed.
   * @param slt See {@link SplitLogTask}
   * @param ctr counter to be updated
   * @param splitTaskDetails details about log split task (specific to coordination engine being
   *          used).
   */
  void endTask(SplitLogTask slt, AtomicLong ctr, SplitTaskDetails splitTaskDetails);

  /**
   * Interface for log-split tasks Used to carry implementation details in encapsulated way through
   * Handlers to the coordination API.
   */
  static interface SplitTaskDetails {

    /**
     * @return full file path in HDFS for the WAL file to be split.
     */
    String getWALFile();
  }

  RegionStoreSequenceIds getRegionFlushedSequenceId(String failedServerName, String key)
      throws IOException;

}
