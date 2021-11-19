/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * BrokenStoreFileCleaner metrics for a specific table in a RegionServer.
 */
@InterfaceAudience.Private
public interface MetricsMasterBrokenStoreFileCleaner {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "BrokenStoreFileCleaner";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * Description
   */
  String METRICS_DESCRIPTION =
    "Metrics about BrokenStoreFileCleaner results on a single HBase RegionServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;


  String DELETES = "BrokenStoreFileCleanerDeletes";
  String DELETES_DESC = "Number of files deleted by BrokenStoreFileCleaner";
  String FAILED_DELETES = "BrokenStoreFileCleanerFailedDeletes";
  String FAILED_DELETES_DESC =
    "Number of files BrokenStoreFileCleaner tried but failed to delete";
  String RUNS = "BrokenStoreFileCleanerRuns";
  String RUNS_DESC = "Number of time the BrokenStoreFileCleaner chore run";
  String RUNTIME = "BrokenStoreFileCleanerRuntime";
  String RUNTIME_DESC = "Time required to run BrokenStoreFileCleaner chore in milliseconds";

  /**
   * Increment the deleted files counter
   * @param deletes
   */
  public void incrementBrokenStoreFileCleanerDeletes(long deletes);

  /**
   * Increment the failed file deletes counter
   * @param failedDeletes
   */
  public void incrementBrokenStoreFileCleanerFailedDeletes(long failedDeletes);

  /**
   * Increment the number of cleaner runs counter
   */
  public void incrementBrokenStoreFileCleanerRuns(long runs);

  /**
   * Update the chore runtime
   * @param milis
   */
  public void updateBrokenStoreFileCleanerTimer(long milis);

}
