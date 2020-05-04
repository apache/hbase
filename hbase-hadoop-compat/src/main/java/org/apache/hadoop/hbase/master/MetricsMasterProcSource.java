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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that classes that expose metrics about the master will implement.
 */
@InterfaceAudience.Private
public interface MetricsMasterProcSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Procedure";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase master procedure";

  // Strings used for exporting to metrics system.
  String NUM_MASTER_WALS_NAME = "numMasterWALs";

  String NUM_MASTER_WALS_DESC = "Number of master WAL files";

  String NUM_SPLIT_PROCEDURE_REQUEST_NAME = "splitProcedure_RequestCount";

  String NUM_SPLIT_PROCEDURE_REQUEST_DESC = "Number of split requests";

  String NUM_SPLIT_PROCEDURE_FAILED_NAME = "splitProcedure_FailedCount";

  String NUM_SPLIT_PROCEDURE_FAILED_DESC = "Number of split requests which failed";

  String NUM_SPLIT_PROCEDURE_SUCCESS_NAME = "splitProcedure_SuccessCount";

  String NUM_SPLIT_PROCEDURE_SUCCESS_DESC = "Number of split requests which were successful";

  String SPLIT_PROCEDURE_TIME_HISTO_NAME = "splitProcedureTime";

  String SPLIT_PROCEDURE_TIME_HISTO_DESC = "Split procedure time histogram";
}
