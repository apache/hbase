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

package org.apache.hadoop.hbase.procedure2;

import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * With this interface, the procedure framework provides means to collect following set of metrics
 * per procedure type for all procedures:
 * <ul>
 *   <li>Count of submitted procedure instances</li>
 *   <li>Time histogram for successfully completed procedure instances</li>
 *   <li>Count of failed procedure instances</li>
 *  </ul>
 *
 *  Please implement this interface to return appropriate metrics.
 */
@InterfaceAudience.Private
public interface ProcedureMetrics {
  /**
   * @return Total number of instances submitted for a type of a procedure
   */
  Counter getSubmittedCounter();

  /**
   * @return Histogram of runtimes for all successfully completed instances of a type of a procedure
   */
  Histogram getTimeHisto();

  /**
   * @return Total number of instances failed for a type of a procedure
   */
  Counter getFailedCounter();
}
