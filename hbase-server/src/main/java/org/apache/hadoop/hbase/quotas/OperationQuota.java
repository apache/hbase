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

package org.apache.hadoop.hbase.quotas;

import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

/**
 * Interface that allows to check the quota available for an operation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface OperationQuota {
  public enum OperationType { MUTATE, GET, SCAN }

  /**
   * Checks if it is possible to execute the specified operation.
   * The quota will be estimated based on the number of operations to perform
   * and the average size accumulated during time.
   *
   * @param numWrites number of write operation that will be performed
   * @param numReads number of small-read operation that will be performed
   * @param numScans number of long-read operation that will be performed
   * @throws ThrottlingException if the operation cannot be performed
   */
  void checkQuota(int numWrites, int numReads, int numScans)
    throws ThrottlingException;

  /** Cleanup method on operation completion */
  void close();

  /**
   * Add a get result. This will be used to calculate the exact quota and
   * have a better short-read average size for the next time.
   */
  void addGetResult(Result result);

  /**
   * Add a scan result. This will be used to calculate the exact quota and
   * have a better long-read average size for the next time.
   */
  void addScanResult(List<Result> results);

  /**
   * Add a mutation result. This will be used to calculate the exact quota and
   * have a better mutation average size for the next time.
   */
  void addMutation(Mutation mutation);

  /** @return the number of bytes available to read to avoid exceeding the quota */
  long getReadAvailable();

  /** @return the number of bytes available to write to avoid exceeding the quota */
  long getWriteAvailable();
}
