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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;

/**
 * An interface which abstract away the action taken to enable or disable
 * a space quota violation policy across the HBase cluster. Implementations
 * must have a no-args constructor.
 */
@InterfaceAudience.Private
public interface SpaceQuotaViolationNotifier {

  /**
   * Initializes the notifier.
   */
  void initialize(Connection conn);

  /**
   * Instructs the cluster that the given table is in violation of a space quota. The
   * provided violation policy is the action which should be taken on the table.
   *
   * @param tableName The name of the table in violation of the quota.
   * @param violationPolicy The policy which should be enacted on the table.
   */
  void transitionTableToViolation(
      TableName tableName, SpaceViolationPolicy violationPolicy) throws IOException;

  /**
   * Instructs the cluster that the given table is in observance of any applicable space quota.
   *
   * @param tableName The name of the table in observance.
   */
  void transitionTableToObservance(TableName tableName) throws IOException;
}
