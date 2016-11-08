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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A SpaceQuotaViolationNotifier implementation for verifying testing.
 */
@InterfaceAudience.Private
public class SpaceQuotaViolationNotifierForTest implements SpaceQuotaViolationNotifier {

  private final Map<TableName,SpaceViolationPolicy> tablesInViolation = new HashMap<>();

  @Override
  public void transitionTableToViolation(TableName tableName, SpaceViolationPolicy violationPolicy) {
    tablesInViolation.put(tableName, violationPolicy);
  }

  @Override
  public void transitionTableToObservance(TableName tableName) {
    tablesInViolation.remove(tableName);
  }

  public Map<TableName,SpaceViolationPolicy> snapshotTablesInViolation() {
    return new HashMap<>(this.tablesInViolation);
  }

  public void clearTableViolations() {
    this.tablesInViolation.clear();
  }
}
