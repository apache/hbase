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
package org.apache.hadoop.hbase.client;

import java.util.function.IntSupplier;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory to create a {@link CatalogReplicaLoadBalanceSelector}
 */
@InterfaceAudience.Private
final class CatalogReplicaLoadBalanceSelectorFactory {
  /**
   * Private Constructor
   */
  private CatalogReplicaLoadBalanceSelectorFactory() {
  }

  /**
   * Create a CatalogReplicaLoadBalanceReplicaSelector based on input config.
   * @param replicaSelectorClass  Selector classname.
   * @param tableName  System table name.
   * @param choreService {@link ChoreService}
   * @return  {@link CatalogReplicaLoadBalanceSelector}
   */
  public static CatalogReplicaLoadBalanceSelector createSelector(String replicaSelectorClass,
    TableName tableName, ChoreService choreService, IntSupplier getReplicaCount) {
    return ReflectionUtils.instantiateWithCustomCtor(replicaSelectorClass,
      new Class[] { TableName.class, ChoreService.class, IntSupplier.class },
      new Object[] { tableName, choreService, getReplicaCount });
  }
}
