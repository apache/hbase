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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Catalog replica selector decides which catalog replica to go for read requests when it is
 * configured as CatalogReplicaMode.LoadBalance.
 */
@InterfaceAudience.Private
interface CatalogReplicaLoadBalanceSelector {

  int UNINITIALIZED_NUM_OF_REPLICAS = -1;

  /**
   * This method is called when input location is stale, i.e, when clients run into
   * org.apache.hadoop.hbase.NotServingRegionException.
   * @param loc stale location
   */
  void onError(HRegionLocation loc);

  /**
   * Select a catalog replica region where client go to loop up the input row key.
   *
   * @param tablename table name
   * @param row  key to look up
   * @param locateType  locate type
   * @return replica id
   */
  int select(TableName tablename, byte[] row, RegionLocateType locateType);
}
