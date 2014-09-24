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
package org.apache.hadoop.hbase.coordination;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Coordinated operations for close region handlers.
 */
@InterfaceAudience.Private
public interface CloseRegionCoordination {

  /**
   * Called before actual region closing to check that we can do close operation
   * on this region.
   * @param regionInfo region being closed
   * @param crd details about closing operation
   * @return true if caller shall proceed and close, false if need to abort closing.
   */
  boolean checkClosingState(HRegionInfo regionInfo, CloseRegionDetails crd);

  /**
   * Called after region is closed to notify all interesting parties / "register"
   * region as finally closed.
   * @param region region being closed
   * @param sn ServerName on which task runs
   * @param crd details about closing operation
   */
  void setClosedState(HRegion region, ServerName sn, CloseRegionDetails crd);

  /**
   * Construct CloseRegionDetails instance from CloseRegionRequest.
   * @return instance of CloseRegionDetails
   */
  CloseRegionDetails parseFromProtoRequest(AdminProtos.CloseRegionRequest request);

  /**
   * Get details object with params for case when we're closing on
   * regionserver side internally (not because of RPC call from master),
   * so we don't parse details from protobuf request.
   */
  CloseRegionDetails getDetaultDetails();

  /**
   * Marker interface for region closing tasks. Used to carry implementation details in
   * encapsulated way through Handlers to the consensus API.
   */
  static interface CloseRegionDetails {
  }
}
