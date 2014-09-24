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
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import java.io.IOException;

/**
 * Cocoordination operations for opening regions.
 */
@InterfaceAudience.Private
public interface OpenRegionCoordination {

  //---------------------
  // RS-side operations
  //---------------------
  /**
   * Tries to move regions to OPENED state.
   *
   * @param r Region we're working on.
   * @param ord details about region opening task
   * @return whether transition was successful or not
   * @throws java.io.IOException
   */
  boolean transitionToOpened(HRegion r, OpenRegionDetails ord) throws IOException;

  /**
   * Transitions region from offline to opening state.
   * @param regionInfo region we're working on.
   * @param ord details about opening task.
   * @return true if successful, false otherwise
   */
  boolean transitionFromOfflineToOpening(HRegionInfo regionInfo,
                                         OpenRegionDetails ord);

  /**
   * Heartbeats to prevent timeouts.
   *
   * @param ord details about opening task.
   * @param regionInfo region we're working on.
   * @param rsServices instance of RegionServerrServices
   * @param context used for logging purposes only
   * @return true if successful heartbeat, false otherwise.
   */
  boolean tickleOpening(OpenRegionDetails ord, HRegionInfo regionInfo,
                        RegionServerServices rsServices, String context);

  /**
   * Tries transition region from offline to failed open.
   * @param rsServices instance of RegionServerServices
   * @param hri region we're working on
   * @param ord details about region opening task
   * @return true if successful, false otherwise
   */
  boolean tryTransitionFromOfflineToFailedOpen(RegionServerServices rsServices,
                                               HRegionInfo hri, OpenRegionDetails ord);

  /**
   * Tries transition from Opening to Failed open.
   * @param hri region we're working on
   * @param ord details about region opening task
   * @return true if successfu. false otherwise.
   */
  boolean tryTransitionFromOpeningToFailedOpen(HRegionInfo hri, OpenRegionDetails ord);

  /**
   * Construct OpenRegionDetails instance from part of protobuf request.
   * @return instance of OpenRegionDetails.
   */
  OpenRegionDetails parseFromProtoRequest(AdminProtos.OpenRegionRequest.RegionOpenInfo
                                            regionOpenInfo);

  /**
   * Get details object with params for case when we're opening on
   * regionserver side with all "default" properties.
   */
  OpenRegionDetails getDetailsForNonCoordinatedOpening();

  //-------------------------
  // HMaster-side operations
  //-------------------------

  /**
   * Commits opening operation on HM side (steps required for "commit"
   * are determined by coordination implementation).
   * @return true if committed successfully, false otherwise.
   */
  public boolean commitOpenOnMasterSide(AssignmentManager assignmentManager,
                                        HRegionInfo regionInfo,
                                        OpenRegionDetails ord);

  /**
   * Interface for region opening tasks. Used to carry implementation details in
   * encapsulated way through Handlers to the coordination API.
   */
  static interface OpenRegionDetails {
    /**
     * Sets server name on which opening operation is running.
     */
    void setServerName(ServerName serverName);

    /**
     * @return server name on which opening op is running.
     */
    ServerName getServerName();
  }
}
