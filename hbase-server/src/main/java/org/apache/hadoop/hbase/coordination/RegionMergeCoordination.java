/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.coordination;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * Coordination operations for region merge transaction. The operation should be coordinated at the
 * following stages:<br>
 * 1. startRegionMergeTransaction - all preparation/initialization for merge region transaction<br>
 * 2. waitForRegionMergeTransaction - wait until coordination complete all works related 
 * to merge<br>
 * 3. confirmRegionMergeTransaction - confirm that the merge could be completed and none of merging
 * regions moved somehow<br>
 * 4. completeRegionMergeTransaction - all steps that are required to complete the transaction.
 * Called after PONR (point of no return) <br>
 */
@InterfaceAudience.Private
public interface RegionMergeCoordination {

  RegionMergeDetails getDefaultDetails();

  /**
   * Dummy interface for region merge transaction details.
   */
  public static interface RegionMergeDetails {
  }

  /**
   * Start the region merge transaction
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @throws IOException
   */
  void startRegionMergeTransaction(HRegionInfo region, ServerName serverName, HRegionInfo a,
      HRegionInfo b) throws IOException;

  /**
   * Get everything ready for region merge
   * @throws IOException
   */
  void waitForRegionMergeTransaction(RegionServerServices services, HRegionInfo mergedRegionInfo,
      HRegion region_a, HRegion region_b, RegionMergeDetails details) throws IOException;

  /**
   * Confirm that the region merge can be performed
   * @param merged region
   * @param a merging region A
   * @param b merging region B
   * @param serverName server event originates from
   * @param rmd region merge details
   * @throws IOException If thrown, transaction failed.
   */
  void confirmRegionMergeTransaction(HRegionInfo merged, HRegionInfo a, HRegionInfo b,
      ServerName serverName, RegionMergeDetails rmd) throws IOException;

  /**
   * @param merged region
   * @param a merging region A
   * @param b merging region B
   * @param serverName server event originates from
   * @param rmd region merge details
   * @throws IOException
   */
  void processRegionMergeRequest(HRegionInfo merged, HRegionInfo a, HRegionInfo b,
      ServerName serverName, RegionMergeDetails rmd) throws IOException;

  /**
   * Finish off merge transaction
   * @param services Used to online/offline regions.
   * @param merged region
   * @param region_a merging region A
   * @param region_b merging region B
   * @param rmd region merge details
   * @param mergedRegion
   * @throws IOException If thrown, transaction failed. Call
   *  {@link org.apache.hadoop.hbase.regionserver.RegionMergeTransaction#rollback(
   *  Server, RegionServerServices)}
   */
  void completeRegionMergeTransaction(RegionServerServices services, HRegionInfo merged,
      HRegion region_a, HRegion region_b, RegionMergeDetails rmd, HRegion mergedRegion)
      throws IOException;

  /**
   * This method is used during rollback
   * @param merged region to be rolled back
   */
  void clean(HRegionInfo merged);

}
