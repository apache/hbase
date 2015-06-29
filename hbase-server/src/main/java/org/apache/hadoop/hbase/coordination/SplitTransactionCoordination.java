/**
 *
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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * Coordination operations for split transaction. The split operation should be coordinated at the
 * following stages:
 * 1. start - all preparation/initialization for split transaction should be done there.
 * 2. waitForSplitTransaction  - the coordination should perform all logic related to split
 *    transaction and wait till it's finished
 * 3. completeSplitTransaction - all steps that are required to complete the transaction.
 *    Called after PONR (point of no return)
 */
@InterfaceAudience.Private
public interface SplitTransactionCoordination {

  /**
   * Dummy interface for split transaction details.
   */
  public static interface SplitTransactionDetails {
  }

  SplitTransactionDetails getDefaultDetails();


  /**
   * init coordination for split transaction
   * @param parent region to be created as offline
   * @param serverName server event originates from
   * @param hri_a daughter region
   * @param hri_b daughter region
   * @throws IOException
   */
  void startSplitTransaction(HRegion parent, ServerName serverName,
      HRegionInfo hri_a, HRegionInfo hri_b) throws IOException;

  /**
   * Wait while coordination process the transaction
   * @param services Used to online/offline regions.
   * @param parent region
   * @param hri_a daughter region
   * @param hri_b daughter region
   * @param std split transaction details
   * @throws IOException
   */
  void waitForSplitTransaction(final RegionServerServices services,
      Region parent, HRegionInfo hri_a, HRegionInfo hri_b, SplitTransactionDetails std)
      throws IOException;

  /**
   * Finish off split transaction
   * @param services Used to online/offline regions.
   * @param first daughter region
   * @param second daughter region
   * @param std split transaction details
   * @param parent
   * @throws IOException If thrown, transaction failed. Call
   *                     {@link org.apache.hadoop.hbase.regionserver.SplitTransaction#rollback(
   *                         Server, RegionServerServices)}
   */
  void completeSplitTransaction(RegionServerServices services, Region first,
      Region second, SplitTransactionDetails std, Region parent) throws IOException;

  /**
   * clean the split transaction
   * @param hri node to delete
   */
  void clean(final HRegionInfo hri);

  /**
   * Required by AssignmentManager
   */
  int processTransition(HRegionInfo p, HRegionInfo hri_a, HRegionInfo hri_b,
      ServerName sn, SplitTransactionDetails std) throws IOException;
}
