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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The listener interface for receiving region state events.
 */
@InterfaceAudience.Private
public interface RegionStateListener {
// TODO: Get rid of this!!!! Ain't there a better way to watch region
// state than introduce a whole new listening mechanism? St.Ack
  /**
   * Process region split event.
   *
   * @param hri An instance of RegionInfo
   * @throws IOException
   */
  void onRegionSplit(RegionInfo hri) throws IOException;

  /**
   * Process region split reverted event.
   *
   * @param hri An instance of RegionInfo
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void onRegionSplitReverted(RegionInfo hri) throws IOException;

  /**
   * Process region merge event.
   * @throws IOException
   */
  void onRegionMerged(RegionInfo mergedRegion) throws IOException;
}
