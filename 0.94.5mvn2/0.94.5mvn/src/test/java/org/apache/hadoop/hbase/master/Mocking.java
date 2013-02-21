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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;

/**
 * Package scoped mocking utility.
 */
public class Mocking {

  static void waitForRegionPendingOpenInRIT(AssignmentManager am, String encodedName)
    throws InterruptedException {
    // We used to do a check like this:
    //!Mocking.verifyRegionState(this.watcher, REGIONINFO, EventType.M_ZK_REGION_OFFLINE)) {
    // There is a race condition with this: because we may do the transition to
    // RS_ZK_REGION_OPENING before the RIT is internally updated. We need to wait for the
    // RIT to be as we need it to be instead. This cannot happen in a real cluster as we
    // update the RIT before sending the openRegion request.

    boolean wait = true;
    while (wait) {
      RegionState state = am.getRegionsInTransition().get(encodedName);
      if (state != null && state.isPendingOpen()){
        wait = false;
      } else {
        Thread.sleep(1);
      }
    }
  }
}
