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

package org.apache.hadoop.hbase.chaos.actions;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Action that restarts an HRegionServer holding one of the regions of the table.
*/
public class RestartRsHoldingTableAction extends RestartActionBaseAction {
  private static final Logger LOG = LoggerFactory.getLogger(RestartRsHoldingTableAction.class);

  private final RegionLocator locator;

  public RestartRsHoldingTableAction(long sleepTime, RegionLocator locator) {
    super(sleepTime);
    this.locator = locator;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info(
      "Performing action: Restart random RS holding table " + this.locator.getName());
    List<HRegionLocation> locations = locator.getAllRegionLocations();
    restartRs(locations.get(ThreadLocalRandom.current().nextInt(locations.size()))
        .getServerName(),
      sleepTime);
  }
}
