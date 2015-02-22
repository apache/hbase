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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;

/**
* Action that restarts an HRegionServer holding one of the regions of the table.
*/
public class RestartRsHoldingTableAction extends RestartActionBaseAction {

  private final RegionLocator locator;

  public RestartRsHoldingTableAction(long sleepTime, RegionLocator locator) {
    super(sleepTime);
    this.locator = locator;
  }

  @Override
  public void perform() throws Exception {
    LOG.info("Performing action: Restart random RS holding table " + this.locator.getName());

    List<HRegionLocation> locations = locator.getAllRegionLocations();
    restartRs(locations.get(RandomUtils.nextInt(locations.size())).getServerName(), sleepTime);
  }
}
