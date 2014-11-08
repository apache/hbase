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

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;

/**
* Action that restarts an HRegionServer holding one of the regions of the table.
*/
public class RestartRsHoldingTableAction extends RestartActionBaseAction {

  private final String tableName;

  public RestartRsHoldingTableAction(long sleepTime, String tableName) {
    super(sleepTime);
    this.tableName = tableName;
  }

  @Override
  public void perform() throws Exception {
    HTable table = null;
    try {
      LOG.info("Performing action: Restart random RS holding table " + this.tableName);
      Configuration conf = context.getHBaseIntegrationTestingUtility().getConfiguration();
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      LOG.debug("Error creating HTable used to get list of region locations.", e);
      return;
    }

    Collection<ServerName> serverNames = table.getRegionLocations().values();
    ServerName[] nameArray = serverNames.toArray(new ServerName[serverNames.size()]);

    restartRs(nameArray[RandomUtils.nextInt(nameArray.length)], sleepTime);
  }
}
