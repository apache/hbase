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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Action that tries to restart the HRegionServer holding Meta.
 */
public class RestartRsHoldingMetaAction extends RestartActionBaseAction {
  public RestartRsHoldingMetaAction(long sleepTime) {
    super(sleepTime);
  }

  @Override
  public void perform() throws Exception {
    LOG.info("Performing action: Restart region server holding META");
    IntegrationTestingUtility util = context.getHBaseIntegrationTestingUtility();
    HBaseAdmin admin = util.getHBaseAdmin();
    final HConnection connection = admin.getConnection();
    HServerAddress server =
        connection.locateRegion(HConstants.META_TABLE_NAME_STRINGBYTES, null).getServerAddress();

    if (server == null) {

      LOG.warn("No server is holding meta right now.");
      return;
    }
    restartRs(server, sleepTime);
  }
}
