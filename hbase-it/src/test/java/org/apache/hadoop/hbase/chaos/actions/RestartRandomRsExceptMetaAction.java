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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartRandomRsExceptMetaAction extends RestartRandomRsAction {
  private static final Logger LOG = LoggerFactory.getLogger(RestartRandomRsExceptMetaAction.class);

  public RestartRandomRsExceptMetaAction(long sleepTime) {
    super(sleepTime);
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    int tries = 10;

    while (tries-- > 0 && getCurrentServers().length > 1) {
      ServerName server = PolicyBasedChaosMonkey.selectRandomItem(getCurrentServers());
      ServerName metaServer = cluster.getServerHoldingMeta();
      if (server != null && !server.equals(metaServer)) {
        restartRs(server, sleepTime);
        break;
      }
    }
  }
}
