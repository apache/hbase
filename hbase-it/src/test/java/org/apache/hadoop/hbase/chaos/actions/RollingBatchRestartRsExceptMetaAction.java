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

import java.util.List;

import org.apache.hadoop.hbase.ServerName;

/**
 * Same as in {@link RollingBatchRestartRsAction} except that this action
 * does not restart the region server holding the META table.
 */
public class RollingBatchRestartRsExceptMetaAction extends RollingBatchRestartRsAction {

  public RollingBatchRestartRsExceptMetaAction(long sleepTime, float ratio, int maxDeadServers) {
    super(sleepTime, ratio, maxDeadServers);
  }

  @Override
  protected List<ServerName> selectServers() throws java.io.IOException {
    ServerName metaServer = cluster.getServerHoldingMeta();
    List<ServerName> servers = super.selectServers();
    servers.remove(metaServer);
    return servers;
  };

}
