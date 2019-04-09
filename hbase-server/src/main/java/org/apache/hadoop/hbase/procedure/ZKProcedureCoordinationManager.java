/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
public class ZKProcedureCoordinationManager implements ProcedureCoordinationManager{
  ZKWatcher watcher;

  public ZKProcedureCoordinationManager(Server server) {
    this.watcher = server.getZooKeeper();
  }

  @Override
  public ProcedureCoordinatorRpcs getProcedureCoordinatorRpcs(String procType, String coordNode) {
    return new ZKProcedureCoordinator(watcher, procType, coordNode);
  }

  @Override
  public ProcedureMemberRpcs getProcedureMemberRpcs(String procType) throws KeeperException {
    return new ZKProcedureMemberRpcs(watcher, procType);
  }
}
