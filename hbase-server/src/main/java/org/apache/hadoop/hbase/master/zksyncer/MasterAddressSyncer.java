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
package org.apache.hadoop.hbase.master.zksyncer;

import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Tracks the active master address on server ZK cluster and synchronize them to client ZK cluster
 * if changed
 */
@InterfaceAudience.Private
public class MasterAddressSyncer extends ClientZKSyncer {
  private final String masterAddressZNode;

  public MasterAddressSyncer(ZKWatcher watcher, ZKWatcher clientZkWatcher, Server server) {
    super(watcher, clientZkWatcher, server);
    masterAddressZNode = watcher.getZNodePaths().masterAddressZNode;
  }

  @Override
  protected boolean validate(String path) {
    return path.equals(masterAddressZNode);
  }

  @Override
  protected Set<String> getPathsToWatch() {
    return Collections.singleton(masterAddressZNode);
  }
}
