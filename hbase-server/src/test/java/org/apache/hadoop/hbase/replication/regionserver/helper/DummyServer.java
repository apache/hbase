/*
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

package org.apache.hadoop.hbase.replication.regionserver.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

public class DummyServer implements Server {
  Configuration conf;
  String hostname;
  ZooKeeperWatcher zkw;

  public DummyServer(Configuration conf, String hostname, ZooKeeperWatcher zkw) {
    this.conf = conf;
    this.hostname = hostname;
    this.zkw = zkw;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zkw;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }
  @Override
  public ClusterConnection getConnection() {
    return null;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return ServerName.valueOf(hostname, 1234, 1L);
  }

  @Override
  public void abort(String why, Throwable e) {
    // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public void stop(String why) {
    // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isStopped() {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }
}
