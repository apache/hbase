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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Parameters for constructing a {@link ReplicationTracker}.
 */
@InterfaceAudience.Private
public final class ReplicationTrackerParams {

  private final Configuration conf;

  private final Stoppable stopptable;

  private ZKWatcher zookeeper;

  private Connection conn;

  private ChoreService choreService;

  private ReplicationTrackerParams(Configuration conf, Stoppable stopptable) {
    this.conf = conf;
    this.stopptable = stopptable;
  }

  public ReplicationTrackerParams zookeeper(ZKWatcher zookeeper) {
    this.zookeeper = zookeeper;
    return this;
  }

  public ReplicationTrackerParams connection(Connection conn) {
    this.conn = conn;
    return this;
  }

  public ReplicationTrackerParams choreService(ChoreService choreService) {
    this.choreService = choreService;
    return this;
  }

  public Configuration conf() {
    return conf;
  }

  public Stoppable stopptable() {
    return stopptable;
  }

  public ZKWatcher zookeeper() {
    return zookeeper;
  }

  public Connection connection() {
    return conn;
  }

  public ChoreService choreService() {
    return choreService;
  }

  public static ReplicationTrackerParams create(Configuration conf, Stoppable stopptable) {
    return new ReplicationTrackerParams(conf, stopptable);
  }
}
