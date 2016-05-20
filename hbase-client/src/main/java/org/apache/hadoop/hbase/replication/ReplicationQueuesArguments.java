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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

@InterfaceAudience.Private
public class ReplicationQueuesArguments {

  private ZooKeeperWatcher zk;
  private Configuration conf;
  private Abortable abort;

  public ReplicationQueuesArguments(Configuration conf, Abortable abort) {
    this.conf = conf;
    this.abort = abort;
  }

  public ReplicationQueuesArguments(Configuration conf, Abortable abort, ZooKeeperWatcher zk) {
    this(conf, abort);
    setZk(zk);
  }

  public ZooKeeperWatcher getZk() {
    return zk;
  }

  public void setZk(ZooKeeperWatcher zk) {
    this.zk = zk;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Abortable getAbort() {
    return abort;
  }

  public void setAbort(Abortable abort) {
    this.abort = abort;
  }
}
