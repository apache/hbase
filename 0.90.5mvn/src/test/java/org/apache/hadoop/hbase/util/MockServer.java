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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;


/**
 * Basic mock Server
 */

public class MockServer implements Server {
  boolean stopped = false;
  final static String NAME = "MockServer";
  final ZooKeeperWatcher zk;
  
  private static final Log LOG = LogFactory.getLog(MockServer.class);
  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();

  public MockServer() throws ZooKeeperConnectionException, IOException {
    this.zk =  new ZooKeeperWatcher(HTU.getConfiguration(), NAME, this);
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.fatal("Abort why=" + why, e);
    this.stopped = true;
  }

  @Override
  public void stop(String why) {
    LOG.debug("Stop why=" + why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public Configuration getConfiguration() {
    return HTU.getConfiguration();
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return this.zk;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getServerName() {
    return NAME;
  }
  
}

