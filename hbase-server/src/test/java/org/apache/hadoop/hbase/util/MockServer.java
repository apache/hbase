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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.hadoop.hbase.keymeta.PBEKeyAccessor;
import org.apache.hadoop.hbase.keymeta.PBEKeymetaAdmin;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic mock Server for handler tests.
 */
public class MockServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(MockServer.class);

  final static ServerName NAME = ServerName.valueOf("MockServer", 123, 123456789);

  protected volatile boolean stopped;
  protected volatile boolean aborted;

  public MockServer() {
    stopped = false;
    aborted = false;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.error(HBaseMarkers.FATAL, "Abort {} why={}", getServerName(), why, e);
    stop(why);
    this.aborted = true;
  }

  @Override
  public void stop(String why) {
    LOG.debug("Stop {} why={}", getServerName(), why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ZKWatcher getZooKeeper() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServerName getServerName() {
    return NAME;
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  @Override
  public ChoreService getChoreService() {
    throw new UnsupportedOperationException();
  }

  @Override public SystemKeyCache getSystemKeyCache() {
    return null;
  }

  @Override public PBEKeyAccessor getPBEKeyAccessor() {
    return null;
  }

  @Override public PBEKeymetaAdmin getPBEKeymetaAdmin() {
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStopping() {
    return false;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncClusterConnection getAsyncClusterConnection() {
    throw new UnsupportedOperationException();
  }
}
