/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.ServerStatus;
import org.apache.hadoop.hbase.client.ServerConnection;

/**
 * These are the set of functions implemented by the HMaster and accessed by 
 * the other packages in the master.
 * 
 * TODO: this list has to be cleaned up, this is a re-factor only change that 
 * preserves the functions in the interface.
 */
public interface MasterStatus extends ServerStatus {

  /**
   * Return the server manager for region server related info
   */
  public ServerManager getServerManager();

  /**
   * Return the region manager for region related info
   */
  public RegionManager getRegionManager();
  
  /**
   * Return the file system manager for dealing with FS related stuff
   */
  public FileSystemManager getFileSystemManager();

  /**
   * Is this the master that is starting the cluster up? If true, yes. 
   * Otherwise this is a failed over master.
   */
  public boolean isClusterStartup();

  /**
   * Return the server RPC connection
   */
  public ServerConnection getServerConnection();
  
  // TODO: the semantics of the following methods should be defined. Once that 
  // is clear, most of these should move to server status
  
  // start shutting down the server
  public void startShutdown();
  // is a shutdown requested
  public AtomicBoolean getShutdownRequested();
  // sets the closed variable in the master to true
  public void setClosed();
  // returns the closed atomic boolean
  public AtomicBoolean getClosed();
  // returns the boolean value of the closed atomic boolean
  public boolean isClosed();
  // is the server shutdown
  public void shutdown();
}
