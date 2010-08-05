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

import org.apache.hadoop.hbase.ServerController;

/**
 * Defines the set of functions implemented by the HMaster related to control
 * of the master process and cluster shutdown.
 */
public interface MasterController extends ServerController {

  // TODO: getServerManager and getFileManager exist because each references the
  //       other in a single call.  should figure how to clean this up.

  /**
   * Returns the server manager which manages  for region server related info
   */
  public ServerManager getServerManager();

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
   * Set whether this is a cluster starting up.
   * @param isClusterStartup whether this is a cluster startup or failover
   */
  public void setClusterStartup(boolean isClusterStartup);

  /**
   * Requests a shutdown of the cluster.
   * <p>
   * Requesting a shutdown
   */
  public void requestShutdown();

  /**
   * Gets a boolean representing whether a shutdown has been requested or not.
   * @return if a shutdown has been requested or not
   */
  public AtomicBoolean getShutdownRequested();

  /**
   * Sets the cluster as closed.
   */
  public void setClosed();

  /**
   * Gets an atomic boolean that represents whether the master is closed.
   * @return boolean used to get/set master closed status
   */
  public AtomicBoolean getClosed();

  /**
   * Returns true if the master is closed, false if not.
   * @return if master is closed
   */
  public boolean isClosed();
}
