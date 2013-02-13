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

import java.io.Closeable;

import org.apache.zookeeper.KeeperException;

/**
 * This provides an interface for getting and setting the replication state of a
 * cluster. This state is used to indicate whether replication is enabled or
 * disabled on a cluster.
 */
public interface ReplicationStateInterface extends Closeable {
	
  /**
   * Get the current state of replication (i.e. ENABLED or DISABLED).
   * 
   * @return true if replication is enabled, false otherwise
   * @throws KeeperException
   */
  public boolean getState() throws KeeperException;
	
  /**
   * Set the state of replication.
   * 
   * @param newState
   * @throws KeeperException
   */
  public void setState(boolean newState) throws KeeperException;
}