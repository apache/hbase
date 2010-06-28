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
package org.apache.hadoop.hbase;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;

/**
 * Set of functions that are exposed by any HBase server (implemented by the 
 * master and region server).
 */
public interface ServerStatus {
  /**
   * Return the address of the current server.
   */
  public HServerAddress getHServerAddress();
  
  /**
   * Get the configuration object for this server.
   */
  public Configuration getConfiguration();

  // TODO: make sure the functions below work on the region server also, or get 
  // moved to the MasterStatus interface
  public AtomicBoolean getShutdownRequested();

  public AtomicBoolean getClosed();

  public boolean isClosed();
  
  public void shutdown();
}
