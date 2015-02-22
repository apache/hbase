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
package org.apache.hadoop.hbase.procedure;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.zookeeper.KeeperException;

/**
 * A life-cycle management interface for globally barriered procedures on
 * region servers.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RegionServerProcedureManager extends ProcedureManager {
  /**
   * Initialize a globally barriered procedure for region servers.
   *
   * @param rss Region Server service interface
   * @throws KeeperException
   */
  public abstract void initialize(RegionServerServices rss) throws KeeperException;

  /**
   * Start accepting procedure requests.
   */
  public abstract void start();

  /**
   * Close <tt>this</tt> and all running procedure tasks
   *
   * @param force forcefully stop all running tasks
   * @throws IOException
   */
  public abstract void stop(boolean force) throws IOException;
}
