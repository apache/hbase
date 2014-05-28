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

package org.apache.hadoop.hbase;

/**
 * Class used to interact with the processes and machines of a real distributed cluster.
 *
 * This will use a pluggable method of interacting with processes,
 * network and hardware of a cluster.
 *
 */
public class HBaseDistributedCluster {
  public ClusterStatus getInitialClusterStatus() {
    return null;
  }

  public void killMaster(HServerAddress server) {

  }

  public void waitForMasterToStop(HServerAddress server, long timeout) {

  }

  public void startMaster(String hostname) {

  }

  public void waitForActiveAndReadyMaster(long timeout) {

  }

  public void killRegionServer(HServerAddress server) {

  }

  public void waitForRegionServerToStop(HServerAddress server, long timeout) {

  }

  public void startRegionServer(String hostname) {

  }

  public void waitForRegionServerToStart(String hostname, long timeout) {

  }

  public ClusterStatus getClusterStatus() {
    return null;
  }
}
