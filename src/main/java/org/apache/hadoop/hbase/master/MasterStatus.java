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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerStatus;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

/**
 * These are the set of functions implemented by the HMaster and accessed by 
 * the other packages in the master.
 * 
 * TODO: this list has to be cleaned up, this is a re-factor only change that 
 * preserves the functions in the interface.
 */
public interface MasterStatus extends ServerStatus {

  public ServerManager getServerManager();

  public RegionManager getRegionManager();

  public boolean isClusterStartup();
  
  public FileSystem getFileSystem();
  
  public Path getOldLogDir();
  
  public ZooKeeperWrapper getZooKeeperWrapper();
  
  public void startShutdown();
  
  public MasterMetrics getMetrics();
  
  public RegionServerOperationQueue getRegionServerOperationQueue();
  
  public ServerConnection getServerConnection();
  
  public boolean checkFileSystem();
  
  public int getThreadWakeFrequency();
  
  public int getNumRetries();
  
  public void deleteEmptyMetaRows(HRegionInterface s,
      byte [] metaRegionName,
      List<byte []> emptyRows);
  
  public HRegionInfo getHRegionInfo(final byte [] row, final Result res) throws IOException;
  
  public Path getRootDir();
  
  public Lock getSplitLogLock();
  
  public int numServers();
  
  public void getLightServers(final HServerLoad l,
      SortedMap<HServerLoad, Set<String>> m);
  
  public SortedMap<HServerLoad, Set<String>> getLoadToServers();
  
  public double getAverageLoad();
}
