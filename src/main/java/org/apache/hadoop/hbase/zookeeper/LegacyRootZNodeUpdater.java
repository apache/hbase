/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.StopStatus;
import org.apache.hadoop.hbase.util.HasThread;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A background thread that wakes up frequently and writes the legacy root 
 * region location znode (host:port only, no server start code).
 */
public class LegacyRootZNodeUpdater extends HasThread {

  private static final int WAIT_MS = 1000;
  
  private ZooKeeperWrapper zkw;
  private StopStatus stopped;
  private final AtomicReference<HServerInfo> rootRegionServerInfo;

  public LegacyRootZNodeUpdater(ZooKeeperWrapper zkw, StopStatus stopped,
      AtomicReference<HServerInfo> rootRegionServerInfo) {
    this.zkw = zkw;
    this.rootRegionServerInfo = rootRegionServerInfo;
    this.stopped = stopped;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(LegacyRootZNodeUpdater.class.getName());
    HServerInfo prevRootLocation = null;
    boolean firstUpdate = true;
    while (!stopped.isStopped()) { 
      HServerInfo rootLocation = rootRegionServerInfo.get();
      if (firstUpdate ||
          (prevRootLocation != rootLocation &&  // check that they are not both null
           (rootLocation == null ||  // this means prevRootLocation != null, so they are different
            !rootLocation.equals(prevRootLocation)))) {
        zkw.writeLegacyRootRegionLocation(rootLocation);
        prevRootLocation = rootLocation;
      }
        try {
          synchronized (rootRegionServerInfo) {
            rootRegionServerInfo.wait(WAIT_MS);
          }
        } catch (InterruptedException ex) {
        // Ignore. We will only stop if the master is shutting down.
      }
      firstUpdate = false;
    }
  }

}
