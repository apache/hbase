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
package org.apache.hadoop.hbase.regionserver.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

public class OpenDaughterRegionHandler extends OpenRegionHandler {
  private static final Log LOG = LogFactory.getLog(OpenDaughterRegionHandler.class);

  public OpenDaughterRegionHandler(Server server,
      RegionServerServices rsServices, CatalogTracker catalogTracker,
      HRegionInfo regionInfo) {
    super(server, rsServices, catalogTracker, regionInfo, EventType.RS2RS_OPEN_REGION);
  }

  @Override
  int transitionZookeeper(String encodedName) {
    // Transition ZK node from no znode to OPENING
    int openingVersion = -1;
    try {
      if ((openingVersion = ZKAssign.transitionNodeOpening(server.getZooKeeper(),
          getRegionInfo(), server.getServerName(), EventType.RS2RS_OPEN_REGION)) == -1) {
        LOG.warn("Error transitioning node from OFFLINE to OPENING, " +
          "aborting open");
      }
    } catch (KeeperException e) {
      LOG.error("Error transitioning node from OFFLINE to OPENING for region " +
        encodedName, e);
    }
    return openingVersion;
  }
}