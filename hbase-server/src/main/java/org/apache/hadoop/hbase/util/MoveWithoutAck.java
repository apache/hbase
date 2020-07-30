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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Move Regions without Acknowledging.Usefule in case of RS shutdown as we might want to shut the
 * RS down anyways and not abort on a stuck region. Improves movement performance
 */
@InterfaceAudience.Private
class MoveWithoutAck implements Callable<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(MoveWithoutAck.class);

  private final RegionInfo region;
  private final ServerName targetServer;
  private final List<RegionInfo> movedRegions;
  private final ServerName sourceServer;
  private final Admin admin;

  MoveWithoutAck(Admin admin, RegionInfo regionInfo, ServerName sourceServer,
    ServerName targetServer, List<RegionInfo> movedRegions) {
    this.admin = admin;
    this.region = regionInfo;
    this.targetServer = targetServer;
    this.movedRegions = movedRegions;
    this.sourceServer = sourceServer;
  }

  @Override
  public Boolean call() {
    try {
      LOG.info("Moving region: {} from {} to {}", region.getEncodedName(), sourceServer,
        targetServer);
      admin.move(region.getEncodedNameAsBytes(), targetServer);
      LOG.info("Requested move {} from {} to {}", region.getEncodedName(), sourceServer,
        targetServer);
    } catch (Exception e) {
      LOG.error("Error Moving Region: {}", region.getEncodedName(), e);
    } finally {
      movedRegions.add(region);
    }
    return true;
  }
}
