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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.zookeeper.KeeperException;


public class ServerShutdownHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(ServerShutdownHandler.class);
  private final HServerInfo hsi;
  private final Server server;
  private final MasterServices services;
  private final DeadServer deadServers;

  public ServerShutdownHandler(final Server server, final MasterServices services,
      final DeadServer deadServers, final HServerInfo hsi) {
    super(server, EventType.M_SERVER_SHUTDOWN);
    this.hsi = hsi;
    this.server = server;
    this.services = services;
    this.deadServers = deadServers;
    // Add to dead servers.
    this.deadServers.add(hsi.getServerName());
  }

  @Override
  public void process() throws IOException {
    try {
      this.server.getCatalogTracker().processServerShutdown(this.hsi);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted", e);
    } catch (KeeperException e) {
      this.server.abort("In server shutdown processing", e);
      throw new IOException("Aborting", e);
    }
    final String serverName = this.hsi.getServerName();

    LOG.info("Splitting logs for " + serverName);
    this.services.getMasterFileSystem().splitLog(serverName);

    // Clean out anything in regions in transition.  Being conservative and
    // doing after log splitting.  Could do some states before -- OPENING?
    // OFFLINE? -- and then others after like CLOSING that depend on log
    // splitting.
    this.services.getAssignmentManager().processServerShutdown(this.hsi);

    NavigableSet<HRegionInfo> hris =
      MetaReader.getServerRegions(this.server.getCatalogTracker(), this.hsi);
    LOG.info("Reassigning the " + hris.size() + " region(s) that " + serverName +
      " was carrying.");

    // We should encounter -ROOT- and .META. first in the Set given how its
    // a sorted set.
    for (HRegionInfo hri: hris) {
      // If table is not disabled but the region is offlined,
      boolean disabled = this.services.getAssignmentManager().
        isTableDisabled(hri.getTableDesc().getNameAsString());
      if (disabled) continue;
      if (hri.isOffline()) {
        LOG.warn("TODO: DO FIXUP ON OFFLINED PARENT? REGION OFFLINE -- IS THIS RIGHT?" + hri);
        continue;
      }
      this.services.getAssignmentManager().assign(hri);
    }
    this.deadServers.remove(serverName);
    LOG.info("Finished processing of shutdown of " + serverName);
  }
}