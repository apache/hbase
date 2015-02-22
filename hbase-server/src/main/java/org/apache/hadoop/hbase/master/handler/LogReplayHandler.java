/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;

/**
 * Handle logReplay work from SSH. Having a separate handler is not to block SSH in re-assigning
 * regions from dead servers. Otherwise, available SSH handlers could be blocked by logReplay work
 * (from {@link org.apache.hadoop.hbase.master.MasterFileSystem#splitLog(ServerName)}). 
 * During logReplay, if a receiving RS(say A) fails again, regions on A won't be able 
 * to be assigned to another live RS which causes the log replay unable to complete 
 * because WAL edits replay depends on receiving RS to be live
 */
@InterfaceAudience.Private
public class LogReplayHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(LogReplayHandler.class);
  private final ServerName serverName;
  protected final Server master;
  protected final MasterServices services;
  protected final DeadServer deadServers;

  public LogReplayHandler(final Server server, final MasterServices services,
      final DeadServer deadServers, final ServerName serverName) {
    super(server, EventType.M_LOG_REPLAY);
    this.master = server;
    this.services = services;
    this.deadServers = deadServers;
    this.serverName = serverName;
    this.deadServers.add(serverName);
  }

  @Override
  public String toString() {
    String name = serverName.toString();
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }

  @Override
  public void process() throws IOException {
    try {
      if (this.master != null && this.master.isStopped()) {
        // we're exiting ...
        return;
      }
      this.services.getMasterFileSystem().splitLog(serverName);
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        // resubmit log replay work when failed
        this.services.getExecutorService().submit((LogReplayHandler) this);
        this.deadServers.add(serverName);
        throw new IOException("failed log replay for " + serverName + ", will retry", ex);
      } else {
        throw new IOException(ex);
      }
    } finally {
      this.deadServers.finish(serverName);
    }
    // logReplay is the last step of SSH so log a line to indicate that
    LOG.info("Finished processing shutdown of " + serverName);
  }
}
