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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.procedure.flush.RegionServerFlushTableProcedureManager;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the globally barriered procedure framework and environment
 * for region server oriented operations.
 * {@link org.apache.hadoop.hbase.regionserver.HRegionServer} interacts
 * with the loaded procedure manager through this class.
 */
@InterfaceAudience.Private
public class RegionServerProcedureManagerHost extends
    ProcedureManagerHost<RegionServerProcedureManager> {

  private static final Logger LOG = LoggerFactory
      .getLogger(RegionServerProcedureManagerHost.class);

  public void initialize(RegionServerServices rss) throws KeeperException {
    for (RegionServerProcedureManager proc : procedures) {
      LOG.debug("Procedure {} initializing", proc.getProcedureSignature());
      proc.initialize(rss);
      LOG.debug("Procedure {} initialized", proc.getProcedureSignature());
    }
  }

  public void start() {
    for (RegionServerProcedureManager proc : procedures) {
      LOG.debug("Procedure {} starting", proc.getProcedureSignature());
      proc.start();
      LOG.debug("Procedure {} started", proc.getProcedureSignature());
    }
  }

  public void stop(boolean force) {
    for (RegionServerProcedureManager proc : procedures) {
      try {
        proc.stop(force);
      } catch (IOException e) {
        LOG.warn("Failed to close procedure " + proc.getProcedureSignature()
            + " cleanly", e);
      }
    }
  }

  @Override
  public void loadProcedures(Configuration conf) {
    loadUserProcedures(conf, REGIONSERVER_PROCEDURE_CONF_KEY);
    // load the default snapshot manager
    procedures.add(new RegionServerSnapshotManager());
    // load the default flush region procedure manager
    procedures.add(new RegionServerFlushTableProcedureManager());
  }

}
