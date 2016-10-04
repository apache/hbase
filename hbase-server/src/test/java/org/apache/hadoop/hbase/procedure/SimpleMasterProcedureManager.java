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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.zookeeper.KeeperException;

public class SimpleMasterProcedureManager extends MasterProcedureManager {

  public static final String SIMPLE_SIGNATURE = "simle_test";
  public static final String SIMPLE_DATA = "simple_test_data";

  private static final Log LOG = LogFactory.getLog(SimpleMasterProcedureManager.class);

  private MasterServices master;
  private ProcedureCoordinator coordinator;

  private boolean done;

  @Override
  public void stop(String why) {
    LOG.info("stop: " + why);
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void initialize(MasterServices master, MetricsMaster metricsMaster)
      throws KeeperException, IOException, UnsupportedOperationException {
    this.master = master;
    this.done = false;

    // setup the default procedure coordinator
    String name = master.getServerName().toString();
    ThreadPoolExecutor tpool = ProcedureCoordinator.defaultPool(name, 1);
    ProcedureCoordinatorRpcs comms = new ZKProcedureCoordinatorRpcs(
        master.getZooKeeper(), getProcedureSignature(), name);

    this.coordinator = new ProcedureCoordinator(comms, tpool);
  }

  @Override
  public String getProcedureSignature() {
    return SIMPLE_SIGNATURE;
  }

  @Override
  public byte[] execProcedureWithRet(ProcedureDescription desc) throws IOException {
    this.done = false;
    // start the process on the RS
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(desc.getInstance());

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    List<String> servers = new ArrayList<String>();
    for (ServerName sn : serverNames) {
      servers.add(sn.toString());
    }
    Procedure proc = coordinator.startProcedure(monitor, desc.getInstance(), new byte[0], servers);
    if (proc == null) {
      String msg = "Failed to submit distributed procedure for '"
          + getProcedureSignature() + "'";
      LOG.error(msg);
      throw new IOException(msg);
    }

    HashMap<String, byte[]> returnData = null;
    try {
      // wait for the procedure to complete.  A timer thread is kicked off that should cancel this
      // if it takes too long.
      returnData = proc.waitForCompletedWithRet();
      LOG.info("Done waiting - exec procedure for " + desc.getInstance());
      this.done = true;
    } catch (InterruptedException e) {
      ForeignException ee =
          new ForeignException("Interrupted while waiting for procdure to finish", e);
      monitor.receive(ee);
      Thread.currentThread().interrupt();
    } catch (ForeignException e) {
      monitor.receive(e);
    }
    // return the first value for testing
    return returnData.values().iterator().next();
  }

  @Override
  public boolean isProcedureDone(ProcedureDescription desc) throws IOException {
    return done;
  }

}
