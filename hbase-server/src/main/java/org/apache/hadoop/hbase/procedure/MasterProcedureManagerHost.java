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
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.zookeeper.KeeperException;

/**
 * Provides the globally barriered procedure framework and environment for
 * master oriented operations. {@link org.apache.hadoop.hbase.master.HMaster} 
 * interacts with the loaded procedure manager through this class.
 */
public class MasterProcedureManagerHost extends
    ProcedureManagerHost<MasterProcedureManager> {

  private Hashtable<String, MasterProcedureManager> procedureMgrMap
      = new Hashtable<String, MasterProcedureManager>();

  @Override
  public void loadProcedures(Configuration conf) {
    loadUserProcedures(conf, MASTER_PROCEUDRE_CONF_KEY);
    for (MasterProcedureManager mpm : getProcedureManagers()) {
      procedureMgrMap.put(mpm.getProcedureSignature(), mpm);
    }
  }

  public void initialize(MasterServices master, final MetricsMaster metricsMaster)
      throws KeeperException, IOException, UnsupportedOperationException {
    for (MasterProcedureManager mpm : getProcedureManagers()) {
      mpm.initialize(master, metricsMaster);
    }
  }

  public void stop(String why) {
    for (MasterProcedureManager mpm : getProcedureManagers()) {
      mpm.stop(why);
    }
  }

  public MasterProcedureManager getProcedureManager(String signature) {
    return procedureMgrMap.get(signature);
  }
}
