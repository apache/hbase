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
package org.apache.hadoop.hbase.master.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.HMaster;

/**
 * Impl for exposing HMaster Information through JMX
 */
public class MXBeanImpl implements MXBean {

  private final HMaster master;

  private static MXBeanImpl instance = null;
  public synchronized static MXBeanImpl init(final HMaster master) {
    if (instance == null) {
      instance = new MXBeanImpl(master);
    }
    return instance;
  }

  protected MXBeanImpl(final HMaster master) {
    this.master = master;
  }

  @Override
  public double getAverageLoad() {
    return master.getAverageLoad();
  }

  @Override
  public String getClusterId() {
    return master.getClusterId();
  }

  @Override
  public String getZookeeperQuorum() {
    return master.getZooKeeperWatcher().getQuorum();
  }

  @Override
  public String[] getCoprocessors() {
    return master.getCoprocessors();
  }

  @Override
  public long getMasterStartTime() {
    return master.getMasterStartTime();
  }

  @Override
  public long getMasterActiveTime() {
    return master.getMasterActiveTime();
  }

  @Override
  public int getRegionServers() {
    return this.master.getServerManager().getOnlineServers().size();
  }

  @Override
  public String[] getDeadRegionServers() {
    List<String> deadServers = new ArrayList<String>();
    for (ServerName name : master.getServerManager().getDeadServers()) {
      deadServers.add(name.getHostAndPort());
    }
    return deadServers.toArray(new String[0]);
  }

  @Override
  public String getServerName() {
    return master.getServerName().getServerName();
  }

  @Override
  public boolean getIsActiveMaster() {
    return master.isActiveMaster();
  }
}
