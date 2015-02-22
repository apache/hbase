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

package org.apache.hadoop.hbase.master.balancer;

import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;

/**
 * Chore that will feed the balancer the cluster status.
 */
@InterfaceAudience.Private
public class ClusterStatusChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(ClusterStatusChore.class);
  private final HMaster master;
  private final LoadBalancer balancer;

  public ClusterStatusChore(HMaster master, LoadBalancer balancer) {
    super(master.getServerName() + "-ClusterStatusChore", master, master.getConfiguration().getInt(
      "hbase.balancer.statusPeriod", 60000));
    this.master = master;
    this.balancer = balancer;
  }

  @Override
  protected void chore() {
    try {
      balancer.setClusterStatus(master.getClusterStatus());
    } catch (InterruptedIOException e) {
      LOG.warn("Ignoring interruption", e);
    }
  }
}
