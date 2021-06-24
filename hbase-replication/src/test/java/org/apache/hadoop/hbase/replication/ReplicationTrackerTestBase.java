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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for testing {@link ReplicationTracker} and {@link ReplicationListener}.
 */
public abstract class ReplicationTrackerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationTrackerTestBase.class);

  private ReplicationTracker rt;
  
  private volatile Set<ServerName> regionServers;

  @Before
  public void setUp() {
    ReplicationTrackerParams params = createParams();
    rt = ReplicationFactory.getReplicationTracker(params);
    regionServers = null;
  }

  protected abstract ReplicationTrackerParams createParams();

  protected abstract void addServer(ServerName sn) throws Exception;

  protected abstract void removeServer(ServerName sn) throws Exception;

  @Test
  public void testWatchRegionServers() throws Exception {
    ServerName sn =
      ServerName.valueOf("hostname2.example.org,1234," + EnvironmentEdgeManager.currentTime());
    addServer(sn);
    rt.registerListener(new DummyReplicationListener());
    assertEquals(1, rt.loadLiveRegionServersAndInitializeListeners().size());
    // delete one
    removeServer(sn);
    // wait for event
    Waiter.waitFor(HBaseConfiguration.create(), 15000, () -> regionServers != null);
    assertTrue(regionServers.isEmpty());
  }

  private class DummyReplicationListener implements ReplicationListener {

    @Override
    public void regionServerListChanged(Set<ServerName> rses) {
      regionServers = rses;
      LOG.debug("Received regionServerListChanged event: {}", regionServers);
    }
  }

  protected static class WarnOnlyStoppable implements Stoppable {

    @Override
    public void stop(String why) {
      LOG.warn("TestReplicationTracker received stop, ignoring. Reason: " + why);
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }
}
